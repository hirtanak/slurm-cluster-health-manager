#!/usr/bin/env python3
import os
import time
import subprocess
import logging
import json
import importlib
from concurrent.futures import ThreadPoolExecutor, as_completed
import remote_node_utils
from report_generator import summarize_and_output
from health_manager_config import (
    USER_HOME,
    BASE_DIR,
    RESULT_DIR,
    TIMESTAMP,
    MAX_PARALLEL,
    RECHECK_INTERVAL_SECONDS,
    MAX_REBOOT_COUNT,
)

# =============================
# SSH/SCP wait and retry
# =============================
def get_nodes() -> list[str]:
    """
    Reload the contents of config.py each time and return a hostname list from the latest PREFIX and NODE_COUNT
    """
    # Clear module cache and reload
    import health_manager_config
    importlib.reload(health_manager_config)

    prefix     = health_manager_config.PREFIX
    node_count = health_manager_config.NODE_COUNT

    return [f"{prefix}-{i}" for i in range(1, node_count + 1)]

def wait_for_ssh(node, timeout=300, interval=5):
    start=time.time()
    while time.time()-start<timeout:
        try:
            import socket as _s
            s=_s.socket(_s.AF_INET,_s.SOCK_STREAM)
            s.settimeout(2)
            s.connect((node,22))
            s.close()
            return True
        except:
            time.sleep(interval)
    return False

def scp_with_retry(src, dst, retries=3, delay=5):
    for i in range(retries):
        if subprocess.run(["scp","-o","StrictHostKeyChecking=no",src,dst]).returncode==0:
            return True
        time.sleep(delay)
    return False

# =============================
# Run run_all_nodes_check.py on node
# =============================
def run_check_on_node(node):
    """
    Run run_all_nodes_check.py on node, and when 
    /tmp/reboot_required appears, restart -> rerun loop.
    Finally, retrieve JSON and return (node, data_dict).
    """
    # Base SSH commands
    ssh_cmd = (
        f"ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null {node} "
        f"'export CHECK_TIMESTAMP={TIMESTAMP} "
        f"CHECK_RESULT_DIR={BASE_DIR} "
        f"PREVIOUS_RESULT_PATH={BASE_DIR}/hpc_check_result_{node}.json "
        f"ENABLE_REBOOT_ON_FAILURE=true "
        f"MAX_REBOOT_COUNT={MAX_REBOOT_COUNT} && "
        f"python3 run_all_nodes_check.py'"
    )

    # Iterate through initial run and optional reboots
    for attempt in range(1, MAX_REBOOT_COUNT + 2):  # +1 initial + MAX retries
        ret = subprocess.run(ssh_cmd, shell=True, timeout=600).returncode
        if ret == 0:
            logging.info(f"{node}: Check Success (exit_code=0)")
        else:
            logging.warning(f"{node}: Check Failure (exit_code={ret})")

        # Handle reboot trigger and perform re-check
        chk = subprocess.run(
            ["ssh", node, "test -f /tmp/reboot_required"],
            capture_output=True
        )
        if chk.returncode != 0:
            logging.debug(f"{node}: /tmp/reboot_required does not exist -> retry terminated")
            break

        # Handle reboot trigger and perform re-check
        if attempt > MAX_REBOOT_COUNT:
            logging.error(f"{node}: Maximum number of restarts {MAX_REBOOT_COUNT} exceeded, Exit..")
            break

        logging.info(f"{node}: Start Restart (attempt {attempt}/{MAX_REBOOT_COUNT})")
        subprocess.run(
            ["ssh", "-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null",
             node, "sudo reboot"],
            timeout=30
        )

        # Waiting for SSH return
        logging.debug(f"{node}: Sleeping {RECHECK_INTERVAL_SECONDS}s before re-check")
        time.sleep(RECHECK_INTERVAL_SECONDS)
        remote_node_utils.wait_for_ssh(node)

        # Waiting for SSH return
        subprocess.run(
            ["ssh", "-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null",
             node, "rm -f /tmp/reboot_required"],
            timeout=10
        )

        # Re-run in the next loop here

    # Load health check result JSON from local path if available
    json_path = os.path.join(BASE_DIR, f"hpc_check_result_{node}.json")
    if os.path.exists(json_path):
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        data.setdefault("node", node)
        data.setdefault("timestamp", TIMESTAMP)
        data.setdefault("initial_returncode", 255)
        data.setdefault("final_returncode", 255)
        data.setdefault("reboot_count", 0)
        return node, data

    # C) Fallback
    logging.error(f"{node}: JSON not found -> {json_path}")
    fallback = {
        "node": node,
        "timestamp": TIMESTAMP,
        "initial_returncode": 255,
        "final_returncode": 255,
        "reboot_count": 0,
        "error": "remote JSON not found"
    }
    return node, fallback


# =============================
# Main execution entry
# =============================
def main():
    # Retrieve VM metadata or resource ID
    nodes = get_nodes()

    results = []
    # Process and save test result data
    with ThreadPoolExecutor(max_workers=MAX_PARALLEL) as executor:
        futures = { executor.submit(run_check_on_node, node): node for node in nodes }
        for fut in as_completed(futures):
            node = futures[fut]
            try:
                _, data = fut.result()
                results.append(data)
            except Exception as e:
                logging.error(f"{node} Exception during check of {node}: {e}")
                # On failure, take fallback action
                results.append({
                    "node": node,
                    "timestamp": TIMESTAMP,
                    "initial_returncode": 255,
                    "final_returncode": 255,
                    "reboot_count": 0,
                    "error": str(e)
                })

    # Output to CSV or HTML
    for entry in results:
        node = entry.get("node", "<unknown>")
        code = entry.get("final_returncode", 1)
        status = "Success" if code == 0 else "Fail"
        logging.info(f"{node}: Final Result = {status} (code={code})")

    # Output summary report to CSV and HTML
    summarize_and_output(results, BASE_DIR)

    # Process and save test result data
    tarfile = f"{USER_HOME}/gpu_all_check_{TIMESTAMP}.tar.gz"
    logging.info(f"Archived in: {tarfile}")
    subprocess.run(
        ["tar", "-czf", tarfile, "-C", f"{USER_HOME}/health_results", TIMESTAMP],
        check=False
    )
    
    logging.info("All processes completed by orchestrator")

    # Evaluate test outcome based on conditions
    #run_ghr_if_needed()

    # Send Teams notification if failed nodes exist

if __name__ == "__main__":
    main()