import os
import subprocess
import socket
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from report_generator import summarize_and_output
from node_health_check_runner import (
    run_gpu_health_check,
    run_nccl_test,
    NCCL_BW_THRESHOLD,
    NCCL_MULTI_BW_THRESHOLD
)

# ============================
# SSH environment variable command assembly 
# ============================
def build_ssh_env(remote_home, previous_result_path, remote_script):
    """
    to run node_health_check_runner.py on remote node 
    return one-liner SSH command string with environment variables.

    Args:
        remote_home: remote home directory 
        previous_result_path: path of previous result JSON 
        remote_script: remote path of execution script 
    :return: 
        SSH command string
    """
    return (
        f"CHECK_TIMESTAMP={os.environ.get('CHECK_TIMESTAMP')} "
        f"CHECK_RESULT_DIR={remote_home}/health_results/{os.environ.get('CHECK_TIMESTAMP')} "
        f"ENABLE_REBOOT_ON_FAILURE={'true' if os.environ.get('ENABLE_REBOOT_ON_FAILURE')=='true' else 'false'} "
        f"MAX_REBOOT_COUNT={os.environ.get('MAX_REBOOT_COUNT')} "
        f"PREVIOUS_RESULT_PATH={previous_result_path} "
        f"NODE_PREFIX={os.environ.get('NODE_PREFIX')} "
        f"NODE_COUNT={os.environ.get('NODE_COUNT')} "
        f"NCCL_BW_THRESHOLD={os.environ.get('NCCL_BW_THRESHOLD')} "
        f"NCCL_MULTI_BW_THRESHOLD={os.environ.get('NCCL_MULTI_BW_THRESHOLD')} "
        f"python3 {remote_script}"
    )

# ============================
# Wait for SSH reconnect 
# ============================
def wait_for_ssh(node, timeout=300, interval=10):
    """
    Wait until SSH connection to the specified node is available.

    :param node: hostname 
    :param timeout: timeout (sec) 
    :param interval: retry interval (sec) 
    :return: True if connection succeeded, False if timeout 
    """
    start = time.time()
    while time.time() - start < timeout:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(2)
                s.connect((node, 22))
            logging.info(f"SSH connection succeeded: {node}")
            return True
        except Exception:
            logging.info(f"Waiting for SSH connection...: {node}")
            time.sleep(interval)
    logging.error(f"SSH connection timeout: {node}")
    return False

# ============================
# Script File Distribution
# ============================
def copy_script_to_node(node, remote_path, local_script, max_retries=5, retry_delay=2):
    """
    Distribute scripts to nodes via SCP. With retries.

    :param node: Host name 
    :param remote_path: Remote destination path 
    :param local_script: Local script path 
    :param max_retries: Maximum number of retries 
    :param retry_delay: Retry interval (in seconds) 
    :return: True if distribution is successful, False if not. return: True if distribution is successful, False if not.
    """
    for attempt in range(1, max_retries + 1):
        try:
            logging.info(f"[COPY] {node} Script distribution - {attempt}/{max_retries})")
            cmd = [
                "scp",
                "-o", "StrictHostKeyChecking=no",
                "-o", "UserKnownHostsFile=/dev/null",
                local_script,
                f"{node}:{remote_path}"
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            if result.returncode == 0:
                logging.info(f"SCP Success: {node}:{remote_path}")
                return True
            logging.warning(f"SCP Failure (rc={result.returncode}): {result.stderr}")
        except Exception as e:
            logging.error(f"SCP Exception: {node} - {e}")
        time.sleep(retry_delay)
    logging.error(f"Script Distribution Failure: {node}")
    return False

# ============================
# Parallel Script Distribution
# ============================
def distribute_scripts_parallel(nodes, remote_path, local_script, max_workers=5, **copy_kwargs):
    """
    Execute script distribution to multiple nodes in parallel.

    :param nodes: List of hostnames 
    :param remote_path: Remote destination path 
    :param local_script: Local script path 
    :param max_workers: Number of parallel workers 
    :param copy_kwargs: copy_script Additional arguments for _to_node 
    :return: {node: bool} mapping
    """
    results = {}
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {
            ex.submit(copy_script_to_node, node, remote_path, local_script, **copy_kwargs): node
            for node in nodes
        }
        for fut in as_completed(futures):
            node = futures[fut]
            try:
                results[node] = fut.result()
            except Exception as e:
                logging.error(f"Exception in distribution: {node} - {e}")
                results[node] = False
    return results

# ============================
# SCP with retry
# ============================
def scp_with_retry(src, dst, retries=3, delay=5):
    """
    Retry SCP commands.

    :param src: source ("node:/path") 
    :param dst: local destination 
    :param retries: number of retries 
    :param delay: retry interval (in seconds) 
    :return: True if success, False if failure
    """
    for i in range(1, retries+1):
        try:
            subprocess.run([
                "scp",
                "-o", "StrictHostKeyChecking=no",
                "-o", "UserKnownHostsFile=/dev/null",
                src, dst
            ], check=True, timeout=60)
            return True
        except Exception as e:
            logging.warning(f"SCP Retry {i}/{retries} Failure: {e}")
            time.sleep(delay)
    return False

# ============================
# JSON Data Collection
# ============================
def fetch_remote_json(node, remote_home, timestamp, local_dir):
    """
    Get a JSON result file from a remote node with SCP.

    :param node: Host 
    :param remote_home: Remote home directory 
    :param timestamp: Check timestamp 
    :param local_dir: Local destination directory 
    :return: Local file path if success, otherwise None
    """
    remote_path = f"{remote_home}/health_results/{timestamp}/hpc_check_result_{node}.json"
    local_path  = os.path.join(local_dir, f"hpc_check_result_{node}.json")
    # Existence check
    check = subprocess.run([
        "ssh", node, f"test -f {remote_path}"
    ], capture_output=True)
    if check.returncode != 0:
        logging.warning(f"Remote JSON undetected: {node}:{remote_path}")
        return None
    # SCP
    if scp_with_retry(f"{node}:{remote_path}", local_path):
        return local_path
    logging.error(f"JSON acquisition failure: {node}")
    return None

# ============================
# Get remote context
# ============================
def get_remote_context(node, user=None):
    """
    Returns the $HOME path on the node and the result/script path.

    :param node: hostname 
    :param user: fallback user name 
    :return: (remote_home, result_dir, script_path)
    """
    # Clear host key
    subprocess.run(["ssh-keygen", "-R", node], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        r = subprocess.run([
            "ssh", "-o", "StrictHostKeyChecking=no",
            "-o", "UserKnownHostsFile=/dev/null",
            node, "echo $HOME"
        ], capture_output=True, text=True, timeout=10)
        home = r.stdout.strip()
        if not home:
            raise ValueError
    except:
        home = f"/shared/home/{user or os.environ.get('USER','azureuser')}"
    script_path = os.path.join(home, "run_all_nodes_check.py")
    return home, script_path

# ============================
# Reboot the node and perform health re-check
# ============================
def handle_reboot_and_recheck(node, remote_home, ssh_env_cmd, result, log_file):
    """
    If /tmp/reboot_required is available, reboot and run recheck, add logs.
    """
    check = subprocess.run([
        "ssh", node, "test -f /tmp/reboot_required && echo REBOOT"
    ], capture_output=True, text=True)
    if "REBOOT" not in check.stdout:
        return result
    # Reboot flag
    result["reboot_count"] = result.get("reboot_count",0) + 1
    subprocess.run(["ssh", node, "sudo reboot"], timeout=10)
    if wait_for_ssh(node):
        proc = subprocess.run([
            "ssh", node, ssh_env_cmd
        ], capture_output=True, text=True, timeout=300)
        result["post_reboot_returncode"] = proc.returncode
        with open(log_file, 'a', encoding='utf-8') as f:
            f.write(f"\n[RECHECK STDOUT]\n{proc.stdout}\n[RECHECK STDERR]\n{proc.stderr}")
    return result
