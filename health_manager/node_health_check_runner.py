#!/usr/bin/env python3
import os
import subprocess
import json
import logging
from datetime import datetime
from pathlib import Path
import re
from report_generator import upgrade_nhc, fetch_imds_resource_id
from health_manager_config import (
    RESULT_DIR,
    TIMESTAMP,
    NODE_NAME,
    NODE_COUNT,
    RESULT_FILE,
    HEALTH_CHECK_SCRIPT,
    HEALTH_CHECK_VERSION,
    NHC_UPGRADE,
    NCCL_BW_THRESHOLD,
    NCCL_MULTI_BW_THRESHOLD,
    PREFIX        as NODE_PREFIX
)

logging.basicConfig(level=logging.DEBUG, format='[%(levelname)s] %(message)s')

# =============================
# Optionally upgrade NHC scripts
# =============================
def run_gpu_health_check():
    """GPU ヘルスチェック via Azure NHC
    戻り値: ok (bool), detail (dict with keys "log", "nhc_error_codes"), physical_host_name (str), vm_name (str)
    """
    if NODE_NAME.endswith("-scheduler"):
        return True, {"log": ""}, "", ""

    log_path = os.path.join(RESULT_DIR, f"{NODE_NAME}_nhc.log")
    cmd = [HEALTH_CHECK_SCRIPT, "-a", "--output", log_path]
    logging.debug(f"{NODE_NAME}: Running NHC command: {' '.join(cmd)}")
    try:
        r = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=120
        )
        logging.debug(f"{NODE_NAME}: NHC returncode={r.returncode}")
        if r.returncode != 0:
            logging.error(f"{NODE_NAME}: NHC script failed: stderr={r.stderr.strip()}")
            return False, {"log": "", "nhc_error_codes": []}, "", ""

        with open(log_path) as f:
            txt = f.read()
        logging.debug(f"{NODE_NAME}: NHC log length={len(txt)} characters")

        # Optionally upgrade NHC scripts
        nhc_error_codes = re.findall(r"\b(NHC\d{4})\b", txt)
        logging.debug(f"{NODE_NAME}: Extracted NHC error codes: {nhc_error_codes}")

        # Check NHC log for FAIL or Error to determine status
        ok = ("FAIL" not in txt) and ("Error" not in txt)
        # # Extract physical and VM name from logs
        physical = ""
        vmname = ""
        for line in txt.splitlines():
            if line.startswith("PHYSICAL HOST NAME:"):
                physical = line.split(":", 1)[1].strip()
            elif line.startswith("VM NAME:"):
                vmname = line.split(":", 1)[1].strip()

        logging.info(f"{NODE_NAME}: NHC check {'PASSED' if ok else 'FAILED'}, physical={physical}, vm={vmname}")
        # Returns: ok, detail(log and error code), physical, vmname
        return ok, {"log": txt, "nhc_error_codes": nhc_error_codes}, physical, vmname

    except subprocess.TimeoutExpired as te:
        logging.error(f"{NODE_NAME}: NHC timeout after 120s: {te}")
        return False, {"log": "", "nhc_error_codes": []}, "", ""
    except Exception as e:
        logging.exception(f"{NODE_NAME}: Unexpected exception in NHC check: {e}")
        return False, {"log": "", "nhc_error_codes": []}, "", ""


# =============================
# NCCL stand-alone
# =============================
def run_nccl_test():
    """
    Single node NCCL test 
    Returns:   
        ok (bool | None), # True=Succeed, False=Fail, None=Skip/N/A 
        bandwidth (float | None), 
        error_code
    """
    log_path = os.path.join(RESULT_DIR, f"{NODE_NAME}_nccl.log")
    nccl_error_codes = []

    # 1) Check the number of GPUs
    try:
        r = subprocess.run(["nvidia-smi", "-L"], capture_output=True, text=True, timeout=10)
        gpu_count = len(set(r.stdout.splitlines()))
        logging.debug(f"{NODE_NAME}: Found {gpu_count} GPUs")
        if gpu_count == 0:
            nccl_error_codes.append("NCCL1001")
            return True, 0.0, nccl_error_codes
        if gpu_count < 8:
            # Insufficient GPUs -> N/A
            nccl_error_codes.append("NCCL1002")
            return None, None, nccl_error_codes
    except Exception as e:
        logging.error(f"{NODE_NAME}: Failed to get GPU count: {e}")
        # Retrieve VM metadata or resource ID
        return None, None, []

    cmd = (
        "source /etc/profile.d/modules.sh && module load mpi/openmpi "
        "&& all_reduce_perf -b 8 -e 4G -f 2 -g 1"
    )
    logging.debug(f"{NODE_NAME}: Running NCCL test command: {cmd}")
    try:
        with open(log_path, "w", encoding="utf-8") as f:
            subprocess.run(
                ["bash", "-lc", cmd],
                stdout=f,
                stderr=subprocess.STDOUT,
                timeout=60
            )
    except Exception as e:
        logging.error(f"{NODE_NAME}: NCCL test execution failed: {e}")
        nccl_error_codes.append("NCCL1003")
        return False, 0.0, nccl_error_codes
    
    # Process and save test result data
    bandwidth = None
    try:
        with open(log_path, encoding="utf-8") as f:
            for line in f:
                if line.strip().startswith("4G"):
                    parts = line.split()
                    try:
                        bandwidth = float(parts[6])
                    except Exception:
                        pass
                    break
    except Exception as e:
        logging.error(f"{NODE_NAME}: Failed to parse NCCL log: {e}")
        nccl_error_codes.append("NCCL1004")

    if bandwidth is None:
        # Retrieve VM metadata or resource ID
        nccl_error_codes.append("NCCL1005")
        return False, 0.0, nccl_error_codes

    # Determine if measured NCCL bandwidth meets the threshold
    passed = bandwidth >= NCCL_BW_THRESHOLD
    if not passed:
        nccl_error_codes.append("NCCL1006")

    # Log the result and return status, bandwidth, and error codes
    logging.info(f"{NODE_NAME}: NCCL test {'PASSED' if passed else 'FAILED'}: bw={bandwidth} MB/s, errors={nccl_error_codes}")
    return passed, bandwidth, nccl_error_codes


# =============================
# NCCL multi-node
# =============================
def run_nccl_multi_node_test():
    """Return multi-node NCCL test results
    Return: dict {"nodes": list of node names, "busbw": float | "N/A", "passed": bool | "N/A", "multi_error_codes": list of str}
    """
    nodes = [f"{NODE_PREFIX}-{i}" for i in range(1, NODE_COUNT+1)]
    multi_error_codes = []

    # Skip when the number of nodes is insufficient
    if len(nodes) < 2:
        multi_error_codes.append("NCCL_MULTI1001")  # Insufficient number of nodes
        return {"nodes": nodes, "busbw": "N/A", "passed": "N/A", "multi_error_codes": multi_error_codes}

    # Check the number of GPUs on each node
    for n in nodes:
        try:
            r = subprocess.run(
                ["ssh", n, "nvidia-smi -L | wc -l"],
                capture_output=True,
                text=True,
                timeout=10
            )
            gpu_count = int(r.stdout.strip())
            logging.debug(f"{n}: GPU count={gpu_count}")
            if gpu_count < 8:
                multi_error_codes.append("NCCL_MULTI1002")  # Insufficient GPUs
                return {"nodes": nodes, "busbw": "N/A", "passed": "N/A", "multi_error_codes": multi_error_codes}
        except Exception as e:
            logging.error(f"{n}: Failed GPU count check: {e}")
            multi_error_codes.append("NCCL_MULTI1003")  # GPU count check failed
            return {"nodes": nodes, "busbw": "N/A", "passed": "N/A", "multi_error_codes": multi_error_codes}

    cmd = (
        f"mpirun -np {len(nodes)} -host {','.join(nodes)} "
        "/opt/nccl-tests/build/all_reduce_perf -b 8 -e 4G -f 2 -g 1"
    )
    log_path = os.path.join(RESULT_DIR, "nccl_multi.log")
    logging.debug(f"Multi-node NCCL cmd: {cmd}")
    try:
        with open(log_path, "w", encoding="utf-8") as f:
            subprocess.run(
                ["bash", "-lc", f"source /etc/profile.d/modules.sh && module load mpi/openmpi && {cmd}"],
                stdout=f,
                stderr=subprocess.STDOUT,
                timeout=180
            )
    except Exception as e:
        logging.error(f"{NODE_NAME}: NCCL multi-node execution failed: {e}")
        multi_error_codes.append("NCCL_MULTI1004")  # Test execution failure
        return {"nodes": nodes, "busbw": "N/A", "passed": False, "multi_error_codes": multi_error_codes}

    # Process and save test result data
    busbw = None
    try:
        with open(log_path, encoding="utf-8") as f:
            for line in f:
                if line.strip().startswith("4G"):
                    parts = line.split()
                    try:
                        busbw = float(parts[6])
                    except Exception:
                        pass
                    break
    except Exception as e:
        logging.error(f"{NODE_NAME}: Failed to parse multi-node log: {e}")
        multi_error_codes.append("NCCL_MULTI1005")  # Log parsing failure

    if busbw is None:
        multi_error_codes.append("NCCL_MULTI1006")  # Failed to acquire bandwidth
        return {"nodes": nodes, "busbw": "N/A", "passed": False, "multi_error_codes": multi_error_codes}

    passed = busbw >= NCCL_MULTI_BW_THRESHOLD
    if not passed:
        multi_error_codes.append("NCCL_MULTI1007")  # Bandwidth threshold not reached

    logging.info(f"{NODE_NAME}: NCCL multi-node {'PASSED' if passed else 'FAILED'}: busbw={busbw}, errors={multi_error_codes}")
    return {"nodes": nodes, "busbw": busbw, "passed": passed, "multi_error_codes": multi_error_codes}


# =============================
# Save or load result JSON
# =============================
def save_result(res):
    with open(RESULT_FILE,"w") as f:
        json.dump(res, f, indent=2)
    logging.info(f"Save Result: {RESULT_FILE}")


# =============================
# Main execution entry
# =============================
def main():
    # Optionally upgrade NHC scripts
    first_run_time = datetime.now().isoformat()
    # Optionally upgrade NHC scripts
    if NHC_UPGRADE:
        upgrade_nhc()

    # Process and save test result data
    prev_path = os.environ.get("PREVIOUS_RESULT_PATH", "")
    if os.path.exists(RESULT_FILE):
        try:
            prev = json.load(open(RESULT_FILE))
            reboot_count = int(prev.get("reboot_count", 0))
            logging.debug(f"Loaded previous reboot_count={reboot_count}")
        except Exception:
            reboot_count = 0
    else:
        reboot_count = 0

    if prev_path and os.path.exists(prev_path):
        try:
            prev = json.load(open(prev_path, "r", encoding="utf-8"))
            reboot_count = int(prev.get("reboot_count", 0))
        except Exception:
            reboot_count = 0
    ENABLE = os.environ.get("ENABLE_REBOOT_ON_FAILURE", "false").lower() in ("1", "true", "yes")
    MAXR = int(os.environ.get("MAX_REBOOT_COUNT", "0"))

    iteration = 0
    while True:
        iteration += 1
        logging.debug(f"[Loop {iteration}] Starting health check iteration, reboot_count={reboot_count}")

        # Run NCCL or NHC test
        gpu_ok, gpu_detail, physical, vmname = run_gpu_health_check()
        nhc_err_codes = gpu_detail.get("nhc_error_codes", [])

        nccl_ok, nccl_bw, nccl_err_codes = run_nccl_test()
        # Skip display if skipped due to lack of GPU, etc.
        nccl_display = (
            "Skip" if nccl_ok is None
            else ("Passed" if nccl_ok is True else "Failed")
        )
        nccl_bw_display = (
            "N/A" if nccl_bw is None
            else f"{nccl_bw:.2f}"
        )
        if nccl_ok is None:
            nccl_err_codes = []  # Do not treat Skip as an error

        multi = run_nccl_multi_node_test()
        multi_ok = multi.get("passed")
        multi_err_codes = multi.get("multi_error_codes", [])
        # Format multi-node NCCL test result for display
        multi_display = (
            "Skip" if multi_ok == "N/A"
            else ("Passed" if multi_ok else "Failed")
        )
        multi_bw = multi.get("busbw")
        multi_bw_display = (
            "N/A" if not isinstance(multi_bw, (int, float))
            else f"{multi_bw:.2f}"
        )
        if multi_ok == "N/A":
            multi_err_codes = []

        # 2) Calculate overall excluding None/“N/A”
        checks = [gpu_ok]
        if nccl_ok is not None:
            checks.append(nccl_ok)
        if multi_ok != "N/A":
            checks.append(multi_ok)

        overall = all(checks)
        logging.debug(f"gpu_ok={gpu_ok}, nccl_ok={nccl_ok}, overall={overall}")
        logging.debug(f"ENABLE={ENABLE}, reboot_count={reboot_count}, MAXR={MAXR}")

        # Process and save test result data
        result = {
            "node":                 NODE_NAME,
            "timestamp":            TIMESTAMP,

            # GPU
            "gpu_check":            gpu_ok,
            "gpu_detail":           gpu_detail,      # {"log": "..."}
            "error_codes":          nhc_err_codes + nccl_err_codes + multi_err_codes,

            # NCCL Status for Single HTML
            "nccl_status": "Skip" if nccl_ok is None
                else ("Passed" if nccl_ok else "Failed"),
            # Enable N/A display in HTML layer
            "nccl_bw": nccl_bw if isinstance(nccl_bw, (int, float)) else None,

            # NCCL Multi (already flattened)
            "nccl_multi_nodes":     multi["nodes"],
            # Enable N/A display in HTML layer
            "nccl_multi_bw": multi["busbw"] if isinstance(multi["busbw"], (int, float)) else None,
            # Return code
            "multi_status": (
                "Skip"   if multi["passed"] == "N/A" else
                "Passed" if multi["passed"]        else
                "Failed"
            ),

            # Handle reboot trigger and perform re-check
            "reboot_count":         reboot_count,

            # Return code
            "initial_returncode":  0 if overall else 1,
            "final_returncode":     0,

            # Version & Host Information
            "health_check_version": HEALTH_CHECK_VERSION,
            "physical_host_name":   physical,
            "vm_name":              vmname
        }

        # Handle reboot trigger and perform re-check
        if not overall and ENABLE and reboot_count < MAXR:
            # Save or load result JSON
            logging.debug(f"[Loop {iteration}] overall={overall}, going to reboot (count before={reboot_count})")
            Path("/tmp/reboot_required").touch()
            # Save or load result JSON
            reboot_count += 1
            save_result({**result, "reboot_count": reboot_count})
            logging.debug(f"[Loop {iteration}] Wrote intermediate JSON, reboot_count={reboot_count}")       
            # Wait for orchestrator to reboot us, then continue loop
            continue

        # normal / final case
        logging.debug(f"[Loop {iteration}] Final pass, reboot_count={reboot_count}")
        result["initial_returncode"] = 0
        result["final_returncode"]   = 0 if overall else 1
        result["reboot_count"]       = reboot_count
        # get resource id
        result["impactedResourceId"] = fetch_imds_resource_id()
        save_result(result)
        logging.debug(f"[Loop {iteration}] Wrote final JSON, exiting")
        break

if __name__ == "__main__":
    main()
