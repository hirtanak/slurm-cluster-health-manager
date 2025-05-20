import os
import json
import time
import logging
from datetime import datetime, timedelta, timezone
from ghr_payload_utils import (
    build_ghr_payload,
    submit_ghr_request,
    record_ghr_log,
    get_current_timestamps,
    GHR_LOG_PATH
)
from health_manager_config import (
    ENABLE_GHR,
    GHR_SKIP_HOURS,
    GHR_IMPACT_CATEGORY,
    GHR_IMPACT_DESCRIPTION,
    GHR_ADDITIONAL,
    GHR_MAX_NODES,
    GHR_MAX_RETRIES,
    GHR_RETRY_INTERVAL,
    GHR_METHOD
)

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')


def has_recent_success(hours: int = GHR_SKIP_HOURS) -> bool:
    """
    Check log file for most recent successful entry; compare by UTC time.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    if not os.path.exists(GHR_LOG_PATH):
        return False

    # Search from the newest to the oldest log entry
    with open(GHR_LOG_PATH, 'r', encoding='utf-8') as f:
        for line in reversed(f.readlines()):
            try:
                entry = json.loads(line)
                ts = datetime.fromisoformat(entry["timestamp"])
                if entry.get("status") == "success" and ts >= cutoff:
                    return True
            except Exception:
                continue
    return False

def run_ghr_if_needed(all_results: list[dict]) -> None:
    """
    GHR submission in batches once every 24h, only if a problematic node is found.
    - Submission frequency: GHR_SKIP_HOURS (hours)
    - Upper limit of number of nodes: GHR_MAX_NODES
    """
    utc, jst = get_current_timestamps()

    # Abort if GHR submission is disabled by config
    if not ENABLE_GHR:
        logging.info(f"[{utc}][{jst}] GHR disabled by config.")
        return

    # Skip if already succeeded within 24h
    if has_recent_success(GHR_SKIP_HOURS):
        logging.info(f"[{utc}][{jst}] Skipping GHR: recent success within {GHR_SKIP_HOURS}h.")
        return

    # Extract nodes with errors
    failed_nodes = []
    for entry in all_results:
        codes = (
            entry.get("nhc_error_codes", []) +
            entry.get("nccl_error_codes", []) +
            entry.get("multi_error_codes", [])
        )
        if codes:
            failed_nodes.append({"node": entry.get("node"), "errors": codes})

    # Exit if no node is found
    if not failed_nodes:
        logging.info(f"[{utc}][{jst}] No failure nodes, no GHR submission.")
        return

    # Trim the list if the number of failed nodes exceeds the allowed maximum
    if len(failed_nodes) > GHR_MAX_NODES:
        logging.warning(f"[{utc}][{jst}] Node count ({len(failed_nodes)}) exceeds GHR_MAX_NODES ({GHR_MAX_NODES}), trimming list.")
        failed_nodes = failed_nodes[:GHR_MAX_NODES]

    # Generate UTC timestamp for payload
    payload_ts = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')
    payload = build_ghr_payload(
        category=GHR_IMPACT_CATEGORY,
        description=GHR_IMPACT_DESCRIPTION,
        additional=GHR_ADDITIONAL,
        nodes=failed_nodes,
        timestamp=payload_ts
    )

    
    for attempt in range(1, GHR_MAX_RETRIES + 1):
        utc, jst = get_current_timestamps()
        try:
            res = submit_ghr_request(payload, method=GHR_METHOD)
            status = "success"
            logging.info(f"[{utc}][{jst}] GHR batch succeeded on attempt {attempt}.")
        except Exception as e:
            status = "failure"
            logging.warning(f"[{utc}][{jst}] GHR batch attempt {attempt} failed: {e}")
            time.sleep(GHR_RETRY_INTERVAL)
        finally:
            req_id = payload.get("properties", {}).get("requestId", "")
            record_ghr_log(status, req_id, failed_nodes)
        if status == "success":
            break

    utc, jst = get_current_timestamps()
    logging.info(f"[{utc}][{jst}] GHR process completed.")