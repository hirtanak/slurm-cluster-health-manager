import os
import json
import socket
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

# =============================
# Timestamp
# =============================
# Start with env. If not, use current JST time.
LOCAL_TIMESTAMP = os.getenv(
    "CHECK_TIMESTAMP",
    datetime.now(ZoneInfo("Asia/Tokyo")).strftime("%Y%m%d-%H%M")
)
# Always get UTC in iso8601Z format for API/logging
UTC_TIMESTAMP = datetime.now(timezone.utc).replace(microsecond=0) \
    .isoformat().replace("+00:00", "Z")

# Compatible aliases
TIMESTAMP = LOCAL_TIMESTAMP

# =============================
# Directory settings
# =============================
USER_HOME      = os.path.expanduser("~")
BASE_DIR       = os.path.join(USER_HOME, "health_results", LOCAL_TIMESTAMP)
os.makedirs(BASE_DIR, exist_ok=True)
# CHECK_RESULT_DIR is used for node-side scripts
os.environ["CHECK_RESULT_DIR"] = BASE_DIR
# For scripts that retrieve JSON from shared directories
RESULT_DIR     = BASE_DIR
NODE_NAME      = socket.gethostname()
RESULT_FILE    = os.path.join(RESULT_DIR, f"hpc_check_result_{NODE_NAME}.json")
RECHECK_INTERVAL_SECONDS = int(os.getenv("RECHECK_INTERVAL_SECONDS", "30"))

# =============================
# SSH / concurrency settings
# =============================
PREFIX           = os.getenv("NODE_PREFIX", "slurm00-htc")
NODE_COUNT       = int(os.getenv("NODE_COUNT", "2"))
MAX_PARALLEL     = int(os.getenv("MAX_PARALLEL", "10"))
MAX_REBOOT_COUNT = int(os.getenv("MAX_REBOOT_COUNT", "1"))

# =============================
# NHC settings
# =============================
HEALTH_CHECK_SCRIPT  = os.getenv(
    "HEALTH_CHECK_SCRIPT",
    "/opt/azurehpc/test/azurehpc-health-checks/run-health-checks.sh"
)
HEALTH_CHECK_VERSION = os.getenv("HEALTH_CHECK_VERSION", "v0.4.4")
NHC_UPGRADE          = os.getenv("NHC_UPGRADE", "false").lower() in ("1","true","yes")

# =============================
# NCCL threshold settings
# =============================
NCCL_BW_THRESHOLD       = float(os.getenv("NCCL_BW_THRESHOLD", "480.0"))
NCCL_MULTI_BW_THRESHOLD = float(os.getenv("NCCL_MULTI_BW_THRESHOLD", "350.0"))

# =============================
# GHR settings
# =============================
ENABLE_GHR            = os.getenv("ENABLE_GHR", "true").lower() == "true"
GHR_METHOD            = os.getenv("GHR_METHOD", "az")
GHR_IMPACT_CATEGORY   = os.getenv("GHR_IMPACT_CATEGORY", "NHC2001")
GHR_IMPACT_DESCRIPTION= os.getenv("GHR_IMPACT_DESCRIPTION", "")
GHR_ADDITIONAL_PROPERTIES = json.loads(os.getenv("GHR_ADDITIONAL_PROPERTIES", "{}"))
# Existing script reference compatibility
GHR_ADDITIONAL        = GHR_ADDITIONAL_PROPERTIES
GHR_MAX_RETRIES       = int(os.getenv("GHR_MAX_RETRIES", "3"))
GHR_RETRY_INTERVAL    = int(os.getenv("GHR_RETRY_INTERVAL_SEC", "5"))
GHR_SKIP_HOURS        = int(os.getenv("GHR_SKIP_HOURS", "24"))
GHR_MAX_NODES         = int(os.getenv("GHR_MAX_NODES", "10"))
GHR_IMDS_TIMEOUT      = float(os.getenv("GHR_IMDS_TIMEOUT", "5.0"))
AZURE_SUBSCRIPTION_ID = os.getenv("AZURE_SUBSCRIPTION_ID", None)

# =============================
# Log paths
# =============================
GHR_LOG_PATH = os.path.join(USER_HOME, "ghr_log.ndjson")

# =============================
# Environment variable export list
# =============================
EXPORT_ENV = {
    "NCCL_BW_THRESHOLD":       str(NCCL_BW_THRESHOLD),
    "NCCL_MULTI_BW_THRESHOLD": str(NCCL_MULTI_BW_THRESHOLD),
    "ENABLE_REBOOT_ON_FAILURE":os.getenv("ENABLE_REBOOT_ON_FAILURE", "true"),
    "MAX_REBOOT_COUNT":        str(MAX_REBOOT_COUNT),
    "NODE_PREFIX":             PREFIX,
    "NODE_COUNT":              str(NODE_COUNT),
    "HEALTH_CHECK_SCRIPT":     HEALTH_CHECK_SCRIPT,
    "HEALTH_CHECK_VERSION":    HEALTH_CHECK_VERSION,
    "NHC_UPGRADE":             str(NHC_UPGRADE).lower(),
}
for k, v in EXPORT_ENV.items():
    os.environ[k] = v
