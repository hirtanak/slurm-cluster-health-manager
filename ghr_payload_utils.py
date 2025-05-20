import os
import json
import logging
import functools
import requests
from datetime import datetime, timezone, timedelta

# =============================

# NHC_ERROR_CODES Mapping

# =============================
NHC_ERROR_CODES = {
    "NHC2001": "Resource.Hpc.Unhealthy.HpcGenericFailure",
    "NHC2002": "Resource.Hpc.Unhealthy.MissingIB",
    "NHC2003": "Resource.Hpc.Unhealthy.IBPerformance",
    "NHC2004": "Resource.Hpc.Unhealthy.IBPortDown",
    "NHC2005": "Resource.Hpc.Unhealthy.IBPortFlapping",
    "NHC2007": "Resource.Hpc.Unhealthy.HpcRowRemapFailure",
    "NHC2008": "Resource.Hpc.Unhealthy.HpcInforomCorruption",
    "NHC2009": "Resource.Hpc.Unhealthy.HpcMissingGpu",
    "NHC2010": "Resource.Hpc.Unhealthy.ManualInvestigation",
    "NHC2011": "Resource.Hpc.Unhealthy.XID95UncontainedECCError",
    "NHC2012": "Resource.Hpc.Unhealthy.XID94ContainedECCError",
    "NHC2013": "Resource.Hpc.Unhealthy.XID79FallenOffBus",
    "NHC2014": "Resource.Hpc.Unhealthy.XID48DoubleBitECC",
    "NHC2015": "Resource.Hpc.Unhealthy.UnhealthyGPUNvidiasmi",
    "NHC2016": "Resource.Hpc.Unhealthy.NvLink",
    "NHC2017": "Resource.Hpc.Unhealthy.HpcDcgmiThermalReport",
    "NHC2018": "Resource.Hpc.Unhealthy.ECCPageRetirementTableFull",
    "NHC2019": "Resource.Hpc.Unhealthy.DBEOverLimit",
    "NHC2020": "Resource.Hpc.Unhealthy.HpcGpuDcgmDiagFailure",
    "NHC2021": "Resource.Hpc.Unhealthy.GPUMemoryBWFailure",
    "NHC2022": "Resource.Hpc.Unhealthy.CPUPerformance",
}

# Log file path
GHR_LOG_PATH = os.path.expanduser("~/ghr_log.ndjson")

# GHR API settings (can be read from config.py, but variables can also be defined here)
GHR_ENDPOINT = os.environ.get("GHR_ENDPOINT", "https://ghr.example.com/api/v1/ghr")

# Timestamping utility
def get_current_timestamps() -> tuple[str, str]:
    """
    Returns a tuple of (utc_iso, jst_iso):
    - utc_iso: UTC timezone ISO8601 without fractional sec + 'Z'
    - jst_iso: JST timezone ISO8601
    """

    # UTC
    now_utc = datetime.now(timezone.utc).replace(microsecond=0)
    utc_iso = now_utc.isoformat().replace('+00:00', 'Z')

    # JST
    jst = now_utc.astimezone(timedelta(hours=9))
    jst_iso = jst.isoformat()
    return utc_iso, jst_iso

# Decorator: centralize logging and exception resubmission 
def log_and_reraise(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            utc, jst = get_current_timestamps()
            logging.error(f"[{utc}][{jst}] Exception in {func.__name__}: {e}")
            raise
    return wrapper

@log_and_reraise
def build_ghr_payload(category: str, description: str, additional: dict, nodes: list[dict], timestamp: str) -> dict:
    """
    Build payload to submit to GHR
    - category: impact category
    - description: detailed description
    - additional: other property dictionary
    - nodes: [{"node": name, "errors": [...]} , ...]
    - timestamp: UTC ISO8601 ‘Z’ format
    """
    properties = {
        "category": category,
        "description": description,
        **additional,
        "timestamp": timestamp,
        "nodes": nodes
    }

    # Generate requestId 
    properties["requestId"] = str(datetime.now(timezone.utc).timestamp()).replace('.', '')
    payload = {"properties": properties}
    return payload

@log_and_reraise
def submit_ghr_request(payload: dict, method: str = "POST") -> requests.Response:
    """
    Send request to GHR API and return response.
    """
    headers = {"Content-Type": "application/json"}
    if method.upper() not in ("POST", "PUT"):  # default to POST
        method = "POST"
    resp = requests.request(method, GHR_ENDPOINT, headers=headers, json=payload, timeout=30)
    resp.raise_for_status()
    return resp

@log_and_reraise
def record_ghr_log(status: str, request_id: str, nodes: list[dict]) -> None:
    """
    Append the result of sending GHR to the log file in NDJSON format.
    """
    utc, jst = get_current_timestamps()
    entry = {
        "timestamp": utc,
        "status": status,
        "requestId": request_id,
        "nodes": nodes
    }
    line = json.dumps(entry)

    # Append the GHR entry to the NDJSON log file
    with open(GHR_LOG_PATH, 'a', encoding='utf-8') as f:
        f.write(line + '\n')