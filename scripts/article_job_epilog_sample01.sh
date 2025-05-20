#!/bin/bash
set -euo pipefail
exec 2>> /sared/slurm/logs/epilog_job_errors.log

# Only process failed jobs
if [[ "${SLURM_JOB_EXIT_CODE:-1}" -ne 0 ]]; then
  echo "[FAIL] Job $SLURM_JOB_ID failed with code $SLURM_JOB_EXIT_CODE"

  # Extract NodeList
  scontrol show job "${SLURM_JOB_ID}" \
    | grep -oP 'NodeList=\K\S+' \
    | grep -v '^(null)$' \
    > "/shared/slurm/failed_jobs/failed_job_${SLURM_JOB_ID}.nodes"
  NODE_LIST=$(cat "/shared/slurm/failed_jobs/failed_job_${SLURM_JOB_ID}.nodes")

  # Trigger recovery orchestration (e.g., via Python script) #
  # Example 1: For scripts that do not require a job ID and check the entire cluster
  /usr/bin/python3 /shared/slurm/scripts/slurm_node_recovery_orchestrator.py
  # Example 2: To check only the nodes associated with the associated job ID (for scripts that require the job ID as an argument)
  /usr/bin/python3 /shared/slurm/scripts/slurm_node_recovery.py --job-id "$SLURM_JOB_ID"
fih
