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

  /usr/bin/python3 /shared/slurm/scripts/notify_teams_failed_job.py \
    "$SLURM_JOB_ID" "$SLURM_JOB_USER" "$SLURM_JOB_EXIT_CODE" $NODE_LIST
fi
