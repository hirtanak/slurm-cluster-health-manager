# slurm-cluster-health-manager

A modular Python-based system for monitoring and managing the health of Slurm-managed GPU clusters. This tool automates diagnostics, failure detection, reboot orchestration, and generates visual reports and GHR submissions.

*Disclaimer**: This project, `slurm-cluster-health-manager`, is an internal tool developed for demonstration and reference purposes. It is **not an official Microsoft product** and is not supported or maintained by Microsoft.
---

## Features

- GPU health check using NHC
- NCCL single- and multi-node bandwidth tests
- SSH distribution and orchestration across nodes
- Automated reboots and post-reboot checks
- JSON, CSV, and HTML report generation
- Microsoft Teams notification integration
- Azure GHR (Global Health Reporting) submission
- Configurable thresholds and retry logic

---

## Directory Structure

```
health_manager/
├── cluster_health_orchestrator.py       # Entry point for orchestrating checks and recovery
├── node_health_check_runner.py          # Per-node check logic (NHC, NCCL)
├── ghr_submission_controller.py         # GHR control logic
├── ghr_payload_utils.py                 # GHR payload builder and logger
├── remote_node_utils.py                 # SSH and SCP utilities
├── report_generator.py                  # CSV and HTML reporting
├── health_manager_config.py             # Configuration and environment variables
├── __init__.py                          # Package exports
```

---

## Usage

```bash
# Run health checks on all nodes
python3 cluster_health_orchestrator.py
```

You can configure thresholds and behavior using environment variables or directly in `health_manager_config.py`.

---

## Configuration

| Variable                     | Description                              | Default      |
|-----------------------------|------------------------------------------|--------------|
| `NCCL_BW_THRESHOLD`         | NCCL single-node threshold in MB/s       | `480.0`      |
| `NCCL_MULTI_BW_THRESHOLD`   | NCCL multi-node threshold in MB/s        | `350.0`      |
| `MAX_REBOOT_COUNT`          | Max number of reboots per node           | `1`          |
| `ENABLE_GHR`                | Enable GHR submission                    | `true`       |

---

## Requirements

- Python 3.8+
- Slurm-managed cluster
- SSH access between nodes
- Optional: Microsoft Teams Webhook URL
- Optional: Azure Health Reporting permissions

---

## License

MIT License

---

## Author

Hiroshi Tanaka and contributors.
