"""
health_manager package 
Comprehensive management of Slurm cluster health checks, automatic recovery, reporting, and GHR integration.
"""

from .cluster_health_orchestrator import main as orchestrate_cluster_health
from .node_health_check_runner import (
    run_gpu_health_check,
    run_nccl_test,
    run_nccl_multi_node_test,
    save_result
)
from .report_generator import (
    summarize_and_output,
    write_csv_summary,
    write_html_summary,
    notify_teams_failed_nodes
)
from .remote_node_utils import (
    wait_for_ssh,
    copy_script_to_node,
    distribute_scripts_parallel,
    fetch_remote_json,
    get_remote_context,
    handle_reboot_and_recheck
)
from .ghr_submission_controller import run_ghr_if_needed
from .ghr_payload_utils import (
    build_ghr_payload,
    submit_ghr_request,
    record_ghr_log,
    get_current_timestamps
)
from .health_manager_config import (
    TIMESTAMP,
    NODE_NAME,
    BASE_DIR,
    RESULT_DIR,
    RESULT_FILE
)
