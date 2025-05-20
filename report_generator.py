import os
import csv
import subprocess, socket, shutil
import logging
import requests
from datetime import datetime
from report_generator import HEALTH_CHECK_VERSION 

# =============================
# Optionally upgrade NHC scripts
# =============================
def upgrade_nhc():
    """ NHC Upgrade (optional) """
    nhc_path = "/opt/azurehpc/test/azurehpc-health-checks"
    try:
        if os.path.isdir(nhc_path):
            shutil.rmtree(nhc_path)
        os.makedirs(os.path.dirname(nhc_path), exist_ok=True)
        subprocess.run([
            "git","clone",
            "https://github.com/Azure/azurehpc-health-checks.git",
            "-b", HEALTH_CHECK_VERSION,
            nhc_path
        ], check=True)
        # Flag fixes
        run_script = os.path.join(nhc_path, "run-health-checks.sh")
        subprocess.run(["sed","-i","s/--runtime=nvidia/--gpus all/g", run_script], check=True)
        # Docker image pull
        pull_script = os.path.join(nhc_path, "dockerfile/pull-image-mcr.sh")
        subprocess.run(["chmod","+x", pull_script], check=True)
        subprocess.run([pull_script], check=True)
        logging.info(f"NHC upgrade completed: v{HEALTH_CHECK_VERSION}")
    except Exception as e:
        logging.warning(f"NHC Upgrade Failure: {e}")


# =============================
# Retrieve VM metadata or resource ID
# =============================
def fetch_imds_resource_id() -> str:
    url = "http://169.254.169.254/metadata/instance/compute/resourceId"
    params = {"api-version": "2021-02-01", "format": "text"}
    resp = requests.get(url, headers={"Metadata": "true"}, params=params, timeout=2)
    resp.raise_for_status()
    return resp.text  # Full ARM resource ID


# =============================
# Output to CSV
# =============================
def write_csv_summary(rows, csv_path):
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        for row in rows:
            writer.writerow(row)
    logging.info(f"CSV output: {csv_path}")

# =============================
# Output to HTML
# =============================
def write_html_summary(title, headers, rows, html_path):
    os.makedirs(os.path.dirname(html_path), exist_ok=True)
    with open(html_path, 'w', encoding='utf-8') as f:
        f.write(f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>{title}</title>
  <style>
    table {{ border-collapse: collapse; width: 100%; }}
    th, td {{ border: 1px solid #ccc; padding: 6px; text-align: center; }}
    tr.ssh-fail td {{ background-color: #f2f2f2; color: #888; }}
    td.all-success {{ color: green; font-weight: bold; }}
    td.fail {{ color: red; font-weight: bold; }}
  </style>
</head>
<body>
  <h1>{title}</h1>
  <table>
    <tr>""")
        for h in headers:
            f.write(f"<th>{h}</th>")
        f.write("</tr>\n")

        for row in rows:
            final = row[-1]
            tr_class = ' class="ssh-fail"' if final == "SSH Fail" else ""
            f.write(f"    <tr{tr_class}>")
            for i, cell in enumerate(row):
                td_class = ''
                if i == len(row) - 1:
                    if cell == "All_Success":
                        td_class = ' class="all-success"'
                    elif cell == "Fail":
                        td_class = ' class="fail"'
                f.write(f"<td{td_class}>{cell}</td>")
            f.write("</tr>\n")

        f.write("""  </table>
</body>
</html>""")
    logging.info(f"HTML output: {html_path}")

# =============================
# Filters for reports
# =============================
def filter_summary_for_report(results, scheduler_node):
    return [r for r in results if r.get("node") != scheduler_node]

# =============================
# Process and save test result data
# =============================
def summarize_and_output(results, result_dir):
    """
    Generates CSV and HTML from a list of results in memory.
    The HTML outputs a table with timestamps outside the table and in fixed column order.
    final_returncode == 255 prints SSH Fail.
    """
    
    os.makedirs(result_dir, exist_ok=True)
    # Retrieve VM metadata or resource ID
    ts = results[0].get("timestamp", "")
    host = socket.gethostname()

    headers = [
        "node",
        "GPU NHC",           # GPU check result (Success/Fail)
        "NCCL Single Status",
        "NCCL Single BW",
        "NCCL Multi Status",
        "NCCL Multi BW",
        "Initial Result",    # First time overall result All_Success / SSH Fail / Fail
        "Reboot",            # Reboot_count
        "Final Result"       # All_Success / SSH Fail / Fail
    ]

    # Format each line (data lines only)
    rows = []
    for entry in results:
        node = entry.get("node", "")
        ts_entry = entry.get("timestamp", "")

        # GPU check is initial_returncode==0 -> Success
        gpu_ok = (entry.get("initial_returncode", 1) == 0)
        gpu_str = "Success" if gpu_ok else "Fail"

        # NCCL Standalone
        nccl_status = entry.get("nccl_status", "N/A")
        nccl_bw     = entry.get("nccl_bw")
        nccl_bw_str = f"{nccl_bw:.2f} GB/s" if isinstance(nccl_bw, (int, float)) else "N/A"

        # NCCL Multi Node
        multi_status = entry.get("multi_status", "N/A")
        multi_bw     = entry.get("nccl_multi_bw")
        multi_bw_str = f"{multi_bw:.2f} GB/s" if isinstance(multi_bw, (int, float)) else "N/A"

        # Process and save test result data
        init_rc = entry.get("initial_returncode", 1)
        if init_rc == 0:
            init_str = "All_Success"
        elif init_rc == 255:
            init_str = "SSH Fail"
        else:
            init_str = "Fail"

        # Number of Reboot
        reboot = entry.get("reboot_count", 0)

        # Process and save test result data
        frc = entry.get("final_returncode", 1)
        if frc == 0:
            final_str = "All_Success"
        elif frc == 255:
            final_str = "SSH Fail"
        else:
            final_str = "Fail"

        rows.append([
            node,
            gpu_str,
            nccl_status,
            nccl_bw_str,
            multi_status,
            multi_bw_str,
            init_str,
            str(reboot),
            final_str
        ])

    csv_path  = os.path.join(result_dir, "hpcai_gpu_check_summary.csv")
    html_path = os.path.join(result_dir, "hpcai_gpu_check_summary.html")
    # CSV is header + data
    write_csv_summary([headers] + rows, csv_path)

    write_html_summary(
        title=f"HPC Check Summary ({ts})",
        headers=headers,
        rows=rows,
        html_path=html_path
    )

    logging.info(f"HTML Output: {html_path}")
    logging.info("Report tabulation and output is complete")


# =============================
# Send Teams notification if failed nodes exist
# =============================
def notify_teams_failed_nodes(failed_nodes, webhook_url):
    if not failed_nodes:
        logging.info("All nodes passed")
        return

    text_lines = ["**❌ List of Failure nodes**"]
    for node in failed_nodes:
        bw = node.get("nccl_bandwidth_gbps")
        bw_str = f"{bw:.1f} GB/s" if isinstance(bw, (int, float)) else "N/A"
        line = f"- {node['node']}: GPU={'OK' if node['gpu_check'] else 'NG'}, NCCL={bw_str}"
        text_lines.append(line)

    payload = {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "summary": "HPC check results",
        "themeColor": "FF0000",
        "title": f"List of HPC Check NG Nodes ({datetime.utcnow().isoformat()})",
        "text": "\n".join(text_lines)
    }

    try:
        resp = requests.post(webhook_url, json=payload)
        resp.raise_for_status()
        logging.info("Teams notification sent")
    except Exception as e:
        logging.warning(f"Teams notification failure: {e}")