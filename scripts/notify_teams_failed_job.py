#!/usr/bin/env python3
imort sys
import json
import requests
from datetime import datetime

WEBHOOK_URL = "https://outlook.office.com/webhook/..."  # Replace with your actual URL

def notify_teams(job_id, user, exit_code, nodes):
    message = {
        "@type": "MessageCard",
        "@context": "https://schema.org/extensions",
        "themeColor": "FF0000",
        "summary": f"Slurm Job {job_id} Failed",
        "sections": [{
            "activityTitle": f" Slurm Job Failure Detected",
            "facts": [
                {"name": "Job ID", "value": job_id},
                {"name": "User", "value": user},
                {"name": "Exit Code", "value": str(exit_code)},
                {"name": "Nodes", "value": ', '.join(nodes)},
                {"name": "Time", "value": datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
            ],
            "markdown": True
        }]
    }

    response = requests.post(WEBHOOK_URL, json=message)
    if response.status_code == 200:
        print(f"[INFO] Teams notification sent successfully.")
    if response.status_code != 200:
        print(f"[ERROR] Failed to send Teams notification: {response.status_code}, {response.text}", file=sys.stderr)

# Example usage
if __name__ == "__main__":
    job_id = sys.argv[1]
    user = sys.argv[2]
    exit_code = sys.argv[3]
    nodes = sys.argv[4:]  # passed as list
    notify_teams(job_id, user, exit_code, nodes)p
