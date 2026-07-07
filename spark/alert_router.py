#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
SDOQAP Automated Alert Router
==============================
Routes quality and system alerts to external messaging platforms
like LINE Notify or Slack when critical events occur.
"""

import os
import sys
import json
import requests

# Load tokens from environment variables
LINE_NOTIFY_TOKEN = os.getenv("LINE_NOTIFY_TOKEN", "")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")

def send_slack_alert(title, message, severity="warning"):
    """Send alert to Slack Webhook."""
    if not SLACK_WEBHOOK_URL:
        return False
        
    color_map = {
        "critical": "#FF0000",
        "warning": "#FFA500",
        "info": "#0000FF"
    }
    
    payload = {
        "attachments": [
            {
                "fallback": f"[{severity.upper()}] {title}: {message}",
                "color": color_map.get(severity, "#808080"),
                "title": f"SDOQAP Observability: {title}",
                "text": message,
                "footer": "SDOQAP Alert Router",
                "ts": int(requests.utils.time.time())
            }
        ]
    }
    
    try:
        res = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=5)
        return res.status_code == 200
    except Exception as e:
        print(f"[ALERT ROUTER] Slack Send Error: {e}")
        return False

def send_line_alert(title, message, severity="warning"):
    """Send alert to LINE Notify."""
    if not LINE_NOTIFY_TOKEN:
        return False
        
    url = "https://notify-api.line.me/api/notify"
    headers = {"Authorization": f"Bearer {LINE_NOTIFY_TOKEN}"}
    
    # Format message for LINE
    emoji = "🚨" if severity == "critical" else "⚠️" if severity == "warning" else "ℹ️"
    line_message = f"\n{emoji} [{severity.upper()}] {title}\n----------------------\n{message}"
    
    payload = {"message": line_message}
    
    try:
        res = requests.post(url, headers=headers, data=payload, timeout=5)
        return res.status_code == 200
    except Exception as e:
        print(f"[ALERT ROUTER] LINE Send Error: {e}")
        return False

def route_alert(title, message, severity="warning"):
    """Route alert payload to all active notification channels."""
    print(f"[ALERT ROUTER] Routing alert: {title} ({severity})")
    
    slack_success = send_slack_alert(title, message, severity)
    line_success = send_line_alert(title, message, severity)
    
    if slack_success:
        print("  - Alert successfully routed to Slack.")
    if line_success:
        print("  - Alert successfully routed to LINE Notify.")
        
    # Return True if at least one channel succeeded or if no channels are configured
    if not SLACK_WEBHOOK_URL and not LINE_NOTIFY_TOKEN:
        print("  - No external channels configured (LINE_NOTIFY_TOKEN / SLACK_WEBHOOK_URL). Alert logged locally.")
        return True
        
    return slack_success or line_success

if __name__ == "__main__":
    # Allow command line trigger
    if len(sys.argv) >= 3:
        title_arg = sys.argv[1]
        msg_arg = sys.argv[2]
        sev_arg = sys.argv[3] if len(sys.argv) > 3 else "warning"
        route_alert(title_arg, msg_arg, sev_arg)
    else:
        print("Usage: python alert_router.py <title> <message> [severity]")
        sys.exit(1)
