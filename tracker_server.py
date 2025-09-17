# tracker_server.py
from flask import Flask, request, redirect, send_file
from datetime import datetime
import os
import io

app = Flask(__name__)

# Create a 1x1 transparent GIF
PIXEL_GIF = b'GIF89a\x01\x00\x01\x00\x80\x00\x00\xff\xff\xff\x00\x00\x00!\xf9\x04\x01\x00\x00\x00\x00,\x00\x00\x00\x00\x01\x00\x01\x00\x00\x02\x02D\x01\x00;'

LOG_DIR = "tracker_logs"
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

OPEN_LOG_FILE = os.path.join(LOG_DIR, "opens.log")
CLICK_LOG_FILE = os.path.join(LOG_DIR, "clicks.log")

def log_event(file_path, data):
    """Appends event data to a log file."""
    timestamp = datetime.utcnow().isoformat()
    log_entry = f"{timestamp} | {data}\n"
    with open(file_path, "a") as f:
        f.write(log_entry)
    print(f"Logged: {log_entry.strip()}")

@app.route('/track.png')
def track_open():
    """Logs an email open event."""
    email_uuid = request.args.get('id', 'N/A')
    
    log_data = {
        "event": "email_open",
        "uuid": email_uuid,
        "ip_address": request.remote_addr,
        "user_agent": request.headers.get('User-Agent', 'N/A')
    }
    log_event(OPEN_LOG_FILE, log_data)
    
    return send_file(io.BytesIO(PIXEL_GIF), mimetype='image/gif')

@app.route('/click')
def track_click():
    """Logs a link click event and redirects to the original URL."""
    email_uuid = request.args.get('id', 'N/A')
    original_url = request.args.get('url', '/') # Default to root if no URL
    
    log_data = {
        "event": "link_click",
        "uuid": email_uuid,
        "original_url": original_url,
        "ip_address": request.remote_addr,
        "user_agent": request.headers.get('User-Agent', 'N/A')
    }
    log_event(CLICK_LOG_FILE, log_data)
    
    # Redirect to the original URL
    return redirect(original_url)

if __name__ == '__main__':
    # You can change the port if needed
    # Make sure your firewall allows connections to this port
    app.run(host='0.0.0.0', port=5001, debug=True)
