# app.py
import streamlit as st
import pandas as pd
import re
import time
import json
import os
import uuid
import csv
from datetime import datetime, date
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from email.utils import formataddr
import smtplib
from urllib.parse import quote_plus

# BeautifulSoup for link tracking
try:
    from bs4 import BeautifulSoup
    BS4_AVAILABLE = True
except ImportError:
    BS4_AVAILABLE = False

# Quill for rich text editor
try:
    from streamlit_quill import st_quill
    QUILL_AVAILABLE = True
except Exception:
    QUILL_AVAILABLE = False

# -----------------------
# Config / Constants
# -----------------------
st.set_page_config(page_title="Bulk Multi-Gmail Sender", layout="wide")
st.title("📨 Bulk Multi-Gmail Sender (Private Repo / Private Use)")

# Filenames
SENT_LOG_CSV = "sent_log.csv"
SENT_COUNTERS_JSON = "sent_counters.json"
MAP_UUID_CSV = "uuid_map.csv"

# Default daily limit
DEFAULT_DAILY_LIMIT = 450

# -----------------------
# Helper utils
# -----------------------
def is_valid_email(email: str) -> bool:
    if not email or not isinstance(email, str):
        return False
    pattern = r"^[a-zA-Z0-9._%+\-']+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$"
    return re.match(pattern, email) is not None

def load_accounts_from_file(path: str):
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def sanitize_recipients(raw_list):
    cleaned = []
    seen = set()
    for r in raw_list:
        r = (r or "").strip().lower()
        if r and is_valid_email(r) and r not in seen:
            seen.add(r)
            cleaned.append(r)
    return cleaned

def ensure_sent_counters(accounts):
    if not os.path.exists(SENT_COUNTERS_JSON):
        counters = {acc["email"]: {"date": str(date.today()), "sent_today": 0} for acc in accounts}
        with open(SENT_COUNTERS_JSON, "w", encoding="utf-8") as f: json.dump(counters, f, indent=2)
    else:
        with open(SENT_COUNTERS_JSON, "r", encoding="utf-8") as f: counters = json.load(f)
        changed = False
        for acc in accounts:
            if acc["email"] not in counters:
                counters[acc["email"]] = {"date": str(date.today()), "sent_today": 0}
                changed = True
        if changed:
            with open(SENT_COUNTERS_JSON, "w", encoding="utf-8") as f: json.dump(counters, f, indent=2)

def read_sent_counters():
    if not os.path.exists(SENT_COUNTERS_JSON): return {}
    with open(SENT_COUNTERS_JSON, "r", encoding="utf-8") as f: return json.load(f)

def update_sent_counter(email_address, delta=1):
    counters = read_sent_counters()
    today_str = str(date.today())
    if email_address not in counters or counters[email_address].get("date") != today_str:
        counters[email_address] = {"date": today_str, "sent_today": 0}
    counters[email_address]["sent_today"] += delta
    with open(SENT_COUNTERS_JSON, "w", encoding="utf-8") as f: json.dump(counters, f, indent=2)

def get_sent_today(email_address):
    counters = read_sent_counters()
    today_str = str(date.today())
    if email_address not in counters or counters[email_address].get("date") != today_str: return 0
    return counters[email_address].get("sent_today", 0)

def append_sent_log(row_dict):
    file_exists = os.path.exists(SENT_LOG_CSV)
    with open(SENT_LOG_CSV, "a", newline='', encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=list(row_dict.keys()))
        if not file_exists: writer.writeheader()
        writer.writerow(row_dict)

def map_uuid_save(uuid_str, recipient, account_email):
    file_exists = os.path.exists(MAP_UUID_CSV)
    with open(MAP_UUID_CSV, "a", newline='', encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        if not file_exists: writer.writerow(["uuid", "recipient", "account", "timestamp"])
        writer.writerow([uuid_str, recipient, account_email, datetime.utcnow().isoformat()])

# -----------------------
# UI: Load accounts & Sidebar
# -----------------------
st.sidebar.header("Accounts (private.json or upload)")
accounts_json_info = """
Format: `[{"email": "...", "password": "...", "name": "..."}, ...]`
"""
st.sidebar.markdown(accounts_json_info)

accounts = None
if os.path.exists("accounts.json"):
    try:
        accounts = load_accounts_from_file("accounts.json")
        st.sidebar.success("Loaded accounts from local accounts.json")
    except Exception as e:
        st.sidebar.error(f"Failed to load accounts.json: {e}")

uploaded_accounts = st.sidebar.file_uploader("Or upload accounts.json", type=["json"])
if uploaded_accounts:
    try:
        accounts = json.load(uploaded_accounts)
        st.sidebar.success("Loaded uploaded accounts JSON")
    except Exception as e:
        st.sidebar.error(f"Invalid JSON file: {e}")
        accounts = None

if not accounts:
    st.warning("No Gmail accounts loaded. Add or upload accounts.json.")
    st.stop()

valid_accounts = [a for a in accounts if "email" in a and "password" in a and "name" in a]
if not valid_accounts:
    st.error("No valid accounts found in accounts JSON.")
    st.stop()

ensure_sent_counters(valid_accounts)

st.sidebar.header("Sending controls")
daily_limit_per_account = st.sidebar.number_input("Daily limit per account", min_value=1, value=DEFAULT_DAILY_LIMIT)
sleep_seconds = st.sidebar.number_input("Seconds between emails", min_value=0.0, value=1.0, step=0.1)

st.sidebar.markdown("### Accounts summary")
for acc in valid_accounts:
    sent_today = get_sent_today(acc["email"])
    st.sidebar.write(f"- {acc['email']} — sent: {sent_today}/{daily_limit_per_account}")

# -----------------------
# Compose email UI
# -----------------------
st.header("Compose Email")
subject = st.text_input("Subject")

if 'email_body' not in st.session_state: st.session_state.email_body = ""

editor_mode = st.radio("Editor Mode", ["Visual (Recommended)", "HTML (Advanced)"], horizontal=True, key="editor_mode")

if editor_mode == "Visual (Recommended)":
    if QUILL_AVAILABLE:
        content = st_quill(value=st.session_state.email_body, key="quill_editor", placeholder="Write email body...", html=True)
        if content != st.session_state.email_body:
            st.session_state.email_body = content
            st.rerun()
else:
    content = st.text_area("Edit Raw HTML", value=st.session_state.email_body, key="html_editor", height=400)
    if content != st.session_state.email_body:
        st.session_state.email_body = content
        st.rerun()

body_html = st.session_state.email_body
st.markdown("**Tip**: Use `[Recipient Name]` for personalization.")
uploaded_attach = st.file_uploader("Optional: Attach file", accept_multiple_files=False)

# -----------------------
# Recipients UI
# -----------------------
st.subheader("Recipients")
col1, col2 = st.columns([2,1])
with col1:
    uploaded_recipients = st.file_uploader("Upload CSV/Excel/TXT", type=["csv", "xlsx", "txt"])
    pasted = st.text_area("Or paste emails (one per line):", height=150)
with col2:
    st.markdown("Options")
    personalize = st.checkbox("Personalize with recipient name", value=True)

recipients, recipient_name_map = [], {}
if uploaded_recipients:
    try:
        if uploaded_recipients.name.endswith((".csv", ".txt")):
            df = pd.read_csv(uploaded_recipients, header=None, dtype=str, keep_default_na=False)
        else:
            df = pd.read_excel(uploaded_recipients, header=None, dtype=str)
        
        emails = [str(e).strip() for e in df.iloc[:, 0].tolist()]
        recipients.extend(emails)
        
        if personalize and df.shape[1] >= 2:
            names = [str(n).strip() for n in df.iloc[:, 1].tolist()]
            for email, name in zip(emails, names):
                if is_valid_email(email) and name:
                    recipient_name_map[email.lower()] = name
    except Exception as e:
        st.error(f"Failed to parse uploaded file: {e}")

if pasted:
    recipients.extend([line.strip() for line in pasted.splitlines() if line.strip()])

recipients = sanitize_recipients(recipients)
st.success(f"Loaded {len(recipients)} unique valid recipients")

# -----------------------
# Tracking UI
# -----------------------
st.subheader("Tracking")
st.markdown("Tracking requires a separate tracker server. A sample `tracker_server.py` is provided.")
enable_open_tracking = st.checkbox("Enable open tracking (invisible pixel)", value=True)
enable_click_tracking = st.checkbox("Enable link click tracking", value=True)
tracker_base_url = st.text_input("Tracker Base URL", "http://127.0.0.1:5001")

if (enable_open_tracking or enable_click_tracking) and not tracker_base_url:
    st.warning("Please provide the Tracker Base URL to enable tracking.")
    st.stop()

# -----------------------
# Send Logic
# -----------------------
def process_html_for_tracking(html_content, base_url, uuid, recipient_email):
    """Adds tracking pixel and rewrites links for click tracking."""
    # Open Tracking Pixel
    pixel_url = f"{base_url}/track.png?id={uuid}"
    pixel_img = f'<img src="{pixel_url}" width="1" height="1" style="display:none; border:0;" alt=""/>'
    
    # Click Tracking
    if BS4_AVAILABLE and enable_click_tracking:
        soup = BeautifulSoup(html_content, 'html.parser')
        links = soup.find_all('a', href=True)
        for link in links:
            original_url = link['href']
            # Avoid tracking mailto links or already tracked links
            if original_url.startswith('mailto:') or 'url=' in original_url:
                continue
            
            encoded_url = quote_plus(original_url)
            tracking_url = f"{base_url}/click?id={uuid}&url={encoded_url}"
            link['href'] = tracking_url
        
        # Add pixel to the end of the body
        if soup.body:
            soup.body.append(BeautifulSoup(pixel_img, 'html.parser'))
            return str(soup)
        else:
            # If no body tag, just append everything
            return str(soup) + pixel_img
    
    # Fallback if BeautifulSoup is not available or click tracking disabled
    return html_content + pixel_img

def build_message(sender_name, sender_email, to_email, subject, html_body, attach_file=None, uuid_id=None):
    msg = MIMEMultipart('related')
    msg['From'] = formataddr((sender_name, sender_email))
    msg['To'] = to_email
    msg['Subject'] = subject
    
    # --- FIXED ALIGNMENT and ADDED TRACKING ---
    final_html = html_body
    
    # Apply tracking if enabled
    if tracker_base_url and uuid_id:
        if enable_open_tracking or enable_click_tracking:
             final_html = process_html_for_tracking(html_body, tracker_base_url, uuid_id, to_email)

    # Wrap in a non-centering div for consistent left alignment and padding
    # Only wrap if it's not a full HTML document already
    if '<html' not in final_html.lower():
        final_html = f"""
        <div style="font-family: Arial, sans-serif; font-size: 16px; line-height: 1.6; color: #333333; padding: 20px;">
            {final_html}
        </div>
        """
    
    msg.attach(MIMEText(final_html, 'html', 'utf-8'))

    if attach_file:
        part = MIMEBase('application', 'octet-stream')
        attach_file.seek(0)
        part.set_payload(attach_file.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename="{attach_file.name}"')
        msg.attach(part)
    return msg

def send_via_smtp(account, msg, to_email):
    try:
        with smtplib.SMTP('smtp.gmail.com', 587, timeout=60) as server:
            server.starttls()
            server.login(account["email"], account["password"])
            server.sendmail(account["email"], [to_email], msg.as_string())
        return True, None
    except Exception as e:
        return False, str(e)

if st.button("🚀 Send Emails"):
    if not subject or not body_html or not recipients:
        st.error("Please provide subject, body, and recipients.")
    else:
        total_sent = 0
        status_rows = []
        progress = st.progress(0)
        status_placeholder = st.empty()

        account_idx = 0
        for i, recipient in enumerate(recipients):
            account = None
            for _ in range(len(valid_accounts)):
                candidate = valid_accounts[account_idx % len(valid_accounts)]
                if get_sent_today(candidate["email"]) < daily_limit_per_account:
                    account = candidate
                    account_idx += 1
                    break
                account_idx += 1
            
            if account is None:
                st.error("All accounts have hit their daily limit. Stopping.")
                break
            
            to_name = recipient_name_map.get(recipient, "")
            personalized_body = body_html.replace("[Recipient Name]", to_name)
            
            uuid_id = str(uuid.uuid4())
            map_uuid_save(uuid_id, recipient, account["email"])
            
            msg = build_message(
                account["name"], account["email"], recipient, subject,
                personalized_body, uploaded_attach, uuid_id
            )
            
            ok, err = send_via_smtp(account, msg, recipient)
            
            row = { "timestamp": datetime.utcnow().isoformat(), "recipient": recipient, "account": account["email"], "uuid": uuid_id, "status": "sent" if ok else "failed", "error": str(err) if err else "" }
            append_sent_log(row)
            status_rows.append(row)

            if ok:
                update_sent_counter(account["email"])
                total_sent += 1

            progress.progress((i + 1) / len(recipients))
            status_placeholder.text(f"Sending {i+1}/{len(recipients)} to {recipient}... Status: {'OK' if ok else 'FAIL'}")
            time.sleep(float(sleep_seconds))

        status_placeholder.empty()
        st.success(f"Send loop finished. Sent {total_sent} emails.")
        st.dataframe(pd.DataFrame(status_rows))
        
        with open(SENT_LOG_CSV, "rb") as f:
            st.download_button("Download send log (CSV)", data=f, file_name=SENT_LOG_CSV)
        if os.path.exists(MAP_UUID_CSV):
            with open(MAP_UUID_CSV, "rb") as f:
                st.download_button("Download UUID map (CSV)", data=f, file_name=MAP_UUID_CSV)
