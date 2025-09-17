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

# Quill for rich text editor
try:
    from streamlit_quill import st_quill
    QUILL_AVAILABLE = True
except Exception:
    QUILL_AVAILABLE = False

# -----------------------
# Config / Constants
# -----------------------
st.set_page_config(page_title="Bulk Multi-Provider Email Sender", layout="wide")
st.title("📨 Bulk Multi-Provider Email Sender")

SMTP_SETTINGS = {
    "gmail": {"host": "smtp.gmail.com", "port": 587},
    "yahoo": {"host": "smtp.mail.yahoo.com", "port": 587},
    "outlook": {"host": "smtp.office365.com", "port": 587},
    "aol": {"host": "smtp.aol.com", "port": 587},
    "protonmail": {"host": "127.0.0.1", "port": 1025}
}

SENT_LOG_CSV = "sent_log.csv"
SENT_COUNTERS_JSON = "sent_counters.json"
MAP_UUID_CSV = "uuid_map.csv"
DEFAULT_DAILY_LIMIT = 450

# -----------------------
# Helper utils
# -----------------------
def is_valid_email(email: str) -> bool:
    if not email or not isinstance(email, str): return False
    return re.match(r"^[a-zA-Z0-9._%+\-']+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$", email) is not None

def load_accounts_from_file(path: str):
    if not os.path.exists(path): return None
    with open(path, "r", encoding="utf-8") as f: return json.load(f)

def sanitize_recipients(raw_list):
    cleaned, seen = [], set()
    for r in raw_list:
        r = (r or "").strip().lower()
        if r and is_valid_email(r) and r not in seen:
            seen.add(r)
            cleaned.append(r)
    return cleaned

def ensure_sent_counters(accounts):
    if not os.path.exists(SENT_COUNTERS_JSON):
        counters = {acc["email"]: {"date": str(date.today()), "sent_today": 0} for acc in accounts}
    else:
        with open(SENT_COUNTERS_JSON, "r", encoding="utf-8") as f: counters = json.load(f)
    changed = False
    for acc in accounts:
        if acc["email"] not in counters:
            counters[acc["email"]] = {"date": str(date.today()), "sent_today": 0}
            changed = True
    if changed or not os.path.exists(SENT_COUNTERS_JSON):
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
# UI: Sidebar & Account Loading
# -----------------------
st.sidebar.header("Accounts (accounts.json)")
st.sidebar.info("Update `accounts.json` to include a `provider` for each account (e.g., 'gmail', 'yahoo').")
accounts = None
if os.path.exists("accounts.json"):
    try: accounts = load_accounts_from_file("accounts.json")
    except Exception as e: st.sidebar.error(f"Failed to load accounts.json: {e}")

uploaded_accounts = st.sidebar.file_uploader("Or upload a temporary accounts.json", type=["json"])
if uploaded_accounts:
    try: accounts = json.load(uploaded_accounts)
    except Exception as e: st.sidebar.error(f"Invalid JSON: {e}"); accounts = None

if not accounts:
    st.warning("No accounts loaded. Create or upload an `accounts.json` file.")
    st.stop()

valid_accounts = []
for acc in accounts:
    if all(k in acc for k in ["email", "password", "name", "provider"]) and acc["provider"].lower() in SMTP_SETTINGS:
        valid_accounts.append(acc)
    else:
        st.sidebar.warning(f"Skipping invalid account entry: {acc.get('email', 'N/A')}")

if not valid_accounts:
    st.error("No valid accounts found with a supported 'provider'.")
    st.stop()

ensure_sent_counters(valid_accounts)

st.sidebar.header("Sending Controls")
daily_limit_per_account = st.sidebar.number_input("Daily limit per account", min_value=1, value=DEFAULT_DAILY_LIMIT)
sleep_seconds = st.sidebar.number_input("Seconds between emails", min_value=0.0, value=1.0, step=0.1)

# -----------------------
# UI: Main Page
# -----------------------
st.header("1. Select Senders & Compose Email")
sender_name_override = st.text_input("Sender Name (Optional, overrides name from account file)")

account_options = [f"{acc['email']} ({acc['provider']}) - Sent: {get_sent_today(acc['email'])}/{daily_limit_per_account}" for acc in valid_accounts]
selected_account_labels = st.multiselect("Select sender accounts to use:", options=account_options, default=account_options)
selected_accounts = [acc for acc in valid_accounts if f"{acc['email']} ({acc['provider']}) - Sent: {get_sent_today(acc['email'])}/{daily_limit_per_account}" in selected_account_labels]

if not selected_accounts: st.warning("Please select at least one sender account.")

subject = st.text_input("Subject")
if 'email_body' not in st.session_state: st.session_state.email_body = ""
content = st_quill(value=st.session_state.email_body, key="quill_editor", placeholder="Write your email here...", html=True)
if content != st.session_state.email_body:
    st.session_state.email_body = content
    st.rerun()
body_html = st.session_state.email_body
uploaded_attach = st.file_uploader("Optional: Attach File", accept_multiple_files=False)

st.header("2. Add Recipients")
recipients, recipient_name_map = [], {}
uploaded_recipients = st.file_uploader("Upload CSV/Excel/TXT", type=["csv", "xlsx", "txt"])
if uploaded_recipients:
    try:
        if uploaded_recipients.name.endswith((".csv", ".txt")): df = pd.read_csv(uploaded_recipients, header=None, dtype=str, keep_default_na=False)
        else: df = pd.read_excel(uploaded_recipients, header=None, dtype=str)
        emails = [str(e).strip() for e in df.iloc[:, 0].tolist()]
        recipients.extend(emails)
        if df.shape[1] >= 2:
            names = [str(n).strip() for n in df.iloc[:, 1].tolist()]
            for email, name in zip(emails, names):
                if is_valid_email(email) and name: recipient_name_map[email.lower()] = name
    except Exception as e: st.error(f"Failed to parse uploaded file: {e}")

pasted = st.text_area("Or paste emails (one per line):", height=150)
if pasted: recipients.extend([line.strip() for line in pasted.splitlines() if line.strip()])
recipients = sanitize_recipients(recipients)
st.success(f"Loaded {len(recipients)} unique valid recipients")

st.header("3. Tracking & Sending")
enable_open_tracking = st.checkbox("Enable Email Open Tracking (Optional)", value=False)
tracker_url = st.text_input("Tracker URL (from webhook.site)", "", help="Go to webhook.site, copy the URL, and paste it here. No signup needed.")

# -----------------------
# Logic: Build & Send
# -----------------------
def build_message(sender_name, sender_email, to_email, subject, html_body, attach_file=None, uuid_id=None):
    msg = MIMEMultipart('related')
    msg['From'] = formataddr((sender_name, sender_email))
    msg['To'] = to_email
    msg['Subject'] = subject
    
    final_html = html_body
    # --- MODIFIED: Only add pixel if tracking is enabled AND URL is provided ---
    if enable_open_tracking and tracker_url.strip():
        pixel_url = f"{tracker_url.strip()}?id={uuid_id}&r={to_email}"
        final_html += f'<img src="{pixel_url}" width="1" height="1" style="display:none; border:0;" alt=""/>'
    
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
    provider = account['provider'].lower()
    settings = SMTP_SETTINGS.get(provider)
    if not settings: return False, f"SMTP settings for '{provider}' not found."
    
    try:
        with smtplib.SMTP(settings['host'], settings['port'], timeout=60) as server:
            server.starttls()
            server.login(account["email"], account["password"])
            server.sendmail(account["email"], [to_email], msg.as_string())
        return True, None
    except Exception as e:
        return False, str(e)

if st.button("🚀 Send Emails"):
    # --- MODIFIED: Removed mandatory check for tracker URL ---
    if not subject or not body_html or not recipients: st.error("Subject, body, and recipients are required.")
    elif not selected_accounts: st.error("Please select at least one sender account.")
    else:
        total_sent, status_rows = 0, []
        progress = st.progress(0)
        status_placeholder = st.empty()
        
        account_idx = 0
        for i, recipient in enumerate(recipients):
            account = None
            for _ in range(len(selected_accounts)):
                candidate = selected_accounts[account_idx % len(selected_accounts)]
                if get_sent_today(candidate["email"]) < daily_limit_per_account:
                    account = candidate; account_idx += 1; break
                account_idx += 1
            
            if account is None: st.error("All selected accounts have hit their daily limit."); break
            
            sender_name = sender_name_override.strip() or account['name']
            to_name = recipient_name_map.get(recipient, "")
            personalized_body = body_html.replace("[Recipient Name]", to_name)
            uuid_id = str(uuid.uuid4())
            map_uuid_save(uuid_id, recipient, account["email"])
            
            msg = build_message(sender_name, account["email"], recipient, subject, personalized_body, uploaded_attach, uuid_id)
            ok, err = send_via_smtp(account, msg, recipient)
            
            row = { "timestamp": datetime.utcnow().isoformat(), "recipient": recipient, "account": account["email"], "uuid": uuid_id, "status": "sent" if ok else "failed", "error": str(err) if err else "" }
            append_sent_log(row)
            status_rows.append(row)

            if ok: update_sent_counter(account["email"]); total_sent += 1

            progress.progress((i + 1) / len(recipients))
            status_placeholder.text(f"Sending {i+1}/{len(recipients)} to {recipient} via {account['email']}... Status: {'OK' if ok else 'FAIL'}")
            time.sleep(float(sleep_seconds))

        status_placeholder.empty()
        st.success(f"Send loop finished. Successfully sent {total_sent} emails.")
        st.dataframe(pd.DataFrame(status_rows))
        
        with open(SENT_LOG_CSV, "rb") as f: st.download_button("Download send log (CSV)", data=f, file_name=SENT_LOG_CSV)
        if os.path.exists(MAP_UUID_CSV):
            with open(MAP_UUID_CSV, "rb") as f: st.download_button("Download UUID map (CSV)", data=f, file_name=MAP_UUID_CSV)
