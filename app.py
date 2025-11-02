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
st.set_page_config(page_title="Advanced Bulk Email Sender", layout="wide")
st.title("📨 Advanced Bulk Email Sender")

# --- Default SMTP Settings ---
DEFAULT_SMTP_SETTINGS = {
    "gmail": {"host": "smtp.gmail.com", "port": 587},
    "yahoo": {"host": "smtp.mail.yahoo.com", "port": 587},
    "outlook": {"host": "smtp.office365.com", "port": 587},
    "aol": {"host": "smtp.aol.com", "port": 587},
    "protonmail": {"host": "127.0.0.1", "port": 1025}
}

# Filenames
SENT_LOG_CSV = "sent_log.csv"
SENT_COUNTERS_JSON = "sent_counters.json"
MAP_UUID_CSV = "uuid_map.csv"
SMTP_CONFIG_JSON = "smtp_config.json"
DEFAULT_DAILY_LIMIT = 450
MAX_RETRIES = 3
RATE_LIMIT_WAIT = 60  # seconds to wait on rate limit

# -----------------------
# Helper utils
# -----------------------
def load_smtp_settings():
    """Loads custom SMTP settings and merges them with defaults."""
    settings = DEFAULT_SMTP_SETTINGS.copy()
    if os.path.exists(SMTP_CONFIG_JSON):
        try:
            with open(SMTP_CONFIG_JSON, "r") as f:
                custom_settings = json.load(f)
                settings.update(custom_settings)
                st.sidebar.success(f"Loaded custom settings from {SMTP_CONFIG_JSON}")
        except json.JSONDecodeError:
            st.sidebar.error(f"Error reading {SMTP_CONFIG_JSON}. Please check its format.")
    return settings

# Load SMTP settings at the start
ALL_SMTP_SETTINGS = load_smtp_settings()

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
    counters = {}
    if os.path.exists(SENT_COUNTERS_JSON):
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
# NEW: Improved Account Selection Logic
# -----------------------
def get_next_available_account(accounts, daily_limit):
    """
    Returns the next available account that hasn't hit its daily limit.
    Uses round-robin selection among accounts sorted by least usage.
    Returns None if all accounts are exhausted.
    """
    available = []
    for acc in accounts:
        sent = get_sent_today(acc["email"])
        if sent < daily_limit:
            available.append((acc, sent))
    
    if not available:
        return None
    
    # Sort by least used first
    available.sort(key=lambda x: x[1])
    return available[0][0]

def is_rate_limit_error(error_msg):
    """Check if error message indicates rate limiting."""
    rate_limit_indicators = [
        "rate limit", "too many", "quota", "429", 
        "temporarily blocked", "slow down", "limit exceeded"
    ]
    error_lower = str(error_msg).lower()
    return any(indicator in error_lower for indicator in rate_limit_indicators)

# -----------------------
# UI: Sidebar & Account Loading
# -----------------------
st.sidebar.header("Accounts & Settings")
with st.sidebar.expander("How to add custom SMTP servers"):
    st.markdown("1. Create a file named `smtp_config.json` in the same directory as `app.py`.")
    st.markdown("2. Add your SMTP provider's details in the format shown below.")
    st.code("""
{
  "provider_name": {
    "host": "smtp.yourprovider.com",
    "port": 587
  }
}
    """, language="json")
    st.markdown("3. In `accounts.json`, set the `provider` field to `provider_name`.")

accounts = None
uploaded_accounts = st.sidebar.file_uploader("Upload accounts.json", type=["json"])
if uploaded_accounts:
    try: accounts = json.load(uploaded_accounts)
    except Exception as e: st.sidebar.error(f"Invalid JSON: {e}"); accounts = None
elif os.path.exists("accounts.json"):
    try: accounts = load_accounts_from_file("accounts.json")
    except Exception as e: st.sidebar.error(f"Failed to load accounts.json: {e}")

if not accounts:
    st.warning("No accounts loaded. Please create or upload an `accounts.json` file.")
    st.stop()

valid_accounts = []
for acc in accounts:
    provider = acc.get("provider", "").lower()
    if all(k in acc for k in ["email", "password", "name"]) and provider in ALL_SMTP_SETTINGS:
        valid_accounts.append(acc)
    else:
        st.sidebar.warning(f"Skipping invalid account: {acc.get('email', 'N/A')}. Check `provider` field.")

if not valid_accounts:
    st.error("No valid accounts found. Ensure each account has a 'provider' that exists in default settings or `smtp_config.json`.")
    st.stop()

ensure_sent_counters(valid_accounts)

st.sidebar.header("Sending Controls")
daily_limit_per_account = st.sidebar.number_input("Daily send limit per account", min_value=1, value=DEFAULT_DAILY_LIMIT)
sleep_seconds = st.sidebar.number_input("Delay between emails (seconds)", min_value=0.0, value=2.0, step=0.1)
batch_size = st.sidebar.number_input("Batch size (emails per batch)", min_value=10, value=50, step=10)
batch_delay = st.sidebar.number_input("Delay between batches (seconds)", min_value=0, value=10, step=1)

# -----------------------
# UI: Main Page
# -----------------------
st.header("1. Senders & Email Content")

# Account selection with detailed info
account_map = {}
for acc in valid_accounts:
    sent_today = get_sent_today(acc['email'])
    remaining = daily_limit_per_account - sent_today
    label = f"{acc['email']} ({acc['provider']}) — Sent: {sent_today}/{daily_limit_per_account} (Remaining: {remaining})"
    account_map[label] = acc

selected_labels = st.multiselect(
    "Select sender accounts to use for this campaign:",
    options=list(account_map.keys()),
    default=list(account_map.keys())
)
selected_accounts = [account_map[label] for label in selected_labels]

if not selected_accounts: 
    st.warning("Please select at least one sender account.")

# Show total capacity
total_capacity = sum(max(0, daily_limit_per_account - get_sent_today(acc['email'])) for acc in selected_accounts)
st.info(f"📊 Total remaining capacity across selected accounts: **{total_capacity}** emails")

sender_name_override = st.text_input("Sender Name (Optional, overrides name from account file)")
subject = st.text_input("Subject")
if 'email_body' not in st.session_state: st.session_state.email_body = ""
content = st_quill(value=st.session_state.email_body, key="quill_editor", placeholder="Write your email here...", html=True)
if content != st.session_state.email_body:
    st.session_state.email_body = content
    st.rerun()
body_html = st.session_state.email_body
uploaded_attach = st.file_uploader("Optional: Attach File", accept_multiple_files=False)

st.header("2. Recipients")
recipients, recipient_name_map = [], {}
uploaded_recipients = st.file_uploader("Upload CSV/Excel/TXT", type=["csv", "xlsx", "txt"])
if uploaded_recipients:
    try:
        if uploaded_recipients.name.endswith((".csv", ".txt")): 
            df = pd.read_csv(uploaded_recipients, header=None, dtype=str, keep_default_na=False)
        else: 
            df = pd.read_excel(uploaded_recipients, header=None, dtype=str)
        emails = [str(e).strip() for e in df.iloc[:, 0].tolist()]
        recipients.extend(emails)
        if df.shape[1] >= 2:
            names = [str(n).strip() for n in df.iloc[:, 1].tolist()]
            for email, name in zip(emails, names):
                if is_valid_email(email) and name: 
                    recipient_name_map[email.lower()] = name
    except Exception as e: 
        st.error(f"Failed to parse uploaded file: {e}")

pasted = st.text_area("Or paste emails (one per line):", height=150)
if pasted: 
    recipients.extend([line.strip() for line in pasted.splitlines() if line.strip()])
recipients = sanitize_recipients(recipients)
st.success(f"✅ Loaded {len(recipients)} unique valid recipients")

if len(recipients) > total_capacity:
    st.warning(f"⚠️ You have {len(recipients)} recipients but only {total_capacity} sending capacity remaining today!")

st.header("3. Tracking & Sending")
enable_open_tracking = st.checkbox("Enable Email Open Tracking (Optional)", value=False)
tracker_url = st.text_input("Tracker URL (from webhook.site)", "", help="Optional. Go to webhook.site, copy URL, paste here.")

# -----------------------
# Logic: Build & Send
# -----------------------
def build_message(sender_name, sender_email, to_email, subject, html_body, attach_file=None, uuid_id=None):
    msg = MIMEMultipart('related')
    msg['From'] = formataddr((sender_name, sender_email))
    msg['To'] = to_email
    msg['Subject'] = subject
    
    final_html = html_body
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

def send_via_smtp(account, msg, to_email, retry_count=0):
    """Send email with retry logic for rate limiting."""
    provider = account['provider'].lower()
    settings = ALL_SMTP_SETTINGS.get(provider)
    if not settings: 
        return False, f"SMTP settings for '{provider}' not found.", False
    
    try:
        with smtplib.SMTP(settings['host'], settings['port'], timeout=60) as server:
            server.starttls()
            server.login(account["email"], account["password"])
            server.sendmail(account["email"], [to_email], msg.as_string())
        return True, None, False
    except Exception as e:
        error_msg = str(e)
        is_rate_limited = is_rate_limit_error(error_msg)
        
        # Retry on rate limit
        if is_rate_limited and retry_count < MAX_RETRIES:
            return False, error_msg, True  # True indicates should retry
        
        return False, error_msg, False

# -----------------------
# IMPROVED SEND LOOP
# -----------------------
if st.button("📤 Send Emails"):
    if not all([subject, body_html, recipients]):
        st.error("❌ Subject, body, and recipients are required.")
    elif not selected_accounts:
        st.error("❌ Please select at least one sender account.")
    else:
        total_sent = 0
        total_failed = 0
        status_rows = []
        progress_bar = st.progress(0)
        status_placeholder = st.empty()
        
        # Track account usage in this session
        session_usage = {acc['email']: 0 for acc in selected_accounts}
        
        try:
            # Process in batches
            num_batches = (len(recipients) + batch_size - 1) // batch_size
            
            for batch_num in range(num_batches):
                batch_start = batch_num * batch_size
                batch_end = min(batch_start + batch_size, len(recipients))
                batch_recipients = recipients[batch_start:batch_end]
                
                st.info(f"🔄 Processing batch {batch_num + 1}/{num_batches} ({len(batch_recipients)} emails)")
                
                for i, recipient in enumerate(batch_recipients):
                    overall_index = batch_start + i
                    
                    # Get next available account
                    account = get_next_available_account(selected_accounts, daily_limit_per_account)
                    
                    if account is None:
                        st.error("🚫 All selected accounts have reached their daily limit!")
                        st.warning(f"✅ Sent: {total_sent} | ❌ Failed: {total_failed} | ⏭️ Skipped: {len(recipients) - overall_index}")
                        break
                    
                    sender_name = sender_name_override.strip() or account['name']
                    to_name = recipient_name_map.get(recipient.lower(), "")
                    personalized_body = body_html.replace("[Recipient Name]", to_name)
                    uuid_id = str(uuid.uuid4())
                    map_uuid_save(uuid_id, recipient, account["email"])
                    
                    msg = build_message(sender_name, account["email"], recipient, subject, 
                                      personalized_body, uploaded_attach, uuid_id)
                    
                    # Send with retry logic
                    retry_count = 0
                    ok, err, should_retry = False, None, False
                    
                    while retry_count <= MAX_RETRIES:
                        ok, err, should_retry = send_via_smtp(account, msg, recipient, retry_count)
                        
                        if ok:
                            break
                        elif should_retry:
                            retry_count += 1
                            st.warning(f"⏳ Rate limit detected for {account['email']}. Waiting {RATE_LIMIT_WAIT}s before retry {retry_count}/{MAX_RETRIES}...")
                            time.sleep(RATE_LIMIT_WAIT)
                        else:
                            break
                    
                    # Log result
                    row = {
                        "timestamp": datetime.utcnow().isoformat(),
                        "recipient": recipient,
                        "account": account["email"],
                        "uuid": uuid_id,
                        "status": "sent" if ok else "failed",
                        "error": str(err) if err else "",
                        "retries": retry_count
                    }
                    append_sent_log(row)
                    status_rows.append(row)
                    
                    if ok:
                        update_sent_counter(account["email"])
                        session_usage[account["email"]] += 1
                        total_sent += 1
                    else:
                        total_failed += 1
                    
                    # Update progress
                    pct = int((overall_index + 1) * 100 / len(recipients))
                    progress_bar.progress(pct)
                    
                    # Show status
                    sent_from_account = get_sent_today(account['email'])
                    status_placeholder.text(
                        f"📧 {overall_index + 1}/{len(recipients)} → {recipient} via {account['email']} "
                        f"({sent_from_account}/{daily_limit_per_account}) | {'✅ OK' if ok else '❌ FAIL'}"
                    )
                    
                    # Delay between emails
                    time.sleep(float(sleep_seconds))
                
                # Delay between batches
                if batch_num < num_batches - 1:
                    st.info(f"⏸️ Batch complete. Waiting {batch_delay}s before next batch...")
                    time.sleep(batch_delay)
        
        except Exception as ex:
            st.error(f"💥 Unexpected error: {ex}")
        
        finally:
            status_placeholder.empty()
            progress_bar.progress(100)
            
            # Final summary
            st.success(f"✅ **Sending Complete!**")
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("✅ Sent", total_sent)
            with col2:
                st.metric("❌ Failed", total_failed)
            with col3:
                st.metric("📊 Total", len(recipients))
            
            # Show account usage
            st.subheader("📈 Account Usage This Session")
            usage_data = []
            for acc in selected_accounts:
                sent_before = get_sent_today(acc['email']) - session_usage[acc['email']]
                sent_now = session_usage[acc['email']]
                total_today = get_sent_today(acc['email'])
                usage_data.append({
                    "Account": acc['email'],
                    "Provider": acc['provider'],
                    "Sent (This Session)": sent_now,
                    "Total Today": total_today,
                    "Remaining": daily_limit_per_account - total_today
                })
            st.dataframe(pd.DataFrame(usage_data))
            
            # Show detailed results
            st.subheader("📋 Detailed Results")
            st.dataframe(pd.DataFrame(status_rows))
            
            # Download buttons
            if os.path.exists(SENT_LOG_CSV):
                with open(SENT_LOG_CSV, "rb") as f:
                    st.download_button("⬇️ Download Send Log (CSV)", data=f, file_name=SENT_LOG_CSV)
            if os.path.exists(MAP_UUID_CSV):
                with open(MAP_UUID_CSV, "rb") as f:
                    st.download_button("⬇️ Download UUID Map (CSV)", data=f, file_name=MAP_UUID_CSV)
