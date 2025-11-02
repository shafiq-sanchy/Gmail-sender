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
from urllib.parse import quote

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
PROGRESS_STATE_JSON = "progress_state.json"
DEFAULT_DAILY_LIMIT = 450
MAX_RETRIES = 2  # Reduced retries

# Session state for control buttons
if 'is_paused' not in st.session_state:
    st.session_state.is_paused = False
if 'should_stop' not in st.session_state:
    st.session_state.should_stop = False
if 'is_sending' not in st.session_state:
    st.session_state.is_sending = False

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

def reset_all_counters():
    """Reset all sent counters to 0."""
    if os.path.exists(SENT_COUNTERS_JSON):
        os.remove(SENT_COUNTERS_JSON)
    st.success("✅ All counters reset!")

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

def save_progress_state(current_index, total, sent, failed):
    """Save current progress state."""
    state = {
        "current_index": current_index,
        "total": total,
        "sent": sent,
        "failed": failed,
        "timestamp": datetime.utcnow().isoformat()
    }
    with open(PROGRESS_STATE_JSON, "w") as f:
        json.dump(state, f, indent=2)

def load_progress_state():
    """Load saved progress state."""
    if os.path.exists(PROGRESS_STATE_JSON):
        with open(PROGRESS_STATE_JSON, "r") as f:
            return json.load(f)
    return None

# -----------------------
# Account Management
# -----------------------
class AccountManager:
    def __init__(self, accounts, daily_limit):
        self.accounts = accounts
        self.daily_limit = daily_limit
        self.rate_limited_accounts = set()  # Skip these accounts
        
    def mark_rate_limited(self, email):
        """Mark account as rate limited and skip it."""
        self.rate_limited_accounts.add(email)
        st.warning(f"🚫 {email} marked as rate limited. Skipping this account.")
    
    def get_next_available_account(self):
        """Get next available account (not rate limited, under limit)."""
        available = []
        for acc in self.accounts:
            email = acc["email"]
            
            # Skip if rate limited or over daily limit
            if email in self.rate_limited_accounts:
                continue
            
            sent = get_sent_today(email)
            if sent >= self.daily_limit:
                continue
            
            available.append((acc, sent))
        
        if not available:
            return None, "All accounts exhausted or rate limited"
        
        # Sort by least used
        available.sort(key=lambda x: x[1])
        return available[0][0], None
    
    def get_status(self):
        """Get status of all accounts."""
        status = []
        for acc in self.accounts:
            email = acc["email"]
            sent = get_sent_today(email)
            is_rate_limited = email in self.rate_limited_accounts
            
            status.append({
                "email": email,
                "sent": sent,
                "limit": self.daily_limit,
                "remaining": max(0, self.daily_limit - sent),
                "status": "🔴 Rate Limited" if is_rate_limited else "🟢 Active"
            })
        return status

def is_rate_limit_error(error_msg):
    """Check if error indicates rate limiting."""
    rate_indicators = [
        "rate limit", "too many", "quota", "429",
        "temporarily blocked", "slow down", "limit exceeded",
        "daily limit", "hourly limit"
    ]
    error_lower = str(error_msg).lower()
    return any(ind in error_lower for ind in rate_indicators)

# -----------------------
# Unsubscribe Link Generator
# -----------------------
def generate_unsubscribe_link(sender_email, recipient_email, recipient_name=""):
    """Generate mailto unsubscribe link."""
    subject = "Unsubscribe Request"
    
    if recipient_name:
        body = f"Email: {recipient_email}\nName: {recipient_name}\n\nI would like to unsubscribe from your mailing list."
    else:
        body = f"Email: {recipient_email}\n\nI would like to unsubscribe from your mailing list."
    
    mailto_link = f"mailto:{sender_email}?subject={quote(subject)}&body={quote(body)}"
    return mailto_link

def add_unsubscribe_footer(html_body, sender_email, recipient_email, recipient_name=""):
    """Add unsubscribe link to email footer."""
    unsubscribe_link = generate_unsubscribe_link(sender_email, recipient_email, recipient_name)
    
    footer = f"""
    <div style="margin-top: 30px; padding-top: 20px; border-top: 1px solid #ddd; text-align: center; font-size: 12px; color: #666;">
        <p>Don't want to receive these emails? <a href="{unsubscribe_link}" style="color: #0066cc; text-decoration: none;">Unsubscribe</a></p>
    </div>
    """
    
    return html_body + footer

# -----------------------
# UI: Sidebar
# -----------------------
st.sidebar.header("⚙️ Accounts & Settings")

with st.sidebar.expander("ℹ️ How to add custom SMTP"):
    st.markdown("Create `smtp_config.json` with your provider details.")
    st.code('''{"provider": {"host": "smtp.example.com", "port": 587}}''', language="json")

with st.sidebar.expander("📧 Unsubscribe Feature"):
    st.markdown("""
    **How it works:**
    - Automatic footer added to all emails
    - Click opens email client with pre-filled unsubscribe request
    - Recipient's info auto-filled in email body
    - Unsubscribe request sent to sender's email
    """)

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
        st.sidebar.warning(f"Skipping: {acc.get('email', 'N/A')}")

if not valid_accounts:
    st.error("No valid accounts found.")
    st.stop()

ensure_sent_counters(valid_accounts)

st.sidebar.header("🎛️ Sending Controls")
daily_limit_per_account = st.sidebar.number_input("Daily limit per account", min_value=1, value=DEFAULT_DAILY_LIMIT)
sleep_seconds = st.sidebar.number_input("Delay between emails (seconds)", min_value=0.0, value=2.0, step=0.1)
batch_size = st.sidebar.number_input("Batch size", min_value=10, value=50, step=10)
batch_delay = st.sidebar.number_input("Batch delay (seconds)", min_value=0, value=10, step=1)

# Personalization options
st.sidebar.header("✨ Personalization")
enable_name_personalization = st.sidebar.checkbox("Enable [Recipient Name] replacement", value=True)
custom_greeting = st.sidebar.text_input("Custom greeting (optional)", placeholder="Dear [Recipient Name],")

# Reset button
if st.sidebar.button("🔄 Reset All Counters"):
    reset_all_counters()
    st.rerun()

# -----------------------
# UI: Main Page
# -----------------------
st.header("1. 📤 Senders & Email Content")

account_map = {}
for acc in valid_accounts:
    sent_today = get_sent_today(acc['email'])
    remaining = daily_limit_per_account - sent_today
    label = f"{acc['email']} ({acc['provider']}) — {sent_today}/{daily_limit_per_account} (Remaining: {remaining})"
    account_map[label] = acc

selected_labels = st.multiselect(
    "Select sender accounts:",
    options=list(account_map.keys()),
    default=list(account_map.keys())
)
selected_accounts = [account_map[label] for label in selected_labels]

if not selected_accounts:
    st.warning("⚠️ Please select at least one sender account.")

total_capacity = sum(max(0, daily_limit_per_account - get_sent_today(acc['email'])) for acc in selected_accounts)
st.info(f"📊 Total remaining capacity: **{total_capacity}** emails")

sender_name_override = st.text_input("Sender Name (optional override)")
subject = st.text_input("Subject")

if 'email_body' not in st.session_state:
    st.session_state.email_body = ""

content = st_quill(value=st.session_state.email_body, key="quill_editor", 
                   placeholder="Write your email here... Use [Recipient Name] for personalization", html=True)
if content != st.session_state.email_body:
    st.session_state.email_body = content
    st.rerun()

body_html = st.session_state.email_body
uploaded_attach = st.file_uploader("Optional: Attach File", accept_multiple_files=False)

# Preview personalization
if enable_name_personalization and "[Recipient Name]" in body_html:
    st.info("✨ Personalization enabled: [Recipient Name] will be replaced with actual names")

st.header("2. 📧 Recipients")
recipients, recipient_name_map = [], {}
uploaded_recipients = st.file_uploader("Upload CSV/Excel/TXT (Column 1: Email, Column 2: Name)", type=["csv", "xlsx", "txt"])

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
        st.error(f"Failed to parse file: {e}")

pasted = st.text_area("Or paste emails (one per line):", height=150)
if pasted:
    recipients.extend([line.strip() for line in pasted.splitlines() if line.strip()])

recipients = sanitize_recipients(recipients)
st.success(f"✅ Loaded {len(recipients)} unique valid recipients")

if recipient_name_map:
    st.info(f"📝 {len(recipient_name_map)} recipients have names for personalization")

if len(recipients) > total_capacity:
    st.warning(f"⚠️ {len(recipients)} recipients but only {total_capacity} capacity!")

st.header("3. 🔍 Tracking")
enable_open_tracking = st.checkbox("Enable open tracking", value=False)
tracker_url = st.text_input("Tracker URL (webhook.site)", "")
enable_unsubscribe = st.checkbox("Add unsubscribe link in footer", value=True)

# -----------------------
# Build & Send Functions
# -----------------------
def build_message(sender_name, sender_email, to_email, subject, html_body, 
                  to_name="", attach_file=None, uuid_id=None):
    msg = MIMEMultipart('related')
    msg['From'] = formataddr((sender_name, sender_email))
    msg['To'] = to_email
    msg['Subject'] = subject
    
    # Personalize body
    personalized_body = html_body
    if enable_name_personalization and to_name:
        personalized_body = personalized_body.replace("[Recipient Name]", to_name)
    elif enable_name_personalization:
        personalized_body = personalized_body.replace("[Recipient Name]", "")
    
    # Add custom greeting
    if custom_greeting and to_name:
        greeting = custom_greeting.replace("[Recipient Name]", to_name)
        personalized_body = greeting + "<br><br>" + personalized_body
    
    # Add tracking pixel
    if enable_open_tracking and tracker_url.strip():
        pixel_url = f"{tracker_url.strip()}?id={uuid_id}&r={to_email}"
        personalized_body += f'<img src="{pixel_url}" width="1" height="1" style="display:none;" alt=""/>'
    
    # Add unsubscribe footer
    if enable_unsubscribe:
        personalized_body = add_unsubscribe_footer(personalized_body, sender_email, to_email, to_name)
    
    msg.attach(MIMEText(personalized_body, 'html', 'utf-8'))
    
    if attach_file:
        part = MIMEBase('application', 'octet-stream')
        attach_file.seek(0)
        part.set_payload(attach_file.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename="{attach_file.name}"')
        msg.attach(part)
    
    return msg

def send_via_smtp(account, msg, to_email):
    """Send email via SMTP."""
    provider = account['provider'].lower()
    settings = ALL_SMTP_SETTINGS.get(provider)
    if not settings:
        return False, f"SMTP settings for '{provider}' not found."
    
    try:
        with smtplib.SMTP(settings['host'], settings['port'], timeout=60) as server:
            server.starttls()
            server.login(account["email"], account["password"])
            server.sendmail(account["email"], [to_email], msg.as_string())
        return True, None
    except Exception as e:
        return False, str(e)

# -----------------------
# Control Buttons
# -----------------------
st.header("4. 🎮 Send Controls")

col1, col2, col3, col4 = st.columns(4)

with col1:
    send_button = st.button("📤 Start Sending", disabled=st.session_state.is_sending, use_container_width=True)

with col2:
    if st.button("⏸️ Pause", disabled=not st.session_state.is_sending, use_container_width=True):
        st.session_state.is_paused = True

with col3:
    if st.button("▶️ Resume", disabled=not st.session_state.is_paused, use_container_width=True):
        st.session_state.is_paused = False

with col4:
    if st.button("⏹️ Stop", disabled=not st.session_state.is_sending, use_container_width=True):
        st.session_state.should_stop = True

# -----------------------
# MAIN SEND LOOP
# -----------------------
if send_button:
    if not all([subject, body_html, recipients]):
        st.error("❌ Subject, body, and recipients required.")
    elif not selected_accounts:
        st.error("❌ Select at least one sender account.")
    else:
        st.session_state.is_sending = True
        st.session_state.should_stop = False
        st.session_state.is_paused = False
        
        account_mgr = AccountManager(selected_accounts, daily_limit_per_account)
        
        total_sent = 0
        total_failed = 0
        total_skipped = 0
        status_rows = []
        
        progress_bar = st.progress(0)
        status_placeholder = st.empty()
        control_status = st.empty()
        
        try:
            num_batches = (len(recipients) + batch_size - 1) // batch_size
            
            for batch_num in range(num_batches):
                if st.session_state.should_stop:
                    st.warning("🛑 Sending stopped by user")
                    break
                
                batch_start = batch_num * batch_size
                batch_end = min(batch_start + batch_size, len(recipients))
                batch_recipients = recipients[batch_start:batch_end]
                
                st.info(f"🔄 Batch {batch_num + 1}/{num_batches} ({len(batch_recipients)} emails)")
                
                for i, recipient in enumerate(batch_recipients):
                    # Check for pause
                    while st.session_state.is_paused:
                        control_status.warning("⏸️ PAUSED - Click Resume to continue")
                        time.sleep(1)
                    
                    # Check for stop
                    if st.session_state.should_stop:
                        break
                    
                    control_status.empty()
                    overall_index = batch_start + i
                    
                    # Get available account
                    account, error_msg = account_mgr.get_next_available_account()
                    
                    if account is None:
                        st.error(f"🚫 {error_msg}")
                        total_skipped = len(recipients) - overall_index
                        break
                    
                    sender_name = sender_name_override.strip() or account['name']
                    to_name = recipient_name_map.get(recipient.lower(), "")
                    uuid_id = str(uuid.uuid4())
                    
                    map_uuid_save(uuid_id, recipient, account["email"])
                    
                    msg = build_message(sender_name, account["email"], recipient, subject,
                                      body_html, to_name, uploaded_attach, uuid_id)
                    
                    # Try sending with limited retries
                    retry_count = 0
                    ok, err = False, None
                    
                    while retry_count <= MAX_RETRIES:
                        ok, err = send_via_smtp(account, msg, recipient)
                        
                        if ok:
                            break
                        
                        # Check if rate limited
                        if is_rate_limit_error(err):
                            st.warning(f"⚠️ Rate limit detected on {account['email']}")
                            # Mark account as rate limited and skip it
                            account_mgr.mark_rate_limited(account['email'])
                            # Get new account
                            account, error_msg = account_mgr.get_next_available_account()
                            if account is None:
                                st.error("🚫 All accounts rate limited!")
                                break
                            # Rebuild message with new account
                            sender_name = sender_name_override.strip() or account['name']
                            msg = build_message(sender_name, account["email"], recipient, 
                                              subject, body_html, to_name, uploaded_attach, uuid_id)
                            retry_count += 1
                        else:
                            break
                    
                    # Log result
                    row = {
                        "timestamp": datetime.utcnow().isoformat(),
                        "recipient": recipient,
                        "name": to_name,
                        "account": account["email"],
                        "uuid": uuid_id,
                        "status": "sent" if ok else "failed",
                        "error": str(err)[:100] if err else ""
                    }
                    append_sent_log(row)
                    status_rows.append(row)
                    
                    if ok:
                        update_sent_counter(account["email"])
                        total_sent += 1
                    else:
                        total_failed += 1
                    
                    # Update progress
                    pct = int((overall_index + 1) * 100 / len(recipients))
                    progress_bar.progress(pct)
                    
                    # Show status
                    sent_from = get_sent_today(account['email'])
                    status_text = f"📧 {overall_index + 1}/{len(recipients)} → "
                    if to_name:
                        status_text += f"{to_name} ({recipient}) "
                    else:
                        status_text += f"{recipient} "
                    status_text += f"via {account['email']} ({sent_from}/{daily_limit_per_account}) | "
                    status_text += "✅ OK" if ok else f"❌ FAIL"
                    
                    status_placeholder.text(status_text)
                    
                    # Save progress
                    save_progress_state(overall_index, len(recipients), total_sent, total_failed)
                    
                    time.sleep(float(sleep_seconds))
                
                # Batch delay
                if batch_num < num_batches - 1 and not st.session_state.should_stop:
                    st.info(f"⏸️ Waiting {batch_delay}s before next batch...")
                    time.sleep(batch_delay)
        
        except Exception as ex:
            st.error(f"💥 Error: {ex}")
            import traceback
            st.code(traceback.format_exc())
        
        finally:
            st.session_state.is_sending = False
            st.session_state.is_paused = False
            status_placeholder.empty()
            control_status.empty()
            progress_bar.progress(100)
            
            # Final summary
            st.success("✅ **Task Complete!**")
            
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("✅ Sent", total_sent)
            with col2:
                st.metric("❌ Failed", total_failed)
            with col3:
                st.metric("⏭️ Skipped", total_skipped)
            with col4:
                st.metric("📊 Total", len(recipients))
            
            # Account status
            st.subheader("📈 Account Status")
            account_status = account_mgr.get_status()
            st.dataframe(pd.DataFrame(account_status))
            
            # Detailed results
            st.subheader("📋 Detailed Results")
            if status_rows:
                results_df = pd.DataFrame(status_rows)
                st.dataframe(results_df)
                
                # Download buttons
                col1, col2 = st.columns(2)
                with col1:
                    if os.path.exists(SENT_LOG_CSV):
                        with open(SENT_LOG_CSV, "rb") as f:
                            st.download_button("⬇️ Download Log", data=f, file_name=SENT_LOG_CSV)
                with col2:
                    if os.path.exists(MAP_UUID_CSV):
                        with open(MAP_UUID_CSV, "rb") as f:
                            st.download_button("⬇️ Download UUID Map", data=f, file_name=MAP_UUID_CSV)
