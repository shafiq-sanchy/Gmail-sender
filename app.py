# app.py

import streamlit as st
import pandas as pd
from redis import from_url
from rq import Queue
from rq_scheduler import Scheduler
from tasks import send_email_task
from datetime import datetime, time, timedelta
import os

# --- App Configuration ---
st.set_page_config(
    page_title="Gmail Bulk Sender",
    page_icon="📧",
    layout="centered",
)

st.title("📧 Gmail Bulk Sender")
st.write("Queue and schedule emails to be sent in the background.")

# --- Redis Connection ---
# We use an environment variable for the Redis URL, essential for cloud deployment.
REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379')
try:
    redis_conn = from_url(REDIS_URL)
    q = Queue(connection=redis_conn)
    scheduler = Scheduler(queue=q, connection=redis_conn)
    st.sidebar.success("Connected to Redis successfully!")
except Exception as e:
    st.sidebar.error(f"Failed to connect to Redis: {e}")
    st.stop()


# --- User Inputs ---
with st.form("email_form"):
    sender_email = st.text_input("Your Gmail Address")
    # For security, it's better to use environment variables or Streamlit secrets for the password
    # For this example, we'll keep it as a text input.
    password = st.text_input("Your App Password", type="password", help="Use a Google App Password, not your regular password.")
    subject = st.text_input("Email Subject")
    message = st.text_area("Email Body")
    
    st.header("Recipients")
    # Option to upload a CSV file or enter a single email
    uploaded_file = st.file_uploader("Upload a CSV file with an 'email' column", type=['csv'])
    single_recipient = st.text_input("Or, enter a single recipient email")

    st.header("Scheduling")
    schedule_option = st.radio("When to send?", ('Send Now', 'Schedule for Later'))
    
    schedule_date = None
    schedule_time = None
    if schedule_option == 'Schedule for Later':
        schedule_date = st.date_input("Date")
        schedule_time = st.time_input("Time")

    submit_button = st.form_submit_button("Submit Job")


# --- Form Submission Logic ---
if submit_button:
    # Basic validation
    if not all([sender_email, password, subject, message]):
        st.error("Please fill in all sender details, subject, and message.")
    elif not uploaded_file and not single_recipient:
        st.error("Please either upload a CSV file or enter a single recipient.")
    else:
        recipients = []
        if uploaded_file is not None:
            try:
                df = pd.read_csv(uploaded_file)
                if 'email' in df.columns:
                    recipients = df['email'].dropna().unique().tolist()
                else:
                    st.error("The uploaded CSV must have a column named 'email'.")
            except Exception as e:
                st.error(f"Error reading CSV file: {e}")
        
        if single_recipient:
            recipients.append(single_recipient)

        if not recipients:
            st.warning("No valid recipient emails found.")
        else:
            job_count = 0
            # --- Enqueue or Schedule the jobs ---
            if schedule_option == 'Send Now':
                for email in recipients:
                    q.enqueue(send_email_task, sender_email, password, email, subject, message)
                    job_count += 1
                st.success(f"Success! {job_count} email jobs have been added to the queue and will be sent shortly.")

            elif schedule_option == 'Schedule for Later' and schedule_date and schedule_time:
                # Combine date and time into a single datetime object
                scheduled_datetime = datetime.combine(schedule_date, schedule_time)

                # Ensure the scheduled time is in the future
                if scheduled_datetime < datetime.now():
                    st.error("The scheduled time must be in the future.")
                else:
                    for email in recipients:
                        scheduler.enqueue_at(
                            scheduled_datetime,
                            send_email_task,
                            sender_email,
                            password,
                            email,
                            subject,
                            message
                        )
                        job_count += 1
                    st.success(f"Success! {job_count} email jobs have been scheduled for {scheduled_datetime.strftime('%Y-%m-%d %H:%M:%S')}.")

# --- Display Queue Information (Optional) ---
st.sidebar.header("Queue Status")
st.sidebar.write(f"Jobs in queue: {len(q)}")
st.sidebar.write("Scheduled Jobs:")
# Display scheduled jobs
try:
    scheduled_jobs = sorted(scheduler.get_jobs(), key=lambda job: job.enqueued_at)
    if scheduled_jobs:
        for job in scheduled_jobs:
            scheduled_time = job.enqueued_at.strftime('%Y-%m-%d %H:%M:%S')
            st.sidebar.text(f"- Job {job.id[:6]} for {job.args[2]} at {scheduled_time}")
    else:
        st.sidebar.text("No jobs scheduled.")
except Exception as e:
    st.sidebar.text(f"Could not retrieve scheduled jobs: {e}")
