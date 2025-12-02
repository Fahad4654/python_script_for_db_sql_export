import os
from dotenv import load_dotenv
import pandas as pd
import zipfile
from datetime import datetime
import time
from msal import ConfidentialClientApplication
import requests
from sqlalchemy import create_engine

# -----------------------------
# Load environment variables
# -----------------------------
load_dotenv()

# -----------------------------
# Timer
# -----------------------------
start_time = time.time()

# -----------------------------
# Database config
# -----------------------------
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "port": os.getenv("DB_PORT", 5432),
}

# SQLAlchemy engine
DB_URL = f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
engine = create_engine(DB_URL)

# -----------------------------
# Queries
# -----------------------------
def load_query(file_path):
    with open(file_path, "r") as f:
        return f.read()

QUERY_1 = load_query("queries/query_beneficiary.sql")
QUERY_2 = load_query("queries/query_project_activity.sql")

# -----------------------------
# OneDrive / Email config
# -----------------------------
TENANT_ID = os.getenv("AZURE_TENANT_ID")
CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")
SENDER_EMAIL = os.getenv("SENDER_EMAIL")
RECIPIENT_EMAIL = os.getenv("RECIPIENT_EMAIL")
CC_EMAIL = os.getenv("CC_EMAIL")  # optional
FOLDER_NAME = "SACP-export"
SENDER_NAME = os.getenv("SENDER_NAME", "SACP Team")
RECIPIENT_NAME = os.getenv("RECIPIENT_NAME", "Recipient")

# -----------------------------
# Export queries to Excel in chunks
# -----------------------------
CHUNK = 50_000  # rows per chunk

def export_queries():
    result_folder = "result"
    os.makedirs(result_folder, exist_ok=True)

    today = datetime.now().strftime("%Y-%m-%d")
    file1 = os.path.join(result_folder, f"Beneficiary_{today}.xlsx")
    file2 = os.path.join(result_folder, f"Project_activity_{today}.xlsx")
    zip_filename = os.path.join(result_folder, f"SACP_exports_{today}.zip")

    def write_query_to_excel(query, filename):
        chunk_no = 1
        startrow = 0
        with pd.ExcelWriter(filename, engine="openpyxl") as writer:
            while True:
                try:
                    with engine.connect() as conn:
                        chunk_iter = pd.read_sql(query, conn, chunksize=CHUNK)
                        has_data = False
                        for chunk in chunk_iter:
                            has_data = True
                            print(f"Writing chunk {chunk_no} ({len(chunk)} rows)...")
                            chunk.to_excel(writer, index=False, sheet_name="Sheet1", startrow=startrow, header=(startrow==0))
                            startrow += len(chunk)
                            chunk_no += 1
                        if not has_data:
                            break
                    break  # finished all chunks
                except Exception as e:
                    print(f"Error during export chunk {chunk_no}, retrying in 5s:", e)
                    time.sleep(5)

    print("\nRunning Query 1 in chunks...")
    write_query_to_excel(QUERY_1, file1)

    print("\nRunning Query 2 in chunks...")
    write_query_to_excel(QUERY_2, file2)

    print("\nExcel files exported.")

    print("Creating ZIP...")
    with zipfile.ZipFile(zip_filename, "w", zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(file1, arcname=os.path.basename(file1))
        zipf.write(file2, arcname=os.path.basename(file2))

    print(f"ZIP created: {zip_filename}")
    return zip_filename

# -----------------------------
# Upload large file to OneDrive in chunks
# -----------------------------
def upload_large_file(file_path, folder_name, headers, max_retries=5):
    import time as t

    filename = os.path.basename(file_path)
    session_url = f"https://graph.microsoft.com/v1.0/users/{SENDER_EMAIL}/drive/root:/{folder_name}/{filename}:/createUploadSession"
    session_payload = {"item": {"@microsoft.graph.conflictBehavior": "replace"}}
    session_resp = requests.post(session_url, headers=headers, json=session_payload)
    if session_resp.status_code not in (200, 201):
        print("Error creating upload session:", session_resp.status_code, session_resp.text)
        exit(1)

    upload_url = session_resp.json()["uploadUrl"]
    file_size = os.path.getsize(file_path)
    chunk_size = 10 * 1024 * 1024  # 10 MB

    with open(file_path, "rb") as f:
        start = 0
        while start < file_size:
            end = min(start + chunk_size - 1, file_size - 1)
            chunk_data = f.read(chunk_size)
            success = False
            attempt = 0
            while not success and attempt < max_retries:
                headers_chunk = {"Content-Range": f"bytes {start}-{end}/{file_size}"}
                resp = requests.put(upload_url, headers=headers_chunk, data=chunk_data)
                if resp.status_code in (200, 201, 202):
                    success = True
                    start = end + 1
                    progress = (start / file_size) * 100
                    print(f"Uploaded {start}/{file_size} bytes ({progress:.2f}%)")
                else:
                    attempt += 1
                    wait_time = 2**attempt
                    print(f"Chunk upload failed (attempt {attempt}/{max_retries}). Retrying in {wait_time}s...")
                    t.sleep(wait_time)
            if not success:
                print("Failed to upload chunk after maximum retries.")
                exit(1)

    file_id = resp.json()["id"]
    print(f"Large file uploaded successfully with ID: {file_id}")
    return file_id

# -----------------------------
# Upload ZIP and send email
# -----------------------------
def upload_and_email(zip_file_path):
    authority = f"https://login.microsoftonline.com/{TENANT_ID}"
    app = ConfidentialClientApplication(
        CLIENT_ID, authority=authority, client_credential=CLIENT_SECRET
    )
    token_response = app.acquire_token_for_client(
        scopes=["https://graph.microsoft.com/.default"]
    )
    access_token = token_response.get("access_token")
    if not access_token:
        print("Failed to obtain access token")
        exit(1)

    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}

    # Ensure folder exists
    folder_check_url = f"https://graph.microsoft.com/v1.0/users/{SENDER_EMAIL}/drive/root:/{FOLDER_NAME}"
    folder_resp = requests.get(folder_check_url, headers=headers)
    if folder_resp.status_code == 404:
        create_folder_url = f"https://graph.microsoft.com/v1.0/users/{SENDER_EMAIL}/drive/root/children"
        payload = {"name": FOLDER_NAME, "folder": {}, "@microsoft.graph.conflictBehavior": "fail"}
        resp = requests.post(create_folder_url, headers=headers, json=payload)
        if resp.status_code not in (201, 409):
            print("Error creating folder:", resp.status_code, resp.text)
            exit(1)
        print(f"Folder '{FOLDER_NAME}' created.")
    else:
        print(f"Folder '{FOLDER_NAME}' already exists.")

    # Upload file
    file_id = upload_large_file(zip_file_path, FOLDER_NAME, headers)

    # Create public link
    link_url = f"https://graph.microsoft.com/v1.0/users/{SENDER_EMAIL}/drive/items/{file_id}/createLink"
    link_payload = {"type": "view", "scope": "anonymous"}
    link_resp = requests.post(link_url, headers=headers, json=link_payload)
    if link_resp.status_code not in (200, 201):
        print("Error creating shareable link:", link_resp.status_code, link_resp.text)
        exit(1)
    download_link = link_resp.json()["link"]["webUrl"]
    print("Public download link:", download_link)

    # Send email
    filename = os.path.basename(zip_file_path)
    email_msg = {
        "message": {
            "subject": "SACP Export Files - Download Link",
            "body": {
                "contentType": "HTML",
                "content": f"""
            <p>Dear {RECIPIENT_NAME},</p>
            <p>Please find the latest SACP export files available for download. 
            You can access the ZIP file using the link below:</p>
            <p><a href='{download_link}'>{filename}</a></p>
            <p>Kindly ensure to download the file at your earliest convenience.</p>
            <p>Best regards,<br/>
            {SENDER_NAME}</p>
            """,
            },
            "toRecipients": [{"emailAddress": {"address": RECIPIENT_EMAIL}}],
            "ccRecipients": ([{"emailAddress": {"address": CC_EMAIL}}] if CC_EMAIL else []),
        },
        "saveToSentItems": "true",
    }
    graph_url = f"https://graph.microsoft.com/v1.0/users/{SENDER_EMAIL}/sendMail"
    response = requests.post(graph_url, headers=headers, json=email_msg)
    if response.status_code == 202:
        print("Email sent successfully with public download link!")
    else:
        print("Error sending email:", response.status_code, response.text)

# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":
    zip_file = export_queries()
    upload_and_email(zip_file)

    end_time = time.time()
    total_time = end_time - start_time
    minutes = int(total_time // 60)
    seconds = total_time % 60
    print(f"\n⏱ TOTAL EXECUTION TIME: {minutes} min {seconds:.2f} sec")
    print("All done!")
