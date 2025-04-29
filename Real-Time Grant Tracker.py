import os
import time
import threading
import io
import csv
from dateutil.parser import parse
from datetime import timedelta, datetime

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import mysql.connector
from mysql.connector import Error

from flask import Flask, render_template_string, request, redirect, url_for, flash, session, jsonify
import requests

import firebase_admin
from firebase_admin import auth, credentials
from functools import wraps

# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------
# Define sources with URL, export button XPath, and CSV header mapping.
SOURCES = {
    "simpler": {
        "url": "https://simpler.grants.gov/search",
        "export_xpath": "//button[contains(., 'Export results')]",
        # Mapping: internal key -> CSV header (or (header, conversion function))
        "mapping": {
            "opportunity_number": "opportunity_number",
            "opportunity_title": "opportunity_title",
            "posted_date": ("post_date", lambda s: parse(s).strftime("%Y-%m-%d")),
            "close_date": ("close_date", lambda s: parse(s).strftime("%Y-%m-%d")),
            "min_grant_amount": "award_floor",
            "max_grant_amount": "award_ceiling",
            "additional_info_url": "additional_info_url",
            "funding_categories": "funding categories",
            "description": "summary_description"
        }
    },
    "nih": {
        "url": "https://grants.nih.gov/funding/nih-guide-for-grants-and-contracts",
        "export_xpath": "//span[normalize-space(text())='Export Results']",
        "mapping": {
            "opportunity_number": "Document_Number",
            "opportunity_title": "Title",
            "posted_date": ("Release_Date", lambda s: parse(s).strftime("%Y-%m-%d")),
            "close_date": ("Expired_Date", lambda s: parse(s).strftime("%Y-%m-%d")),
            "additional_info_url": "URL"
        }
    }
}

# Folder for CSV downloads
DOWNLOAD_FOLDER = os.path.join(os.getcwd(), "downloads")
if not os.path.exists(DOWNLOAD_FOLDER):
    os.makedirs(DOWNLOAD_FOLDER)

# MySQL configuration (ensure your database/table exists)
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'open_grants'
}

# --------------------------------------------------
# Initialize Firebase Admin with your service account
# --------------------------------------------------
cred = credentials.Certificate(r"API key")
firebase_admin.initialize_app(cred)

# --------------------------------------------------
# FLASK APP SETUP
# --------------------------------------------------
app = Flask(__name__)
app.secret_key = "API key"
app.permanent_session_lifetime = timedelta(minutes=10)

# --------------------------------------------------
# UTILITY FUNCTION: Format Date
# --------------------------------------------------
def format_date(date_str):
    try:
        dt = parse(date_str)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return date_str

# --------------------------------------------------
# DOWNLOAD CSV FUNCTION (with timestamped renaming)
# --------------------------------------------------
def download_csv(url, export_xpath):
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    prefs = {
        "download.default_directory": DOWNLOAD_FOLDER,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }
    options.add_experimental_option("prefs", prefs)
    
    driver = webdriver.Chrome(options=options)
    driver.get(url)
    time.sleep(7)  # Wait for page load
    
    try:
        export_button = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.XPATH, export_xpath))
        )
        driver.execute_script("arguments[0].scrollIntoView(true);", export_button)
        driver.execute_script("arguments[0].click();", export_button)
        print("Clicked export button via JavaScript.")
    except Exception as e:
        print("Error clicking export button:", e)
        driver.quit()
        return None

    # Wait for CSV file to appear
    timeout = 60  # seconds
    start_time = time.time()
    csv_path = None
    while time.time() - start_time < timeout:
        for filename in os.listdir(DOWNLOAD_FOLDER):
            print("Found file:", filename)
            if filename.lower().endswith(".csv") and not filename.lower().endswith(".crdownload"):
                csv_path = os.path.join(DOWNLOAD_FOLDER, filename)
                new_filename = f"export_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}.csv"
                new_path = os.path.join(DOWNLOAD_FOLDER, new_filename)
                os.rename(csv_path, new_path)
                print(f"CSV file downloaded: {new_path}")
                driver.quit()
                return new_path
        time.sleep(1)
    driver.quit()
    print("Error: CSV file did not appear within timeout.")
    return None

# --------------------------------------------------
# PARSE CSV WITH MAPPING
# --------------------------------------------------
def parse_csv_with_mapping(csv_text, mapping):
    reader = csv.DictReader(io.StringIO(csv_text))
    print("CSV Headers:", reader.fieldnames)
    grants = []
    for row in reader:
        try:
            grant = {}
            for key, map_val in mapping.items():
                if isinstance(map_val, tuple):
                    col, conv = map_val
                    grant[key] = conv(row[col].strip())
                else:
                    grant[key] = row[map_val].strip()
            grants.append(grant)
        except Exception as e:
            print("Skipping row due to error:", e)
    return grants

# --------------------------------------------------
# UNIFIED FUNCTION TO GET GRANTS FROM A SOURCE
# --------------------------------------------------
def get_grants_from_web(source):
    if source not in SOURCES:
        raise ValueError("Unknown source")
    source_config = SOURCES[source]
    url = source_config["url"]
    export_xpath = source_config["export_xpath"]
    mapping = source_config["mapping"]
    
    csv_file = download_csv(url, export_xpath)
    if not csv_file:
        print(f"CSV download failed for {source}")
        return []
    try:
        with open(csv_file, 'r', encoding='utf-8') as f:
            csv_text = f.read()
    except Exception as e:
        print("Error reading CSV file:", e)
        return []
    grants = parse_csv_with_mapping(csv_text, mapping)
    return grants

# --------------------------------------------------
# DATABASE HELPER FUNCTIONS
# --------------------------------------------------
def grant_exists(cursor, opp_number):
    query = "SELECT COUNT(*) FROM grants WHERE opportunity_number = %s"
    cursor.execute(query, (opp_number,))
    result = cursor.fetchone()
    return result[0] > 0

def insert_grants_into_db(grants):
    if not grants:
        print("No grants to insert.")
        return
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        insert_query = """
        INSERT INTO grants 
          (opportunity_number, opportunity_title, posted_date, close_date, min_grant_amount, max_grant_amount, additional_info_url, funding_categories, description)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        count = 0
        for g in grants:
            if grant_exists(cursor, g["opportunity_number"]):
                print(f"Duplicate {g['opportunity_number']}, skipping.")
                continue
            values = (
                g.get("opportunity_number"),
                g.get("opportunity_title"),
                g.get("posted_date"),
                g.get("close_date"),
                g.get("min_grant_amount", ""),
                g.get("max_grant_amount", ""),
                g.get("additional_info_url", ""),
                g.get("funding_categories", ""),
                g.get("description", "")
            )
            cursor.execute(insert_query, values)
            count += 1
        conn.commit()
        print(f"Inserted {count} new grants into the database.")
    except Exception as e:
        print("Error inserting grants:", e)
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

def update_all_grants():
    simpler_grants = get_grants_from_web("simpler")
    print(f"Fetched {len(simpler_grants)} grants from simpler.")
    nih_grants = get_grants_from_web("nih")
    print(f"Fetched {len(nih_grants)} grants from NIH.")
    all_grants = simpler_grants + nih_grants
    if all_grants:
        insert_grants_into_db(all_grants)
    else:
        print("No grants fetched from any source.")

# --------------------------------------------------
# SECURITY: Authentication Decorator
# --------------------------------------------------
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if "user" not in session:
            flash("Please log in to access this page.", "warning")
            return redirect("/login")
        return f(*args, **kwargs)
    return decorated_function

# --------------------------------------------------
# FLASK TEMPLATES
# --------------------------------------------------
# Grants page template with search box and count display.
grants_template = """
<!DOCTYPE html>
<html>
<head>
  <title>Grants Database</title>
  <script src="/static/js/sorttable.js"></script>
  <style>
    table { border-collapse: collapse; width: 100%; }
    th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
    th { cursor: pointer; background-color: #f2f2f2; }
    a { color: blue; text-decoration: underline; }
    .search-box { margin-bottom: 20px; }
  </style>
</head>
<body>
  <h1>Grants Database</h1>
  <div class="search-box">
    <form method="get" action="/grants">
      <input type="text" name="keyword" placeholder="Search grants..." value="{{ keyword|default('') }}">
      <button type="submit">Search</button>
    </form>
    <p>{{ count_text }}</p>
  </div>
  <table class="sortable">
    <thead>
      <tr>
        <th>ID</th>
        <th>Opportunity Number</th>
        <th>Opportunity Title</th>
        <th>Posted Date</th>
        <th>Close Date</th>
        <th>Award Floor</th>
        <th>Award Ceiling</th>
        <th>Additional Info URL</th>
        <th>Description</th>
      </tr>
    </thead>
    <tbody>
      {% for row in grants %}
      <tr>
        <td>{{ row[0] }}</td>
        <td>{{ row[1] }}</td>
        <td>{{ row[2] }}</td>
        <td>{{ row[3] }}</td>
        <td>{{ row[4] }}</td>
        <td>{{ row[5] }}</td>
        <td>{{ row[6] }}</td>
        <td><a href="{{ row[7] }}" target="_blank">{{ row[7] }}</a></td>
        <td>{{ row[8] }}</td>
      </tr>
      {% endfor %}
    </tbody>
  </table>
</body>
</html>
"""

login_template = """
<!DOCTYPE html>
<html>
<head>
  <title>Login</title>
  <style>
    body { font-family: Arial, sans-serif; padding: 20px; }
    .container { max-width: 300px; margin: auto; }
    .flashes { list-style-type: none; padding: 0; }
    .flashes li { margin: 5px 0; padding: 10px; border: 1px solid #ccc; }
    .success { background-color: #d4edda; border-color: #c3e6cb; }
    .warning { background-color: #fff3cd; border-color: #ffeeba; }
    .danger { background-color: #f8d7da; border-color: #f5c6cb; }
  </style>
</head>
<body>
  <div class="container">
    <h1>Login</h1>
    {% with messages = get_flashed_messages(with_categories=true) %}
      {% if messages %}
        <ul class="flashes">
          {% for category, message in messages %}
            <li class="{{ category }}">{{ message }}</li>
          {% endfor %}
        </ul>
      {% endif %}
    {% endwith %}
    <form method="post">
      <input type="email" name="email" placeholder="Email" required>
      <input type="password" name="password" placeholder="Password" required>
      <button type="submit">Login</button>
    </form>
    <p><a href="{{ url_for('signup') }}">Sign Up</a> | <a href="{{ url_for('forgot_password') }}">Forgot Password</a></p>
  </div>
</body>
</html>
"""

signup_template = """
<!DOCTYPE html>
<html>
<head>
  <title>Sign Up</title>
  <style>
    body { font-family: Arial, sans-serif; padding: 20px; }
    .container { max-width: 300px; margin: auto; }
  </style>
</head>
<body>
  <div class="container">
    <h1>Sign Up</h1>
    <form method="post">
      <input type="email" name="email" placeholder="Email" required>
      <input type="password" name="password" placeholder="Password" required>
      <button type="submit">Sign Up</button>
    </form>
    <p><a href="{{ url_for('login') }}">Already have an account? Login</a></p>
  </div>
</body>
</html>
"""

forgot_password_template = """
<!DOCTYPE html>
<html>
<head>
  <title>Forgot Password</title>
  <style>
    body { font-family: Arial, sans-serif; padding: 20px; }
    .container { max-width: 300px; margin: auto; }
  </style>
</head>
<body>
  <div class="container">
    <h1>Forgot Password</h1>
    <form method="post">
      <input type="email" name="email" placeholder="Email" required>
      <button type="submit">Reset Password</button>
    </form>
    <p><a href="{{ url_for('login') }}">Back to Login</a></p>
  </div>
</body>
</html>
"""

# --------------------------------------------------
# FLASK ROUTES
# --------------------------------------------------
@app.route("/")
def index():
    if "user" in session:
        return redirect("/grants")
    else:
        return redirect("/login")

@app.route("/grants", methods=["GET"])
@login_required
def grants_page():
    keyword = request.args.get("keyword", "").strip()
    base_query = """
      SELECT id, opportunity_number, opportunity_title, posted_date, close_date,
             min_grant_amount, max_grant_amount, additional_info_url, description
      FROM grants
    """
    params = []
    if keyword:
        base_query += " WHERE opportunity_title LIKE %s OR description LIKE %s"
        params.extend([f"%{keyword}%", f"%{keyword}%"])
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute(base_query, params)
        rows = cursor.fetchall()
        grant_count = len(rows)
    except Exception as e:
        return f"Database error: {e}"
    finally:
        cursor.close()
        if conn.is_connected():
            conn.close()
    
    if keyword:
        count_text = f"{grant_count} matching grant(s) found."
    else:
        count_text = f"{grant_count} grants found."
    
    return render_template_string(grants_template, grants=rows, count_text=count_text, keyword=keyword)

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        email = request.form["email"]
        password = request.form["password"]
        API_KEY = "AIzaSyBWq_JO8BInbNbbedbZc1blo-fbIeC6qMI"
        signin_url = f"https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword?key={API_KEY}"
        payload = {"email": email, "password": password, "returnSecureToken": True}
        signin_response = requests.post(signin_url, json=payload)
        if signin_response.status_code == 200:
            session["user"] = email
            flash("Logged in successfully.", "success")
            return redirect("/grants")
        else:
            flash("Invalid username and password combination. Please enter valid credentials or register.", "danger")
            return redirect("/login")
    return render_template_string(login_template)

@app.route("/signup", methods=["GET", "POST"])
def signup():
    if request.method == "POST":
        email = request.form["email"]
        password = request.form["password"]
        try:
            user = auth.create_user(email=email, password=password)
            print(f"Created user: {user.uid}")
        except Exception as e:
            flash("Sign up failed: " + str(e), "danger")
            return redirect("/signup")
        API_KEY = "AIzaSyBWq_JO8BInbNbbedbZc1blo-fbIeC6qMI"
        signin_url = f"https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword?key={API_KEY}"
        payload = {"email": email, "password": password, "returnSecureToken": True}
        signin_response = requests.post(signin_url, json=payload)
        if signin_response.status_code == 200:
            id_token = signin_response.json().get("idToken")
            if not id_token:
                flash("Sign up successful, but did not receive an ID token.", "warning")
                return redirect("/login")
            verify_url = f"https://identitytoolkit.googleapis.com/v1/accounts:sendOobCode?key={API_KEY}"
            verify_payload = {"requestType": "VERIFY_EMAIL", "idToken": id_token}
            verify_response = requests.post(verify_url, json=verify_payload)
            if verify_response.status_code == 200:
                flash("Sign up successful. A verification email has been sent. Please verify your email and then log in.", "success")
            else:
                flash("Sign up successful, but failed to send verification email.", "warning")
        else:
            flash("Sign up successful, but failed to sign in for email verification.", "warning")
        return redirect("/login")
    return render_template_string(signup_template)

@app.route("/forgot_password", methods=["GET", "POST"])
def forgot_password():
    if request.method == "POST":
        email = request.form["email"]
        API_KEY = "AIzaSyBWq_JO8BInbNbbedbZc1blo-fbIeC6qMI"
        url = f"https://identitytoolkit.googleapis.com/v1/accounts:sendOobCode?key={API_KEY}"
        payload = {"requestType": "PASSWORD_RESET", "email": email}
        response = requests.post(url, json=payload)
        if response.status_code == 200:
            flash("Password reset email sent. Check your inbox.", "success")
        else:
            flash("Failed to send password reset email.", "danger")
        return redirect("/login")
    return render_template_string(forgot_password_template)

@app.route("/logout")
def logout():
    session.pop("user", None)
    flash("Logged out successfully.", "success")
    return redirect("/login")

# --------------------------------------------------
# SCHEDULE UPDATES & OPEN BROWSER (Optional)
# --------------------------------------------------
def open_browser():
    import webbrowser
    webbrowser.open("http://127.0.0.1:5000/login")

def schedule_updates(interval=300):
    threading.Timer(interval, schedule_updates, args=[interval]).start()

# --------------------------------------------------
# MAIN UPDATE FUNCTION
# --------------------------------------------------
def update_all_grants():
    simpler_grants = get_grants_from_web("simpler")
    print(f"Fetched {len(simpler_grants)} grants from simpler.")
    nih_grants = get_grants_from_web("nih")
    print(f"Fetched {len(nih_grants)} grants from NIH.")
    all_grants = simpler_grants + nih_grants
    if all_grants:
        insert_grants_into_db(all_grants)
    else:
        print("No grants fetched from any source.")

# --------------------------------------------------
# MAIN
# --------------------------------------------------
if __name__ == "__main__":
    update_all_grants()
    schedule_updates(300)  # Optional scheduled updates.
    threading.Timer(5, open_browser).start()
    print("Starting Flask server on http://127.0.0.1:5000/")
    app.run(debug=True, use_reloader=False)