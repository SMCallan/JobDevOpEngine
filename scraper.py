import os
import requests
from typing import List, Dict, Any

# ==========================================
# 🛑 SECURE CONFIGURATION
# Keys are injected dynamically via Environment Variables (GitHub Actions)
# ==========================================

# 1. Discord Webhook
DISCORD_WEBHOOK_URL = os.environ.get('DISCORD_WEBHOOK_URL')

# 2. Adzuna Credentials
ADZUNA_APP_ID = os.environ.get('ADZUNA_APP_ID')
ADZUNA_APP_KEY = os.environ.get('ADZUNA_APP_KEY')

# 3. Reed API Key
REED_API_KEY = os.environ.get('REED_API_KEY')

# 4. Cloudflare D1 Credentials (State Management)
CF_ACCOUNT_ID = os.environ.get('CF_ACCOUNT_ID')
CF_DATABASE_ID = os.environ.get('CF_DATABASE_ID')
CF_API_TOKEN = os.environ.get('CF_API_TOKEN')

# ==========================================
# 🎯 SEARCH CRITERIA & FILTERS (BROADENED FOR UI SEARCH)
# ==========================================
# We cast a wider net here so the Database fills up, allowing the React UI search bar to do the heavy lifting.
ADZUNA_KEYWORDS = ["security engineer", "appsec", "python", "cloud security", "devsecops", "devops"]
REED_KEYWORDS = "(security OR appsec OR python OR devops) NOT (Graduate OR Trainee)"

# Kill the noise: Ignore roles containing these keywords in the title
BLACKLIST = ["graduate", "trainee", "recruitment", "sales", "retail", "commerce", "full stack", "frontend", "front-end"]


# ==========================================
# 🗄️ MODULE 1: CLOUDFLARE D1 (Direct HTTP)
# ==========================================
def run_d1_query(sql_query: str) -> dict:
    """Executes raw SQL against the Cloudflare D1 HTTP API."""
    url = f"https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT_ID}/d1/database/{CF_DATABASE_ID}/query"
    headers = {
        "Authorization": f"Bearer {CF_API_TOKEN}",
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.post(url, json={"sql": sql_query}, headers=headers, timeout=10)
        if response.status_code != 200:
            print(f"❌ CF API Error ({response.status_code}): {response.text}")
        return response.json()
    except Exception as e:
        print(f"❌ Network Error to CF: {e}")
        return {}

def init_db() -> None:
    """Forces the expanded table to exist so we don't get 'table not found' errors."""
    print("⚙️ Initializing Cloudflare DB Table (Full Schema)...")
    run_d1_query("CREATE TABLE IF NOT EXISTS jobs (id TEXT PRIMARY KEY, title TEXT, company TEXT, salary TEXT, link TEXT, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP);")

def clean_old_jobs() -> None:
    """Deletes jobs older than 30 days to keep the database lean."""
    print("🧹 Pruning jobs older than 30 days from the database...")
    run_d1_query("DELETE FROM jobs WHERE timestamp < datetime('now', '-30 days');")

def is_new_job(job_id: str) -> bool:
    """Queries D1 to see if the job ID already exists in the new 'jobs' table."""
    if not all([CF_ACCOUNT_ID, CF_DATABASE_ID, CF_API_TOKEN]):
        return True 

    data = run_d1_query(f"SELECT id FROM jobs WHERE id = '{job_id}';")
    
    try:
        results = data.get('result', [{}])[0].get('results', [])
        return len(results) == 0
    except Exception:
        return True # Default to True if parsing fails so you don't miss jobs

def save_job_to_db(job: dict) -> None:
    """Saves the full job details to prevent future duplicates AND populate the website."""
    if not all([CF_ACCOUNT_ID, CF_DATABASE_ID, CF_API_TOKEN]):
        return
        
    # Escape single quotes for SQL insertion
    title = job['title'].replace("'", "''")
    company = job['company'].replace("'", "''")
    salary = job['salary'].replace("'", "''")
    link = job['link'].replace("'", "''")
    
    # INSERT OR IGNORE prevents a crash if the ID somehow already exists
    sql = f"INSERT OR IGNORE INTO jobs (id, title, company, salary, link) VALUES ('{job['id']}', '{title}', '{company}', '{salary}', '{link}');"
    run_d1_query(sql)


# ==========================================
# 📡 MODULE 2: ADZUNA API
# ==========================================
def fetch_adzuna_london() -> List[Dict[str, str]]:
    print("🇬🇧 Fetching live London jobs from Adzuna API...")
    url = "https://api.adzuna.com/v1/api/jobs/gb/search/1"
    unique_jobs = {}
    headers = {"Accept": "application/json"}

    for kw in ADZUNA_KEYWORDS:
        print(f"  🔍 Searching Adzuna for: '{kw}'...")
        params = {
            "app_id": ADZUNA_APP_ID,
            "app_key": ADZUNA_APP_KEY,
            "results_per_page": 50, # Increased payload per page
            "what": kw,
            "where": "london",
            "sort_by": "date"
        }

        try:
            response = requests.get(url, params=params, headers=headers, timeout=10)
            if response.status_code == 200:
                jobs = response.json().get('results', [])
                
                for job in jobs:
                    raw_id = str(job.get('id'))
                    prefixed_id = f"adzuna_{raw_id}"
                    title = job.get("title", "").replace('<strong>', '').replace('</strong>', '')
                    
                    if any(word in title.lower() for word in BLACKLIST) or prefixed_id in unique_jobs:
                        continue

                    company_name = job.get("company", {}).get("display_name", "Unknown Company")
                    s_min, s_max = job.get('salary_min'), job.get('salary_max')
                    salary_str = f"£{int(s_min)} - £{int(s_max)}" if s_min and s_max else "💰 Unlisted"

                    unique_jobs[prefixed_id] = {
                        "id": prefixed_id,
                        "title": title,
                        "company": company_name,
                        "salary": salary_str,
                        "link": job.get("redirect_url"),
                        "source": "Adzuna"
                    }
        except Exception as e:
            print(f"  ⚠️ Adzuna request failed for '{kw}': {e}")
        
    found_list = list(unique_jobs.values())
    print(f"✅ Adzuna found {len(found_list)} relevant jobs.")
    return found_list


# ==========================================
# 📡 MODULE 3: REED API
# ==========================================
def fetch_reed_london() -> List[Dict[str, str]]:
    print("🇬🇧 Fetching live London jobs from Reed API...")
    if not REED_API_KEY or REED_API_KEY == 'YOUR_REED_API_KEY_HERE':
        print("⚠️ Skipping Reed: No API Key provided.")
        return []

    url = "https://www.reed.co.uk/api/1.0/search"
    params = {
        "keywords": REED_KEYWORDS,
        "locationName": "london",
        "distanceFromLocation": 5,
        "resultsToTake": 100 # Increased payload
    }

    found_jobs = []
    try:
        response = requests.get(url, params=params, auth=(REED_API_KEY, ''), timeout=10)
        response.raise_for_status()
        jobs = response.json().get('results', [])
        
        for job in jobs:
            raw_id = str(job.get('jobId'))
            prefixed_id = f"reed_{raw_id}"
            title = job.get("jobTitle", "")
            
            if any(word in title.lower() for word in BLACKLIST):
                continue

            min_sal, max_sal = job.get('minimumSalary'), job.get('maximumSalary')
            salary_str = f"£{min_sal} - £{max_sal}" if min_sal and max_sal else "💰 Unlisted"

            found_jobs.append({
                "id": prefixed_id,
                "title": title,
                "company": job.get("employerName"),
                "salary": salary_str,
                "link": job.get("jobUrl"),
                "source": "Reed"
            })
            
    except Exception as e:
        print(f"⚠️ Reed API failed: {e}")
        
    print(f"✅ Reed found {len(found_jobs)} relevant jobs.")
    return found_jobs


# ==========================================
# 🚀 MODULE 4: DISCORD NOTIFICATION ENGINE
# ==========================================
def send_to_discord(jobs: List[Dict[str, str]]) -> None:
    if not jobs:
        return

    print(f"🚀 Preparing to send {len(jobs)} NEW jobs to Discord...")
    embeds = []
    for job in jobs:
        embeds.append({
            "title": f"[{job['source']}] {job['title']}",
            "description": f"**Company:** {job['company']}\n**Salary:** {job['salary']}\n**Link:** [Apply Here]({job['link']})",
            "color": 15158332 # London Red
        })

    payload = {"content": f"🇬🇧 **Latest Top 10 London Roles** 🇬🇧", "embeds": embeds}

    try:
        response = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=10)
        response.raise_for_status()
        print("✅ Successfully sent jobs to Discord!")
    except Exception as e:
        print(f"❌ Failed to send to Discord: {e}")


# ==========================================
# ⚙️ MAIN EXECUTION PIPELINE
# ==========================================
if __name__ == "__main__":
    print("--- Starting UK API Job Aggregator Pipeline ---")
    
    # 1. Initialize & Clean
    if all([CF_ACCOUNT_ID, CF_DATABASE_ID, CF_API_TOKEN]):
        init_db()
        clean_old_jobs()
        
    # 2. Fetch Data
    adzuna_list = fetch_adzuna_london()
    reed_list = fetch_reed_london()
    
    # Widen the net: Take up to 250 from each to build a rich search DB
    final_selection = adzuna_list[:250] + reed_list[:250]
    
    print(f"📊 Filtering {len(final_selection)} total candidates through D1 Database...")
    
    # 3. DATABASE INGESTION: Save EVERYTHING that is new
    new_jobs_only = []
    for job in final_selection:
        if is_new_job(job['id']):
            new_jobs_only.append(job)
            save_job_to_db(job)
            
    print(f"💾 Saved {len(new_jobs_only)} brand new roles to the Database!")
    
    # 4. DISCORD ALERT: Only send the top 10 to prevent notification spam
    if new_jobs_only:
        send_to_discord(new_jobs_only[:10]) 
    else:
        print("☕ Database check complete: No new roles found. Enjoy your day!")
        
    print("--- Pipeline Finished ---")
