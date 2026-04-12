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
# 🎯 SEARCH CRITERIA & FILTERS
# ==========================================
ADZUNA_KEYWORDS = ["devsecops", "appsec", "python security", "cloud security"]
REED_KEYWORDS = "(devsecops OR appsec OR python) NOT (Graduate OR Trainee)"

# Kill the noise: Ignore roles containing these keywords in the title
BLACKLIST = ["graduate", "trainee", "recruitment", "sales", "retail", "commerce", "full stack", "frontend", "front-end"]


# ==========================================
# 🗄️ MODULE 1: CLOUDFLARE D1 STATE MANAGEMENT
# ==========================================
def get_cf_url() -> str:
    """Constructs the Cloudflare D1 Query API endpoint."""
    return f"https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT_ID}/d1/database/{CF_DATABASE_ID}/query"

def get_cf_headers() -> dict:
    """Constructs the Auth headers for Cloudflare."""
    return {
        "Authorization": f"Bearer {CF_API_TOKEN}", 
        "Content-Type": "application/json"
    }

def is_new_job(job_id: str) -> bool:
    """Queries D1 to check if we have already sent this job to Discord."""
    if not all([CF_ACCOUNT_ID, CF_DATABASE_ID, CF_API_TOKEN]):
        print("⚠️ DB Check Skipped: Missing Cloudflare Credentials in Environment.")
        return True 

    # FIX: Cloudflare REST API prefers raw SQL strings over the 'params' array
    payload = {"sql": f"SELECT id FROM seen_jobs WHERE id = '{job_id}'"}
    
    try:
        response = requests.post(get_cf_url(), json=payload, headers=get_cf_headers(), timeout=10)
        
        # Verbose error logging so we can see the exact Cloudflare rejection reason
        if response.status_code != 200:
            print(f"⚠️ DB Read Error {response.status_code} for {job_id}: {response.text}")
            return True
            
        data = response.json()
        results = data.get('result', [{}])[0].get('results', [])
        return len(results) == 0
    except Exception as e:
        print(f"⚠️ DB Read Check failed for {job_id}: {e}")
        return True 

def save_job_to_db(job_id: str) -> None:
    """Inserts a successfully processed job ID into the D1 database."""
    if not all([CF_ACCOUNT_ID, CF_DATABASE_ID, CF_API_TOKEN]):
        return

    # FIX: Direct string interpolation for the INSERT
    payload = {"sql": f"INSERT INTO seen_jobs (id) VALUES ('{job_id}')"}
    
    try:
        response = requests.post(get_cf_url(), json=payload, headers=get_cf_headers(), timeout=10)
        if response.status_code != 200:
            print(f"⚠️ DB Write Error {response.status_code} for {job_id}: {response.text}")
    except Exception as e:
        print(f"⚠️ DB Write failed for {job_id}: {e}")

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
            "results_per_page": 15,
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
                    prefixed_id = f"adzuna_{raw_id}" # Prefix guarantees global uniqueness
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
        "distanceFromLocation": 5
    }

    found_jobs = []
    try:
        response = requests.get(url, params=params, auth=(REED_API_KEY, ''), timeout=10)
        response.raise_for_status()
        jobs = response.json().get('results', [])
        
        for job in jobs:
            raw_id = str(job.get('jobId'))
            prefixed_id = f"reed_{raw_id}" # Prefix guarantees global uniqueness
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
        print("🛑 No new London jobs found today. Staying silent.")
        return

    print(f"🚀 Preparing to send {len(jobs)} NEW jobs to Discord...")
    embeds = []
    for job in jobs:
        embeds.append({
            "title": f"[{job['source']}] {job['title']}",
            "description": f"**Company:** {job['company']}\n**Salary:** {job['salary']}\n**Link:** [Apply Here]({job['link']})",
            "color": 15158332 # London Red
        })

    payload = {"content": f"🇬🇧 **Latest London DevSecOps & Python Roles** 🇬🇧", "embeds": embeds}

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
    
    adzuna_list = fetch_adzuna_london()
    reed_list = fetch_reed_london()
    
    # INTERLEAVE: Take top candidates from both platforms
    final_selection = adzuna_list[:10] + reed_list[:10]
    
    print(f"📊 Filtering {len(final_selection)} total candidates through D1 Database...")
    
    # DATABASE FILTERING
    new_jobs_only = []
    for job in final_selection:
        if is_new_job(job['id']):
            new_jobs_only.append(job)
            save_job_to_db(job['id'])
            
            # Stop if we hit our daily maximum of 10 to prevent Discord spam
            if len(new_jobs_only) >= 10:
                break
    
    if new_jobs_only:
        send_to_discord(new_jobs_only)
    else:
        print("☕ Database check complete: No new roles found. Enjoy your day!")
        
    print("--- Pipeline Finished ---")
