import os
import requests
import json

# ==========================================
# 🛑 CONFIGURATION & SECRETS
# Using your provided placeholders for local execution.
# When running in GitHub Actions, it will pull from your Repository Secrets.
# ==========================================

# 1. Discord Webhook
DISCORD_WEBHOOK_URL = os.environ.get('DISCORD_WEBHOOK_URL', 'https://discord.com/api/webhooks/YOUR_FAKE_PLACEHOLDER')

# 2. Adzuna Credentials
ADZUNA_APP_ID = os.environ.get('ADZUNA_APP_ID', '721a4f20')
ADZUNA_APP_KEY = os.environ.get('ADZUNA_APP_KEY', 'fe4a4b6b7ce55491d418ee2511b58bf3')

# 3. Reed API Key
REED_API_KEY = os.environ.get('REED_API_KEY', 'YOUR_REED_API_KEY_HERE')

# Search Criteria
ADZUNA_KEYWORDS = ["devsecops", "appsec", "python security", "cloud security"]
REED_KEYWORDS = "(devsecops OR appsec OR python) NOT (Graduate OR Trainee)"

# 🔥 ENHANCED BLACKLIST: Add "commerce" and "full stack" to kill the noise
BLACKLIST = ["graduate", "trainee", "recruitment", "sales", "retail", "commerce", "full stack", "frontend", "front-end"]

# ==========================================
# 📡 MODULE 1: ADZUNA API
# ==========================================
def fetch_adzuna_london():
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
                data = response.json()
                jobs = data.get('results', [])
                for job in jobs:
                    job_id = str(job.get('id'))
                    title = job.get("title", "").replace('<strong>', '').replace('</strong>', '')
                    
                    if any(word in title.lower() for word in BLACKLIST) or job_id in unique_jobs:
                        continue

                    company_name = job.get("company", {}).get("display_name", "Unknown Company")
                    s_min, s_max = job.get('salary_min'), job.get('salary_max')
                    salary_str = f"£{int(s_min)} - £{int(s_max)}" if s_min and s_max else "💰 Unlisted"

                    unique_jobs[job_id] = {
                        "title": title,
                        "company": company_name,
                        "salary": salary_str,
                        "link": job.get("redirect_url"),
                        "source": "Adzuna"
                    }
        except Exception as e:
            print(f"  ⚠️ Adzuna request failed for '{kw}': {e}")
        
    found_list = list(unique_jobs.values())
    print(f"✅ Adzuna found {len(found_list)} unique relevant jobs.")
    return found_list

# ==========================================
# 📡 MODULE 2: REED API
# ==========================================
def fetch_reed_london():
    print("🇬🇧 Fetching live London jobs from Reed API...")
    url = "https://www.reed.co.uk/api/1.0/search"
    
    if REED_API_KEY == 'YOUR_REED_API_KEY_HERE':
        print("⚠️ Skipping Reed: No API Key provided.")
        return []

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
            title = job.get("jobTitle", "")
            if any(word in title.lower() for word in BLACKLIST):
                continue

            min_sal, max_sal = job.get('minimumSalary'), job.get('maximumSalary')
            salary_str = f"£{min_sal} - £{max_sal}" if min_sal and max_sal else "💰 Unlisted"

            found_jobs.append({
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
# 🚀 DISCORD NOTIFICATION ENGINE
# ==========================================
def send_to_discord(jobs):
    if not jobs:
        print("🛑 No London jobs found today. Staying silent.")
        return

    print(f"🚀 Preparing to send {len(jobs)} jobs to Discord...")
    embeds = []
    for job in jobs:
        embeds.append({
            "title": f"[{job['source']}] {job['title']}",
            "description": f"**Company:** {job['company']}\n**Salary:** {job['salary']}\n**Link:** [Apply Here]({job['link']})",
            "color": 15158332 # London Red
        })

    payload = {"content": f"🇬🇧 **Latest London DevSecOps & Python Roles** 🇬🇧", "embeds": embeds}

    try:
        response = requests.post(DISCORD_WEBHOOK_URL, json=payload)
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
    
    # INTERLEAVE: Take top 5 from Adzuna and top 5 from Reed
    final_selection = adzuna_list[:5] + reed_list[:5]
    final_selection = final_selection[:10]
    
    print(f"📊 Final aggregated count: {len(final_selection)}")
    send_to_discord(final_selection)
    print("--- Pipeline Finished ---")
