# ⚙️ Secure Engineering Role Radar: Ingestion Engine

> **Note:** This repository contains the backend ingestion pipeline, scoring engine and job-source integrations.  
> The React frontend and Cloudflare Workers Edge API live here: **[my-job-board](https://github.com/SMCallan/my-job-board)**

This is the automated data pipeline behind the **Secure Engineering Role Radar**: a serverless market-intelligence system for tracking London software, platform, AppSec, DevSecOps, cloud security and AI security roles.

It is more than a scraper. The engine pulls live roles from job APIs, normalises noisy salary and role data, filters out low-signal listings, scores opportunities against a secure engineering profile, stores enriched records in Cloudflare D1, and sends a curated Discord digest of the highest-value new roles.

---

## 🎯 Purpose

The system is designed to answer a practical question:

> Which London engineering roles are genuinely worth reviewing or applying to based on role fit, salary, company quality and culture-risk signals?

Instead of treating every job advert equally, the pipeline prioritises roles aligned with:

- secure full-stack engineering
- application security and product security
- platform engineering and developer experience
- cloud security and DevSecOps
- security automation
- AI security and AI evaluation
- technically credible solutions/security engineering roles

The target salary range is tuned around a realistic **£60k–£85k core band**, with higher-scoring stretch roles above that range.

---

## 🛠️ Pipeline Overview

1. **Automated Trigger — GitHub Actions**  
   A scheduled GitHub Actions workflow runs the pipeline automatically, keeping the dataset fresh without manual maintenance.

2. **Data Extraction — Adzuna + Reed APIs**  
   `scraper.py` queries Adzuna and Reed for London-based engineering and security roles using a calibrated keyword strategy.

3. **Noise Reduction**  
   The engine removes low-relevance listings such as graduate schemes, internships, junior roles, helpdesk/support roles, recruitment posts, retail roles and unrelated analyst/admin listings.

4. **Normalisation**  
   Job titles, companies, descriptions, salary fields, locations and links are cleaned into a consistent format.

5. **Salary Parsing**  
   The salary parser handles messy job-board formats such as:

   - `£60,000 - £85,000`
   - `£60k - £85k`
   - `£500 - £650 per day`
   - unlisted or incomplete salary data

6. **Role Scoring**  
   Each job is scored out of 100 using a weighted fit model covering:

   - role alignment
   - skill overlap
   - salary band
   - culture-risk indicators
   - company quality
   - action recommendations derived from the score and available apply links

7. **Cloudflare D1 Storage**  
   Enriched job records are written to Cloudflare D1 using parameterised SQL queries, avoiding manual string interpolation for untrusted external job-board data.

8. **Discord Digest**  
   High-fit new roles are sent to Discord as a curated daily digest, reducing notification noise and surfacing only the most relevant opportunities.

---

## 🧠 Scoring Model

Each role receives a `fit_score` from 0 to 100.

| Category | Weight | Purpose |
|---|---:|---|
| Role Fit | 30 | Rewards strong matches for secure full-stack, AppSec, platform, DevEx, product security and AI security roles |
| Skill Match | 25 | Scores overlap with Python, TypeScript, React, Node.js, SQL, Docker, CI/CD, APIs, cloud, OWASP and security tooling |
| Salary Fit | 20 | Prioritises £60k–£85k core roles and flags higher-value stretch opportunities |
| Culture Risk | 15 | Detects pressure signals such as heavy on-call, “rockstar”, “wear many hats”, “fast-paced”, 24/7 and hypergrowth language |
| Company Quality | 10 | Boosts target companies and mature engineering/security organisations |

The pipeline also emits `action_recommendation` and `action_urgency` fields. These are derived from fit score, salary fit, culture-risk result and whether the source API supplied an apply URL. Adzuna and Reed do not provide a reliable native “easy apply” flag in the fields consumed here, so “Apply now” means “high-value role worth immediate application”, not “one-click apply is supported by the job board”.

---

## 🏷️ Enriched Job Metadata

The pipeline preserves the original frontend-compatible fields:

- `id`
- `title`
- `company`
- `salary`
- `link`
- `timestamp`

It also adds richer fields for ranking, filtering and future dashboard features:

- `source`
- `location`
- `description`
- `salary_min`
- `salary_max`
- `salary_type`
- `salary_band`
- `fit_score`
- `role_track`
- `culture_risk`
- `seniority`
- `action_recommendation`
- `action_urgency`
- `tags_json`
- `score_reasons_json`
- `raw_json`
- `last_seen_at`

---

## 🔍 Target Role Tracks

The engine is calibrated for roles such as:

- Software Engineer, Full Stack
- Secure Software Engineer
- Security Software Engineer
- Application Security Engineer
- Product Security Engineer
- Platform Engineer
- Developer Tools Engineer
- Developer Experience Engineer
- Internal Tools Engineer
- Cloud Security Engineer
- DevSecOps Engineer
- Site Reliability Engineer
- AI Security Engineer
- AI Evaluation Engineer
- Trust & Safety Engineer
- Security Automation Engineer
- Technical/Solutions Engineer, where the role remains genuinely technical

---

## ⚠️ Culture-Risk Detection

The scraper looks for advert language that may indicate poor boundaries, excessive pressure or unclear expectations.

Examples of risk terms:

- `fast-paced environment`
- `high-pressure`
- `wear many hats`
- `rockstar`
- `ninja`
- `10x`
- `work hard play hard`
- `urgent requirement`
- `24/7`
- `on-call rota`
- `out of hours`
- `weekend work`
- `startup mindset`
- `hypergrowth`
- `comfortable with ambiguity`

It also detects positive signals such as:

- `work-life balance`
- `flexible working`
- `hybrid working`
- `remote-first`
- `training budget`
- `mentorship`
- `career development`
- `documentation`
- `secure development lifecycle`
- `accessibility`
- `inclusive culture`
- `reasonable adjustments`
- `sustainable pace`

---

## 🚦 Action Recommendations

The frontend should use backend-generated action fields rather than inventing dashboard counters from raw source data:

- `Apply now` / tag `apply-now`: high fit score, core/stretch salary, an apply URL is present and no blocking culture-risk signal is detected.
- `Shortlist` / tag `shortlist`: good fit worth reviewing or tailoring before applying.
- `Research culture first` / tag `high-culture-risk`: enough pressure/on-call/chaos language exists that the role should not be treated as an immediate application.
- `Review manually` / tag `manual-review`: the API did not supply an apply URL, so the role cannot be actioned directly from the app.
- `Review carefully`: insufficient combined role/salary/culture evidence for an immediate apply recommendation.

Design note for the companion frontend: if a metric cannot be backed by these fields, hide it rather than showing a decorative counter. The API supports professional, decision-focused cards around score, action, salary band, culture badge and score reasons.

---

## 🧱 Core Tech Stack

- **Language:** Python 3
- **Automation:** GitHub Actions
- **APIs:** Adzuna API, Reed API
- **Database:** Cloudflare D1 / SQLite
- **Alerts:** Discord Webhooks
- **Deployment Model:** Serverless scheduled ingestion
- **Frontend/API Companion:** React, Vite, Cloudflare Workers and Cloudflare Pages

---

## 🔐 Security Notes

This project treats third-party job data as untrusted external input.

Security-conscious implementation details include:

- environment-variable based secret management
- no hardcoded API keys
- parameterised Cloudflare D1 queries
- HTML stripping and text normalisation
- bounded raw JSON storage
- request timeouts and retry handling
- deduplication by source ID and title/company fingerprint
- additive schema migration rather than destructive table resets

---

## ⚙️ Environment Variables

Required for full production use:

```bash
CF_ACCOUNT_ID=
CF_DATABASE_ID=
CF_API_TOKEN=
````

Required for job-source ingestion:

```bash
ADZUNA_APP_ID=
ADZUNA_APP_KEY=
REED_API_KEY=
```

Optional:

```bash
DISCORD_WEBHOOK_URL=
JOB_LOCATION=london
RETENTION_DAYS=30
MINIMUM_SAVE_SCORE=40
MINIMUM_ALERT_SCORE=65
REQUEST_TIMEOUT=15
MAX_ADZUNA_RESULTS_PER_KEYWORD=50
MAX_REED_RESULTS=100
```

---

## 🚀 Running Locally

```bash
# Clone the repository
git clone https://github.com/SMCallan/JobDevOpEngine.git

# Move into the project
cd JobDevOpEngine

# Create a virtual environment
python3 -m venv .venv

# Activate it
source .venv/bin/activate

# Install dependencies
pip install requests

# Run the scraper
python scraper.py
```

Without Cloudflare credentials, the pipeline can still fetch, score and print roles locally, but it will skip database writes.

---

## 📊 Example Output

```text
--- Starting Secure Engineering Role Radar pipeline ---
🇬🇧 Fetching London roles from Adzuna...
🇬🇧 Fetching London roles from Reed...
📥 Pulled 312 raw jobs from all sources.
🧹 Deduped to 184 unique roles.
🎯 67 roles passed the minimum fit score of 40.
💾 12 brand new roles identified.

🏆 Top roles this run:
  -  84/100 | Application Security      | Product Security Engineer               | Target Company       | £75,000 - £95,000 | Apply now
  -  79/100 | Platform / DevEx           | Platform Engineer                       | Mature Tech Co       | £65,000 - £85,000 | Shortlist
  -  76/100 | Secure Full-Stack          | Full Stack Engineer                     | Security SaaS Co     | £60,000 - £80,000 | Apply now
--- Pipeline finished ---
```

---

## 🧭 Project Direction

Planned improvements include:

* frontend display of fit scores, role tracks and action recommendations
* salary-band filtering
* culture-risk badges backed by `culture_risk` and `high-culture-risk` tags
* saved/applied/interview application tracking
* weekly Markdown or CSV export of top roles
* source reliability scoring
* company reputation metadata
* richer AppSec and platform-engineering taxonomy
* tests for salary parsing, scoring and blacklist logic

---

## 📌 Summary

This repository powers a live, automated job-market intelligence engine for secure engineering roles in London.

It demonstrates:

* Python automation
* REST API integration
* CI/CD scheduling
* serverless data storage
* secure handling of untrusted external data
* salary parsing and data normalisation
* role classification and scoring
* practical DevSecOps workflow design
* accessible, data-driven career tooling

The result is a focused pipeline that turns noisy public job-board data into ranked, explainable and actionable engineering opportunities.

```
```
