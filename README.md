# ⚙️ DevSecOps Market Intelligence: The Ingestion Engine

> **Note:** This repository houses the backend data pipeline and web scrapers. The React frontend and Edge API for this project can be found here: **[🔗 my-job-board](https://github.com/SMCallan/my-job-board)**

This is the automated data ingestion pipeline for the DevSecOps Market Intelligence Engine. Built with Python and heavily automated via GitHub Actions, this engine operates as a zero-maintenance, serverless data scraper.

### 🛠️ The Pipeline
1. **Automated Triggers:** A GitHub Actions CRON job fires daily at 11:31 AM UTC.
2. **Data Extraction:** `scraper.py` executes, querying the Adzuna and Reed APIs to pull 500+ live cybersecurity roles.
3. **Transformation & Load:** The Python script cleans the JSON payloads, normalises the data structures, and commits the fresh leads securely to a Cloudflare D1 (SQLite) database.

**Core Tech Stack:** Python 3, GitHub Actions (CI/CD), SQLite3, REST APIs.
