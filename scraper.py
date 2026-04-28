
"""
Secure Engineering Role Radar - ingestion engine.

This script:
- Pulls London roles from Adzuna and Reed.
- Normalises titles, salaries, companies and descriptions.
- Scores each role against a secure full-stack / platform / AppSec profile.
- Stores enriched job data in Cloudflare D1 using parameterised queries.
- Sends a Discord digest of the highest-value new opportunities.

Required env vars:
    DISCORD_WEBHOOK_URL      optional, for digest alerts
    ADZUNA_APP_ID            optional, enables Adzuna
    ADZUNA_APP_KEY           optional, enables Adzuna
    REED_API_KEY             optional, enables Reed
    CF_ACCOUNT_ID            required for D1 writes
    CF_DATABASE_ID           required for D1 writes
    CF_API_TOKEN             required for D1 writes

Designed to remain compatible with the existing frontend fields:
    id, title, company, salary, link, timestamp
"""

from __future__ import annotations

import html
import json
import os
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import requests


# ==========================================
# CONFIGURATION
# ==========================================

DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL")

ADZUNA_APP_ID = os.environ.get("ADZUNA_APP_ID")
ADZUNA_APP_KEY = os.environ.get("ADZUNA_APP_KEY")

REED_API_KEY = os.environ.get("REED_API_KEY")

CF_ACCOUNT_ID = os.environ.get("CF_ACCOUNT_ID")
CF_DATABASE_ID = os.environ.get("CF_DATABASE_ID")
CF_API_TOKEN = os.environ.get("CF_API_TOKEN")

LONDON_LOCATION = os.environ.get("JOB_LOCATION", "london")
RETENTION_DAYS = int(os.environ.get("RETENTION_DAYS", "30"))
MINIMUM_SAVE_SCORE = int(os.environ.get("MINIMUM_SAVE_SCORE", "40"))
MINIMUM_ALERT_SCORE = int(os.environ.get("MINIMUM_ALERT_SCORE", "65"))

REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "15"))
MAX_ADZUNA_RESULTS_PER_KEYWORD = int(os.environ.get("MAX_ADZUNA_RESULTS_PER_KEYWORD", "50"))
MAX_REED_RESULTS = int(os.environ.get("MAX_REED_RESULTS", "100"))


# ==========================================
# TARGET SEARCH STRATEGY
# ==========================================

ADZUNA_KEYWORDS = [
    "secure software engineer",
    "security software engineer",
    "software engineer security",
    "full stack engineer",
    "full-stack engineer",
    "backend python",
    "python software engineer",
    "application security engineer",
    "appsec engineer",
    "product security engineer",
    "platform engineer",
    "developer tools engineer",
    "developer experience engineer",
    "internal tools engineer",
    "cloud security engineer",
    "devsecops engineer",
    "site reliability engineer",
    "ai security engineer",
    "ai evaluation engineer",
    "trust and safety engineer",
    "security automation engineer",
]

REED_KEYWORDS = (
    '("secure software engineer" OR "security software engineer" OR '
    '"software engineer security" OR "full stack engineer" OR '
    '"full-stack engineer" OR "backend python" OR "python software engineer" OR '
    '"application security engineer" OR "appsec engineer" OR '
    '"product security engineer" OR "platform engineer" OR '
    '"developer tools engineer" OR "developer experience engineer" OR '
    '"internal tools engineer" OR "cloud security engineer" OR '
    '"devsecops engineer" OR "site reliability engineer" OR '
    '"ai security engineer" OR "ai evaluation engineer" OR '
    '"trust and safety engineer" OR "security automation engineer") '
    'NOT (Graduate OR Trainee OR Junior OR Intern OR Apprentice OR Helpdesk OR "1st line")'
)

BLACKLIST_TERMS = [
    "graduate",
    "trainee",
    "intern",
    "apprentice",
    "junior",
    "recruitment",
    "recruiter",
    "talent acquisition",
    "sales",
    "business development",
    "account executive",
    "retail",
    "store",
    "shop",
    "customer service",
    "support",
    "helpdesk",
    "service desk",
    "1st line",
    "2nd line",
    "desktop support",
    "field engineer",
    "admin",
    "administrator",
    "soc analyst",
    "security analyst",
    "monitoring analyst",
    "teacher",
    "tutor",
    "lecturer",
    "joiner",
    "carpenter",
    "plumber",
    "electrician",
    "mechanic",
    "unpaid",
    "volunteer",
    "chief",
    "vp",
    "director",
    "head of",
]

CORE_ROLE_KEYWORDS = [
    "secure software engineer",
    "security software engineer",
    "software engineer security",
    "full stack engineer",
    "full-stack engineer",
    "fullstack engineer",
    "backend engineer",
    "backend python",
    "python software engineer",
    "application security engineer",
    "appsec engineer",
    "product security engineer",
    "platform engineer",
    "developer tools engineer",
    "developer experience engineer",
    "devex engineer",
    "internal tools engineer",
]

ADJACENT_ROLE_KEYWORDS = [
    "cloud security engineer",
    "devsecops engineer",
    "site reliability engineer",
    "sre",
    "ai security engineer",
    "ai evaluation engineer",
    "trust and safety engineer",
    "security automation engineer",
    "infrastructure engineer",
    "solutions engineer",
    "security consultant",
    "technical consultant",
]

PROFILE_SKILLS = [
    "python",
    "typescript",
    "javascript",
    "react",
    "node",
    "node.js",
    "sql",
    "sqlite",
    "docker",
    "ci/cd",
    "github actions",
    "linux",
    "api",
    "apis",
    "rest",
    "cloudflare",
    "aws",
    "azure",
    "gcp",
    "kubernetes",
    "terraform",
    "owasp",
    "appsec",
    "application security",
    "product security",
    "threat modelling",
    "threat modeling",
    "vulnerability",
    "secure sdlc",
    "devsecops",
    "llm",
    "ai security",
    "prompt injection",
]

TARGET_COMPANIES = [
    "microsoft",
    "github",
    "linkedin",
    "sap",
    "salesforce",
    "servicenow",
    "cisco",
    "adobe",
    "workday",
    "atlassian",
    "bloomberg",
    "mastercard",
    "s&p global",
    "gitlab",
    "elastic",
    "snyk",
    "cloudflare",
    "okta",
    "palo alto networks",
    "crowdstrike",
    "google",
    "arm",
]

CULTURE_RISK_TERMS = [
    "fast-paced environment",
    "high-pressure",
    "thrive under pressure",
    "wear many hats",
    "rockstar",
    "ninja",
    "10x",
    "work hard play hard",
    "urgent requirement",
    "immediate start",
    "24/7",
    "on-call rota",
    "on call rota",
    "out of hours",
    "weekend work",
    "startup mindset",
    "hypergrowth",
    "comfortable with ambiguity",
    "move fast",
]

CULTURE_POSITIVE_TERMS = [
    "work-life balance",
    "flexible working",
    "hybrid working",
    "remote-first",
    "remote first",
    "training budget",
    "mentorship",
    "career development",
    "documentation",
    "secure development lifecycle",
    "accessibility",
    "inclusive culture",
    "psychological safety",
    "reasonable adjustments",
    "sustainable pace",
]


# ==========================================
# DATA MODEL
# ==========================================

@dataclass
class ScoreResult:
    fit_score: int
    role_track: str
    salary_band: str
    salary_type: str
    culture_risk: str
    seniority: str
    tags: List[str]
    reasons: List[str]


# ==========================================
# TEXT / MATCHING HELPERS
# ==========================================

TAG_RE = re.compile(r"<[^>]+>")
WHITESPACE_RE = re.compile(r"\s+")
MONEY_RE = re.compile(r"(?:£|gbp\s*)?\s*([0-9][0-9,]*(?:\.\d+)?)\s*([kK])?")


def normalise_text(value: Any) -> str:
    """Return safe, readable plain text from API fields that may contain HTML."""
    if value is None:
        return ""
    text = html.unescape(str(value))
    text = TAG_RE.sub(" ", text)
    return WHITESPACE_RE.sub(" ", text).strip()


def normalise_for_match(value: Any) -> str:
    return normalise_text(value).lower()


def term_in_text(term: str, text: str) -> bool:
    """
    Match phrases safely without accidentally matching tiny terms inside words.
    Example: "soc" should not match "associate".
    """
    term = term.lower().strip()
    if not term:
        return False

    escaped = re.escape(term)
    if " " in term or "/" in term or "." in term or "-" in term:
        pattern = rf"(?<!\w){escaped}(?!\w)"
    else:
        pattern = rf"\b{escaped}\b"
    return re.search(pattern, text, flags=re.IGNORECASE) is not None


def matched_terms(terms: Iterable[str], text: str) -> List[str]:
    return [term for term in terms if term_in_text(term, text)]


def title_is_blacklisted(title: str) -> bool:
    text = normalise_for_match(title)
    return any(term_in_text(term, text) for term in BLACKLIST_TERMS)


def clamp(value: int, low: int = 0, high: int = 100) -> int:
    return max(low, min(high, value))


# ==========================================
# SALARY NORMALISATION
# ==========================================

def parse_salary_values(*values: Any) -> Tuple[Optional[float], Optional[float]]:
    """
    Extract min/max numeric salary values from API numbers or salary strings.

    Handles examples:
      - 60000, 85000
      - "£60,000 - £85,000"
      - "£60k - £85k"
      - "£500 - £650 per day"
    """
    numbers: List[float] = []

    for value in values:
        if value in (None, "", "💰 Unlisted"):
            continue

        if isinstance(value, (int, float)):
            if value > 0:
                numbers.append(float(value))
            continue

        text = normalise_for_match(value)
        for amount, suffix in MONEY_RE.findall(text):
            try:
                number = float(amount.replace(",", ""))
            except ValueError:
                continue

            if suffix.lower() == "k":
                number *= 1000

            if number > 0:
                numbers.append(number)

    if not numbers:
        return None, None

    return min(numbers), max(numbers)


def format_salary(min_salary: Optional[float], max_salary: Optional[float]) -> str:
    if min_salary is None and max_salary is None:
        return "💰 Unlisted"

    if min_salary is not None and max_salary is not None:
        if int(min_salary) == int(max_salary):
            return f"£{int(max_salary):,}"
        return f"£{int(min_salary):,} - £{int(max_salary):,}"

    value = min_salary if min_salary is not None else max_salary
    return f"£{int(value):,}"


def classify_salary(
    min_salary: Optional[float],
    max_salary: Optional[float],
    title: str,
    description: str,
) -> Tuple[str, str, int, List[str]]:
    """
    Returns: salary_band, salary_type, salary_score, reasons.
    Scoring is tuned for a sane £60k-£85k core target, with £85k+ as stretch.
    """
    reasons: List[str] = []

    if min_salary is None and max_salary is None:
        return "Unknown", "unknown", 8, ["Salary unlisted; review only if role/company fit is strong"]

    top = max_salary if max_salary is not None else min_salary
    assert top is not None

    text = f"{title} {description}".lower()
    looks_contract = any(term in text for term in ["contract", "day rate", "outside ir35", "inside ir35", "per day", "daily rate"])

    if top <= 2000 or looks_contract:
        if top >= 700:
            return "High day-rate contract", "contract_day_rate", 14, [f"Strong contract day rate signal: up to £{int(top):,}"]
        if top >= 450:
            return "Viable contract", "contract_day_rate", 11, [f"Possible contract day rate: up to £{int(top):,}"]
        return "Low/unclear contract", "contract_day_rate", 5, [f"Low or unclear day-rate signal: up to £{int(top):,}"]

    if top < 50000:
        return "Below target", "annual", 4, [f"Below target salary ceiling: £{int(top):,}"]

    if 50000 <= top < 60000:
        return "Acceptable floor", "annual", 12, [f"Acceptable floor but below ideal band: up to £{int(top):,}"]

    if 60000 <= top <= 85000:
        return "Core target", "annual", 20, [f"Core target salary band: up to £{int(top):,}"]

    if 85000 < top <= 120000:
        return "Stretch target", "annual", 19, [f"High-value stretch salary band: up to £{int(top):,}"]

    return "High-value stretch", "annual", 18, [f"Very high-value salary band: up to £{int(top):,}"]


# ==========================================
# ROLE SCORING
# ==========================================

def infer_seniority(title: str, description: str) -> str:
    text = normalise_for_match(f"{title} {description}")

    if any(term_in_text(term, text) for term in ["principal", "staff engineer", "lead engineer"]):
        return "Lead/Staff"
    if term_in_text("senior", text):
        return "Senior"
    if any(term_in_text(term, text) for term in ["mid-level", "mid level", "midweight"]):
        return "Mid-level"
    return "Unspecified/Mid"


def infer_role_track(title: str, description: str) -> str:
    text = normalise_for_match(f"{title} {description}")

    track_terms = [
        ("Product Security", ["product security", "security software engineer"]),
        ("Application Security", ["application security", "appsec", "owasp"]),
        ("Secure Full-Stack", ["full stack", "full-stack", "react", "typescript"]),
        ("Platform / DevEx", ["platform engineer", "developer tools", "developer experience", "internal tools", "devex"]),
        ("Cloud Security", ["cloud security", "aws security", "azure security", "gcp security"]),
        ("DevSecOps", ["devsecops", "ci/cd", "terraform", "kubernetes"]),
        ("AI Security / Evaluation", ["ai security", "ai evaluation", "llm", "prompt injection", "trust and safety"]),
        ("SRE / Reliability", ["site reliability", "sre", "observability", "incident response"]),
    ]

    for track, terms in track_terms:
        if any(term_in_text(term, text) for term in terms):
            return track

    return "General Software Engineering"


def classify_culture(title: str, description: str) -> Tuple[str, int, List[str]]:
    text = normalise_for_match(f"{title} {description}")
    risk_matches = matched_terms(CULTURE_RISK_TERMS, text)
    positive_matches = matched_terms(CULTURE_POSITIVE_TERMS, text)

    score = 10
    reasons: List[str] = []

    if positive_matches:
        score += min(5, len(positive_matches) * 2)
        reasons.append(f"Positive culture signals: {', '.join(positive_matches[:3])}")

    if risk_matches:
        score -= min(10, len(risk_matches) * 3)
        reasons.append(f"Culture-risk terms found: {', '.join(risk_matches[:3])}")

    if any(term in text for term in ["on-call", "on call", "out of hours", "24/7"]):
        risk = "Check on-call"
    elif score >= 13:
        risk = "Low culture risk"
    elif score >= 8:
        risk = "Medium / verify"
    elif score >= 5:
        risk = "Possible chaos"
    else:
        risk = "High pressure risk"

    return risk, clamp(score, 0, 15), reasons


def company_score(company: str) -> Tuple[int, List[str], List[str]]:
    text = normalise_for_match(company)
    reasons: List[str] = []
    tags: List[str] = []

    if not text or text == "unknown company":
        return 3, ["Unknown company"], ["unknown-company"]

    for target in TARGET_COMPANIES:
        if target in text:
            return 10, [f"Target company match: {company}"], ["target-company"]

    if any(term in text for term in ["recruitment", "recruiter", "consulting", "consultancy"]):
        return 4, ["Recruiter/consultancy-posted role; verify end client and working pattern"], ["verify-company"]

    return 6, ["Company is not on target list; review if role fit is strong"], ["standard-company"]


def score_job(job: Dict[str, Any]) -> Dict[str, Any]:
    """
    Enrich a normalised job dict with fit_score, salary band, culture risk,
    role track, seniority, tags and scoring reasons.
    """
    title = normalise_text(job.get("title"))
    company = normalise_text(job.get("company")) or "Unknown Company"
    description = normalise_text(job.get("description"))
    combined = normalise_for_match(f"{title} {company} {description}")

    core_matches = matched_terms(CORE_ROLE_KEYWORDS, combined)
    adjacent_matches = matched_terms(ADJACENT_ROLE_KEYWORDS, combined)
    skill_matches = matched_terms(PROFILE_SKILLS, combined)

    role_score = 0
    reasons: List[str] = []
    tags: List[str] = []

    if core_matches:
        role_score = min(30, 18 + len(core_matches) * 4)
        reasons.append(f"Core role match: {', '.join(core_matches[:3])}")
        tags.append("core-role")
    elif adjacent_matches:
        role_score = min(24, 12 + len(adjacent_matches) * 3)
        reasons.append(f"Adjacent role match: {', '.join(adjacent_matches[:3])}")
        tags.append("adjacent-role")
    else:
        role_score = 6
        reasons.append("Weak title/description match against target role taxonomy")
        tags.append("weak-role-match")

    skill_score = min(25, len(skill_matches) * 3)
    if skill_matches:
        reasons.append(f"Skill overlap: {', '.join(skill_matches[:6])}")
        tags.extend([f"skill:{skill}" for skill in skill_matches[:6]])
    else:
        reasons.append("No strong skill overlap found in advert text")

    min_salary, max_salary = parse_salary_values(
        job.get("salary_min"),
        job.get("salary_max"),
        job.get("salary"),
    )
    salary_band, salary_type, salary_score, salary_reasons = classify_salary(min_salary, max_salary, title, description)
    reasons.extend(salary_reasons)

    culture_risk, culture_score, culture_reasons = classify_culture(title, description)
    reasons.extend(culture_reasons)

    c_score, company_reasons, company_tags = company_score(company)
    reasons.extend(company_reasons)
    tags.extend(company_tags)

    seniority = infer_seniority(title, description)
    role_track = infer_role_track(title, description)

    # Small adjustment: senior/staff roles are not ignored, but they are marked as stretch.
    if seniority in {"Lead/Staff"}:
        reasons.append("Lead/staff seniority signal; treat as stretch unless responsibilities are narrow")
        tags.append("stretch-seniority")

    total = role_score + skill_score + salary_score + culture_score + c_score

    result = ScoreResult(
        fit_score=clamp(total),
        role_track=role_track,
        salary_band=salary_band,
        salary_type=salary_type,
        culture_risk=culture_risk,
        seniority=seniority,
        tags=sorted(set(tags)),
        reasons=reasons[:10],
    )

    enriched = dict(job)
    enriched.update(
        {
            "title": title,
            "company": company,
            "description": description,
            "salary": job.get("salary") or format_salary(min_salary, max_salary),
            "salary_min": min_salary,
            "salary_max": max_salary,
            "fit_score": result.fit_score,
            "role_track": result.role_track,
            "salary_band": result.salary_band,
            "salary_type": result.salary_type,
            "culture_risk": result.culture_risk,
            "seniority": result.seniority,
            "tags": result.tags,
            "score_reasons": result.reasons,
        }
    )
    return enriched


def should_keep_job(job: Dict[str, Any]) -> bool:
    title = normalise_text(job.get("title"))
    if not title:
        return False
    if title_is_blacklisted(title):
        return False
    return True


# ==========================================
# CLOUDFLARE D1
# ==========================================

def d1_enabled() -> bool:
    return all([CF_ACCOUNT_ID, CF_DATABASE_ID, CF_API_TOKEN])


def run_d1_query(sql_query: str, params: Optional[Sequence[Any]] = None) -> Dict[str, Any]:
    """
    Execute a parameterised SQL query against Cloudflare D1 via the REST API.

    Important:
    - D1 write failures should fail the GitHub Action.
    - Previously this function only printed errors and returned {}, which made
      broken database writes look like successful pipeline runs.
    """
    if not d1_enabled():
        print("⚠️ D1 credentials not configured; skipping database call.")
        return {}

    url = f"https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT_ID}/d1/database/{CF_DATABASE_ID}/query"
    headers = {
        "Authorization": f"Bearer {CF_API_TOKEN}",
        "Content-Type": "application/json",
    }
    payload: Dict[str, Any] = {"sql": sql_query}
    if params is not None:
        payload["params"] = list(params)

    try:
        response = requests.post(url, json=payload, headers=headers, timeout=REQUEST_TIMEOUT)
    except requests.RequestException as exc:
        raise RuntimeError(f"Network error calling Cloudflare D1: {exc}") from exc

    if response.status_code != 200:
        raise RuntimeError(
            f"Cloudflare D1 API error ({response.status_code}): {response.text[:1000]}"
        )

    try:
        data = response.json()
    except ValueError as exc:
        raise RuntimeError(f"Could not decode Cloudflare D1 response as JSON: {exc}") from exc

    if not data.get("success", False):
        raise RuntimeError(
            f"Cloudflare D1 query unsuccessful: {json.dumps(data.get('errors', []))[:1000]}"
        )

    return data


def first_d1_results(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    try:
        result = data.get("result", [])
        if not result:
            return []
        rows = result[0].get("results", [])
        return rows if isinstance(rows, list) else []
    except (AttributeError, IndexError, TypeError):
        return []


def init_db() -> None:
    """Create the jobs table and apply additive schema migrations."""
    print("⚙️ Initialising Cloudflare D1 schema...")

    run_d1_query(
        """
        CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            company TEXT,
            salary TEXT,
            link TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    required_columns = {
        "source": "source TEXT",
        "location": "location TEXT",
        "description": "description TEXT",
        "salary_min": "salary_min REAL",
        "salary_max": "salary_max REAL",
        "salary_type": "salary_type TEXT",
        "salary_band": "salary_band TEXT",
        "fit_score": "fit_score INTEGER DEFAULT 0",
        "role_track": "role_track TEXT",
        "culture_risk": "culture_risk TEXT",
        "seniority": "seniority TEXT",
        "tags_json": "tags_json TEXT",
        "score_reasons_json": "score_reasons_json TEXT",
        "raw_json": "raw_json TEXT",
        # Do NOT use DEFAULT CURRENT_TIMESTAMP here.
        # SQLite/D1 can reject ALTER TABLE ADD COLUMN when the default is non-constant.
        # We set CURRENT_TIMESTAMP explicitly in INSERT/UPDATE statements instead.
        "last_seen_at": "last_seen_at TEXT",
    }

    existing_rows = first_d1_results(run_d1_query("PRAGMA table_info(jobs);"))
    existing_columns = {row.get("name") for row in existing_rows if isinstance(row, dict)}

    for column, ddl in required_columns.items():
        if column not in existing_columns:
            print(f"  ➕ Adding column: {column}")
            run_d1_query(f"ALTER TABLE jobs ADD COLUMN {ddl};")

    # Backfill last_seen_at for legacy rows.
    run_d1_query(
        """
        UPDATE jobs
        SET last_seen_at = COALESCE(last_seen_at, timestamp, CURRENT_TIMESTAMP)
        WHERE last_seen_at IS NULL;
        """
    )

    # Indexes for the Worker/API to use later.
    run_d1_query("CREATE INDEX IF NOT EXISTS idx_jobs_timestamp ON jobs(timestamp DESC);")
    run_d1_query("CREATE INDEX IF NOT EXISTS idx_jobs_fit_score ON jobs(fit_score DESC);")
    run_d1_query("CREATE INDEX IF NOT EXISTS idx_jobs_role_track ON jobs(role_track);")
    run_d1_query("CREATE INDEX IF NOT EXISTS idx_jobs_salary_band ON jobs(salary_band);")
    run_d1_query("CREATE INDEX IF NOT EXISTS idx_jobs_last_seen_at ON jobs(last_seen_at DESC);")


def clean_old_jobs() -> None:
    print(f"🧹 Pruning jobs older than {RETENTION_DAYS} days...")
    run_d1_query(f"DELETE FROM jobs WHERE timestamp < datetime('now', '-{RETENTION_DAYS} days');")


def is_new_job(job_id: str) -> bool:
    data = run_d1_query("SELECT id FROM jobs WHERE id = ? LIMIT 1;", [job_id])
    return len(first_d1_results(data)) == 0


def mark_job_seen(job_id: str) -> None:
    run_d1_query("UPDATE jobs SET last_seen_at = CURRENT_TIMESTAMP WHERE id = ?;", [job_id])


def save_job_to_db(job: Dict[str, Any]) -> None:
    """
    Insert or update a job in D1.

    This intentionally uses UPSERT rather than INSERT OR IGNORE so that:
    - existing legacy rows receive fit_score, role_track, salary_band and culture_risk;
    - existing rows get refreshed when the scoring model improves;
    - last_seen_at is updated whenever the scraper sees the job again.
    """
    if not d1_enabled():
        return

    sql = """
        INSERT INTO jobs (
            id,
            title,
            company,
            salary,
            link,
            source,
            location,
            description,
            salary_min,
            salary_max,
            salary_type,
            salary_band,
            fit_score,
            role_track,
            culture_risk,
            seniority,
            tags_json,
            score_reasons_json,
            raw_json,
            last_seen_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(id) DO UPDATE SET
            title = excluded.title,
            company = excluded.company,
            salary = excluded.salary,
            link = excluded.link,
            source = excluded.source,
            location = excluded.location,
            description = excluded.description,
            salary_min = excluded.salary_min,
            salary_max = excluded.salary_max,
            salary_type = excluded.salary_type,
            salary_band = excluded.salary_band,
            fit_score = excluded.fit_score,
            role_track = excluded.role_track,
            culture_risk = excluded.culture_risk,
            seniority = excluded.seniority,
            tags_json = excluded.tags_json,
            score_reasons_json = excluded.score_reasons_json,
            raw_json = excluded.raw_json,
            last_seen_at = CURRENT_TIMESTAMP;
    """

    params = [
        job.get("id"),
        job.get("title"),
        job.get("company"),
        job.get("salary"),
        job.get("link"),
        job.get("source"),
        job.get("location"),
        job.get("description"),
        job.get("salary_min"),
        job.get("salary_max"),
        job.get("salary_type"),
        job.get("salary_band"),
        job.get("fit_score"),
        job.get("role_track"),
        job.get("culture_risk"),
        job.get("seniority"),
        json.dumps(job.get("tags", []), ensure_ascii=False),
        json.dumps(job.get("score_reasons", []), ensure_ascii=False),
        json.dumps(job.get("raw", {}), ensure_ascii=False)[:10000],
    ]

    run_d1_query(sql, params)


# ==========================================
# API CLIENTS
# ==========================================

def request_json(
    method: str,
    url: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    auth: Optional[Tuple[str, str]] = None,
    retries: int = 2,
) -> Dict[str, Any]:
    for attempt in range(retries + 1):
        try:
            response = requests.request(
                method,
                url,
                params=params,
                headers=headers,
                auth=auth,
                timeout=REQUEST_TIMEOUT,
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as exc:
            if attempt >= retries:
                print(f"⚠️ Request failed after {retries + 1} attempts: {url} :: {exc}")
                return {}
            sleep_for = 1 + attempt
            print(f"  ↻ Request failed; retrying in {sleep_for}s: {exc}")
            time.sleep(sleep_for)
        except ValueError as exc:
            print(f"⚠️ Response was not valid JSON: {url} :: {exc}")
            return {}

    return {}


def fetch_adzuna_london() -> List[Dict[str, Any]]:
    print("🇬🇧 Fetching London roles from Adzuna...")

    if not all([ADZUNA_APP_ID, ADZUNA_APP_KEY]):
        print("⚠️ Skipping Adzuna: ADZUNA_APP_ID or ADZUNA_APP_KEY is missing.")
        return []

    url = "https://api.adzuna.com/v1/api/jobs/gb/search/1"
    unique_jobs: Dict[str, Dict[str, Any]] = {}
    headers = {"Accept": "application/json"}

    for keyword in ADZUNA_KEYWORDS:
        print(f"  🔍 Adzuna keyword: {keyword}")
        params = {
            "app_id": ADZUNA_APP_ID,
            "app_key": ADZUNA_APP_KEY,
            "results_per_page": MAX_ADZUNA_RESULTS_PER_KEYWORD,
            "what": keyword,
            "where": LONDON_LOCATION,
            "sort_by": "date",
        }

        payload = request_json("GET", url, params=params, headers=headers)
        for item in payload.get("results", []):
            raw_id = str(item.get("id") or "").strip()
            if not raw_id:
                continue

            job_id = f"adzuna_{raw_id}"
            if job_id in unique_jobs:
                continue

            title = normalise_text(item.get("title"))
            if title_is_blacklisted(title):
                continue

            company_name = normalise_text(item.get("company", {}).get("display_name")) or "Unknown Company"
            description = normalise_text(item.get("description"))

            salary_min, salary_max = parse_salary_values(item.get("salary_min"), item.get("salary_max"))
            salary = format_salary(salary_min, salary_max)

            unique_jobs[job_id] = {
                "id": job_id,
                "title": title,
                "company": company_name,
                "salary": salary,
                "salary_min": salary_min,
                "salary_max": salary_max,
                "link": item.get("redirect_url"),
                "source": "Adzuna",
                "location": normalise_text(item.get("location", {}).get("display_name")) or "London",
                "description": description,
                "raw": item,
            }

    found = list(unique_jobs.values())
    print(f"✅ Adzuna found {len(found)} relevant raw roles.")
    return found


def fetch_reed_london() -> List[Dict[str, Any]]:
    print("🇬🇧 Fetching London roles from Reed...")

    if not REED_API_KEY or REED_API_KEY == "YOUR_REED_API_KEY_HERE":
        print("⚠️ Skipping Reed: REED_API_KEY is missing.")
        return []

    url = "https://www.reed.co.uk/api/1.0/search"
    params = {
        "keywords": REED_KEYWORDS,
        "locationName": LONDON_LOCATION,
        "distanceFromLocation": 10,
        "resultsToTake": MAX_REED_RESULTS,
    }

    payload = request_json("GET", url, params=params, auth=(REED_API_KEY, ""))
    found_jobs: List[Dict[str, Any]] = []

    seen_ids: set[str] = set()
    for item in payload.get("results", []):
        raw_id = str(item.get("jobId") or "").strip()
        if not raw_id:
            continue

        job_id = f"reed_{raw_id}"
        if job_id in seen_ids:
            continue
        seen_ids.add(job_id)

        title = normalise_text(item.get("jobTitle"))
        if title_is_blacklisted(title):
            continue

        min_salary, max_salary = parse_salary_values(item.get("minimumSalary"), item.get("maximumSalary"))
        salary = format_salary(min_salary, max_salary)

        found_jobs.append(
            {
                "id": job_id,
                "title": title,
                "company": normalise_text(item.get("employerName")) or "Unknown Company",
                "salary": salary,
                "salary_min": min_salary,
                "salary_max": max_salary,
                "link": item.get("jobUrl"),
                "source": "Reed",
                "location": normalise_text(item.get("locationName")) or "London",
                "description": normalise_text(item.get("jobDescription")),
                "raw": item,
            }
        )

    print(f"✅ Reed found {len(found_jobs)} relevant raw roles.")
    return found_jobs


# ==========================================
# DEDUPLICATION AND ENRICHMENT
# ==========================================

def job_fingerprint(job: Dict[str, Any]) -> str:
    title = re.sub(r"\W+", "", normalise_for_match(job.get("title")))
    company = re.sub(r"\W+", "", normalise_for_match(job.get("company")))
    return f"{company}:{title}"


def dedupe_jobs(jobs: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen_ids: set[str] = set()
    seen_fingerprints: set[str] = set()
    deduped: List[Dict[str, Any]] = []

    for job in jobs:
        job_id = str(job.get("id") or "")
        fingerprint = job_fingerprint(job)

        if not job_id or job_id in seen_ids or fingerprint in seen_fingerprints:
            continue

        seen_ids.add(job_id)
        seen_fingerprints.add(fingerprint)
        deduped.append(job)

    return deduped


def enrich_and_filter_jobs(jobs: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    enriched: List[Dict[str, Any]] = []

    for job in jobs:
        if not should_keep_job(job):
            continue

        scored = score_job(job)
        if scored["fit_score"] < MINIMUM_SAVE_SCORE:
            continue

        enriched.append(scored)

    return sorted(enriched, key=lambda j: int(j.get("fit_score", 0)), reverse=True)


# ==========================================
# DISCORD NOTIFICATION ENGINE
# ==========================================

def truncate(value: Any, limit: int = 240) -> str:
    text = normalise_text(value)
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "…"


def send_to_discord(jobs: List[Dict[str, Any]]) -> None:
    if not jobs:
        return

    if not DISCORD_WEBHOOK_URL:
        print("⚠️ Skipping Discord alert: DISCORD_WEBHOOK_URL is not set.")
        return

    alert_jobs = [job for job in jobs if int(job.get("fit_score", 0)) >= MINIMUM_ALERT_SCORE][:10]
    if not alert_jobs:
        print(f"ℹ️ No new jobs met the alert threshold of {MINIMUM_ALERT_SCORE}.")
        return

    print(f"🚀 Sending {len(alert_jobs)} high-fit new roles to Discord...")

    embeds = []
    for job in alert_jobs:
        reasons = job.get("score_reasons", [])
        reason_text = "\n".join(f"• {truncate(reason, 120)}" for reason in reasons[:3])

        embeds.append(
            {
                "title": f"{job.get('fit_score', 0)}/100 · [{job.get('source')}] {truncate(job.get('title'), 180)}",
                "url": job.get("link"),
                "description": (
                    f"**Company:** {truncate(job.get('company'), 80)}\n"
                    f"**Salary:** {job.get('salary')} · {job.get('salary_band')}\n"
                    f"**Track:** {job.get('role_track')} · **Culture:** {job.get('culture_risk')}\n"
                    f"**Seniority:** {job.get('seniority')}\n\n"
                    f"{reason_text}"
                ),
                "color": 15158332,
            }
        )

    payload = {
        "content": "🇬🇧 **Secure Engineering Role Radar — Top New London Roles**",
        "embeds": embeds,
    }

    try:
        response = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        print("✅ Discord digest sent.")
    except requests.RequestException as exc:
        print(f"❌ Failed to send Discord digest: {exc}")


# ==========================================
# MAIN PIPELINE
# ==========================================

def run_pipeline() -> None:
    print("--- Starting Secure Engineering Role Radar pipeline ---")

    if d1_enabled():
        init_db()
        clean_old_jobs()
    else:
        print("⚠️ D1 credentials missing. Running fetch/scoring only; no DB writes.")

    raw_jobs = fetch_adzuna_london() + fetch_reed_london()
    print(f"📥 Pulled {len(raw_jobs)} raw jobs from all sources.")

    deduped = dedupe_jobs(raw_jobs)
    print(f"🧹 Deduped to {len(deduped)} unique roles.")

    final_jobs = enrich_and_filter_jobs(deduped)
    print(f"🎯 {len(final_jobs)} roles passed the minimum fit score of {MINIMUM_SAVE_SCORE}.")

    new_jobs_only: List[Dict[str, Any]] = []
    saved_or_updated_count = 0

    for job in final_jobs:
        if not d1_enabled():
            new_jobs_only.append(job)
            continue

        is_new = is_new_job(job["id"])

        # Always save. save_job_to_db() now performs an upsert, so old rows are
        # enriched/backfilled and new rows are inserted.
        save_job_to_db(job)
        saved_or_updated_count += 1

        if is_new:
            new_jobs_only.append(job)

    print(f"💾 {len(new_jobs_only)} brand new roles identified.")
    print(f"🔁 {saved_or_updated_count} scored roles inserted or refreshed in D1.")

    if new_jobs_only:
        send_to_discord(new_jobs_only)
    else:
        print("☕ No new roles found today.")

    if final_jobs:
        print("\n🏆 Top roles this run:")
        for job in final_jobs[:10]:
            print(
                f"  - {job['fit_score']:>3}/100 | {job['role_track']:<24} | "
                f"{truncate(job['title'], 50):<52} | {truncate(job['company'], 28):<30} | {job['salary']}"
            )

    print("--- Pipeline finished ---")


if __name__ == "__main__":
    run_pipeline()
