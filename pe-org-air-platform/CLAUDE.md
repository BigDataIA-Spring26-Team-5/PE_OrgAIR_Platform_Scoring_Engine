# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

PE-OrgAIR Platform is a FastAPI-based scoring system that evaluates companies' AI readiness and organizational capabilities. It collects evidence from multiple sources (SEC filings, job postings, patents, digital presence), maps them to 7 assessment dimensions, and calculates composite V^R (Value at Risk) scores.

**Technology Stack:** Python 3.11+, FastAPI, Pydantic, Snowflake, Redis, AWS S3, Docker

## Common Commands

### Development
```bash
# Activate virtual environment
cd pe-org-air-platform
source .venv/bin/activate  # Unix
.venv\Scripts\activate     # Windows

# Install dependencies (poetry-based project)
pip install -r requirements.txt
# Or with poetry:
poetry install

# Run FastAPI server (development mode with auto-reload)
python -m app.main
# Or via uvicorn directly:
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

# Run Streamlit UI
streamlit run streamlit/app.py
```

### Testing
```bash
# Run all tests
pytest

# Run specific test file
pytest tests/test_evidence_mapper.py

# Run with coverage
pytest --cov=app --cov-report=html

# Run tests matching pattern
pytest -k "test_rubric_scorer"
```

### Docker
```bash
# Build and run with docker-compose
cd docker
docker-compose up --build

# Run in detached mode
docker-compose up -d

# View logs
docker-compose logs -f api

# Stop services
docker-compose down
```

### Linting and Formatting
```bash
# Format code (uses black)
black app/ tests/

# Lint code (uses ruff)
ruff check app/ tests/

# Type checking (uses mypy)
mypy app/
```

## Architecture Overview

### Multi-Stage Evidence Pipeline

The system operates in distinct stages that transform raw data into actionable scores:

**Stage 1 - Collection (Pipelines)**
- `sec_edgar.py` - Downloads 10-K filings from SEC EDGAR
- `job_signals.py` - Scrapes job postings (LinkedIn, Indeed, Glassdoor) via python-jobspy
- `patent_signals.py` - Fetches patent data from PatentsView API
- `tech_signals.py` - Detects technology stack via BuiltWith/Wappalyzer
- `glassdoor_collector.py` - Collects company reviews (CS3 addition)
- `leadership_analyzer.py` - Analyzes board composition from DEF-14A (CS3 addition)

**Stage 2 - Processing**
- `document_parser.py` + `pdf_parser.py` - Extract text/tables from PDFs
- `section_analyzer.py` - Identifies and extracts SEC filing sections (Item 1, 1A, 7)
- `chunking.py` - Splits documents into semantic chunks for analysis

**Stage 3 - Scoring (app/scoring/)**
- `evidence_mapper.py` - Maps 4 external signals + 3 SEC sections → 7 dimensions using weighted mapping table
- `rubric_scorer.py` - Scores SEC sections against AI readiness rubrics
- `talent_concentration.py` - Calculates TC from Glassdoor reviews + job postings (skill concentration, individual mentions)
- `vr_calculator.py` - Combines weighted dimension scores × talent risk adjustment → final V^R score
- `hr_calculator.py` - Calculates human resources risk factors
- `position_factor.py` - Calculates competitive position adjustments
- `integration_service.py` - Orchestrates cross-module scoring integration

**Stage 4 - Orchestration (app/services/)**
- `scoring_service.py` - Runs full evidence collection + dimension scoring for a company
- `vr_scoring_service.py` - Orchestrates TC + V^R computation from S3 data (runs after scoring_service)

### Data Flow

```
SEC EDGAR → PDF Parser → Section Analyzer → Rubric Scorer ─┐
Job Boards → JobSpy → Job Signals ────────────────────────┤
PatentsView → Patent Signals ─────────────────────────────┤──► Evidence Mapper ──► 7 Dimension Scores ─┐
BuiltWith → Tech Signals ─────────────────────────────────┤                                           │
DEF-14A → Board Analyzer → Governance Signal ─────────────┘                                           │
                                                                                                      ├──► V^R Calculator ──► Final V^R Score
Glassdoor → Review Collector ──► Talent Concentration (TC) ───────────────────────────────────────────┘
Job Postings (S3) ─────────────► Skill Concentration ──────┘
```

### CS3 Portfolio Companies

The 5 CS3 target companies: **NVDA**, **JPM**, **WMT**, **GE**, **DG**

TC + V^R can be computed individually or as a portfolio via the `/api/v1/scoring/tc-vr/` endpoints.

### The 7 V^R Dimensions

All scoring ultimately contributes to these 7 dimensions (from `app/models/enumerations.py`):
1. **Data Infrastructure** - Data pipelines, warehouses, governance
2. **AI Governance** - Ethics, compliance, responsible AI practices
3. **Technology Stack** - Cloud platforms, AI/ML frameworks, technical maturity
4. **Talent & Skills** - AI/ML hiring, workforce capabilities
5. **Leadership Vision** - Executive AI commitment, strategic direction
6. **Use Case Portfolio** - Breadth and depth of AI applications
7. **Culture & Change** - Innovation culture, adaptability

### Evidence-to-Dimension Mapping

The `evidence_mapper.py` implements a weighted mapping matrix (Table 1, CS3 p.7):

- **technology_hiring** (job postings) → Talent (0.70), Tech Stack (0.20), Data Infrastructure (0.10)
- **innovation_activity** (patents) → Tech Stack (0.50), Use Cases (0.30), Data Infrastructure (0.20)
- **digital_presence** (tech stack) → Data Infrastructure (0.60), Tech Stack (0.40)
- **leadership_signals** → Leadership (0.60), AI Governance (0.40)
- **sec_item_1** (Business section) → Maps to all dimensions with specific weights
- **sec_item_1a** (Risk Factors) → AI Governance (0.80), Data Infrastructure (0.20)
- **sec_item_7** (MD&A) → Use Cases (0.50), Leadership (0.30), Governance (0.20)

## Key Patterns and Conventions

### Company Name Mapping

Companies have multiple name variations for different data sources. Use `config.py` helpers:

```python
from app.config import (
    get_company_search_name,      # For job boards: "John Deere"
    get_patent_search_names,       # For patents: ["Deere & Company"]
    get_company_aliases,           # For fuzzy matching
    COMPANY_NAME_MAPPINGS          # Full mapping dict
)

# Example: Deere & Company
ticker = "DE"
job_name = get_company_search_name(ticker)  # "John Deere"
patent_names = get_patent_search_names(ticker)  # ["Deere & Company"]
```

**Important:** Job boards list "John Deere", but patents are filed as "Deere & Company". Always use the mapping functions.

### Configuration Management

Settings are managed via Pydantic with `.env` file loading (`app/config.py`):

```python
from app.config import settings, get_settings

# Access config values
db_url = settings.SNOWFLAKE_ACCOUNT
api_key = settings.OPENAI_API_KEY.get_secret_value()  # SecretStr

# Scoring parameters (v2.0)
alpha = settings.ALPHA_VR_WEIGHT  # 0.60
dimension_weights = settings.dimension_weights  # List of 7 weights
```

Dimension weights MUST sum to 1.0 (validated by `@model_validator`).

### Snowflake Schema

The database follows this schema:
- `industries` - Industry classifications
- `companies` - Company master data (ticker, name, industry_id)
- `documents` - SEC filings metadata
- `signals` - External signal scores (jobs, patents, tech)
- `assessments` - V^R assessment runs
- `dimension_scores` - Individual dimension scores per assessment
- `evidence` - Supporting evidence items

### Redis Caching

Redis is used for caching API responses and expensive computations:

```python
from app.services.redis_cache import RedisCache

cache = RedisCache()
await cache.set_cache(key, value, ttl=3600)
result = await cache.get_cache(key)
```

Default TTLs: sectors=24h, scores=1h

### S3 Storage

Documents are stored in S3 with this structure:
```
s3://bucket/
  raw/
    {ticker}/
      10-K/
        {accession_number}.pdf
  parsed/
    {ticker}/
      {file_hash}.json
  chunks/
    {ticker}/
      {file_hash}_chunks.json
```

## Domain-Specific Knowledge

### Scoring Formulas

**Job Signal Score** (CS2 p.10-11):
```python
ai_score = min(ai_count * 15, 50)  # AI job volume (max 50)
ratio_score = (ai_count / total_tech) * 50  # AI ratio (max 50)
diversity_bonus = min(keyword_diversity * 2, 20)  # Keyword diversity (max 20)
volume_bonus = min((total_tech / 10) * 3, 30)  # Total volume (max 30)
final_score = min(ai_score + ratio_score + diversity_bonus + volume_bonus, 100)
```

**Patent Signal Score** (CS2 p.18-19):
```python
volume = min(ai_patents * 5, 50)
recency = min(recent_ai_patents * 2, 20)
diversity = min(category_count * 10, 30)
score = volume + recency + diversity  # Max 100
```

**Tech Stack Score** (CS2 p.21-23):
- Primary signal: Total live technologies detected (BuiltWith count)
- Diversity: Number of technology categories
- Active maintenance: live / (live + dead) ratio
- AI tools bonus: TensorFlow, Kubernetes, etc. detected

**Talent Concentration (TC)** (CS3 Task 5.0e):
- Calculated from Glassdoor reviews (individual executive mentions) + job postings (skill concentration)
- `_EXPANDED_AI_SKILLS` list (~50 skills) with `_SKILL_DENOMINATOR = 25` (25+ unique skills = zero concentration)
- Short terms like "go", "r", "c++" use whole-word matching to avoid false positives
- TC feeds into V^R as a talent risk adjustment multiplier

**V^R Score** (CS3 Task 5.2):
- `V^R = weighted_dimension_score × talent_risk_adjustment × position_factor`
- Expected ranges per company defined in `EXPECTED_RANGES` dict in `tc_vr_scoring.py`

### AI Keywords

Keywords are centralized in `app/pipelines/keywords.py`:
- `AI_KEYWORDS` - Core AI/ML terms (neural network, deep learning, etc.)
- `AI_TECHSTACK_KEYWORDS` - Frameworks (TensorFlow, PyTorch, etc.)
- `TOP_AI_TOOLS` - Specific tools (Kubernetes, Airflow, etc.)

Used for job posting classification and tech stack detection.

### Graceful Shutdown

The API implements signal handlers for Ctrl+C / SIGTERM:
- Windows fallback using signal.signal (loop.add_signal_handler not supported)
- `shutdown.py` provides `is_shutting_down()` flag for long-running pipelines
- Check this flag in loops to stop gracefully

### SEC EDGAR Requirements

- **User-Agent REQUIRED:** Set `SEC_USER_AGENT` with company name + email
- **Rate limit:** 10 requests/second max (enforced by SEC)
- Accession numbers format: `0001193125-23-123456`
- Item sections: 1 (Business), 1A (Risk), 7 (MD&A), 7A (Market Risk)

### Testing with Seed Data

Test fixtures use consistent UUIDs from seed data:
- Industries: `550e8400-e29b-41d4-a716-446655440001` to `446655440005`
- Companies: `a1000000-...`, `a2000000-...`, etc.
- Assessments: `b1000000-...`, `b2000000-...`, etc.

See `tests/conftest.py` for fixtures.

### Layered Architecture

The codebase follows a **Router → Service → Repository** pattern:
- **Routers** (`app/routers/`) - FastAPI endpoints, request/response models
- **Services** (`app/services/`) - Business logic orchestration
- **Repositories** (`app/repositories/`) - Snowflake data access (all extend `base.py`)
- **Pipelines** (`app/pipelines/`) - Data collection and processing
- **Scoring** (`app/scoring/`) - Score calculation algorithms

## API Endpoints

Key routes (see Swagger UI at http://localhost:8000/docs):

- `POST /api/scoring/run` - Run full scoring pipeline for a company
- `POST /api/v1/scoring/tc-vr/{ticker}` - Compute TC + V^R for one company
- `POST /api/v1/scoring/tc-vr/portfolio` - Compute TC + V^R for all 5 CS3 companies
- `GET /api/v1/scoring/tc-vr/{ticker}` - View last computed TC + V^R (from Snowflake)
- `POST /api/v1/board-governance/analyze/{ticker}` - Analyze board composition from DEF-14A
- `GET /api/companies/{ticker}` - Get company details
- `POST /api/signals/jobs` - Trigger job signal collection
- `POST /api/signals/patents` - Trigger patent signal collection
- `GET /api/assessments/{id}` - Get assessment results
- `POST /api/documents/upload` - Upload SEC filing for processing

## Environment Variables

Required variables (see `.env.example`):
- `SNOWFLAKE_*` - Database credentials (account, user, password, role, warehouse)
- `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` - S3 access
- `REDIS_URL` - Redis connection (default: redis://localhost:6379/0)
- `SEC_USER_AGENT` - SEC EDGAR User-Agent (REQUIRED: "CompanyName email@domain.com")
- `BUILTWITH_API_KEY` - BuiltWith API key (optional, for tech stack detection)
- `SECRET_KEY` - App secret (≥32 chars in production)

Optional LLM keys (for future rubric scoring enhancements):
- `OPENAI_API_KEY` - OpenAI API access
- `ANTHROPIC_API_KEY` - Anthropic Claude access

## Important Notes

- **Never commit `.env` file** - Contains secrets
- **PatentsView quirk:** Each assignee spelling is a separate entity (e.g., "Walmart Inc." ≠ "Walmart Apollo, LLC")
- **Job board fuzzy matching:** Use `rapidfuzz.fuzz.ratio` with 75+ threshold for company name matching
- **Dimension weights validation:** Changes to dimension weights in config.py MUST sum to 1.0
- **Windows compatibility:** Signal handlers have a fallback for Windows (see `app/main.py`)
- **Case Study versions:** CS2 = external signals (4 signals + SEC), CS3 = CS2 + Glassdoor reviews + board governance + TC + V^R scoring
- **CS3 portfolio:** NVDA, JPM, WMT, GE, DG (defined in `tc_vr_scoring.py` as `CS3_PORTFOLIO`)
