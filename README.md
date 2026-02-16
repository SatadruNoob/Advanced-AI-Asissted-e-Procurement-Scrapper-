Here is a professionally structured, GitHub-ready `README.md` file based on your project specifications. I have optimized the formatting for readability, added syntax highlighting, and included a clean directory tree.

---

# README.md

# 🏭 Multi-Portal Tender Scraper (Production Version)

An enterprise-grade, fault-tolerant, multi-process tender scraping system. Designed for high-volume extraction from government e-procurement portals with integrated AI filtering, UPSERT database logic, and automated reporting.

## 🚀 Overview

This system automates the end-to-end lifecycle of tender data collection:

* **Multi-Portal Support:** Simultaneous scraping of diverse government portals.
* **AI-Driven Intelligence:** Uses Mistral LLM to filter and classify tender relevance.
* **Robust Persistence:** SQLite backend with UPSERT logic to prevent duplicates.
* **Resilience:** Built-in resume capability, session refreshing, and isolated error handling.
* **Reporting:** Automatic generation of Excel mirrors and consolidated performance analytics.

---

## 🏗 Architecture Overview

The system operates via three core modules:

1. **`production_orchestrator.py`**: The "Brain." Manages multiprocessing, process isolation, and staggered starts.
2. **`production_portal_scraper.py`**: The "Worker." Handles Phase 1 (Pagination), AI Filtering, and Phase 2 (Deep Extraction).
3. **`data_aggregation.py`**: The "Analyst." Compiles cross-portal stats and generates the final business reports.

### Workflow Logic

```text
production_orchestrator.py
        │
        ├── Multiprocessing (Isolated Environments)
        │
        ├── production_portal_scraper.py
        │       ├── Phase 1: Unlimited pagination & raw extraction
        │       ├── AI Filtering: Title classification via Mistral
        │       ├── Phase 2: Work Description & Deep Link extraction
        │       └── Storage: UPSERT to SQLite & Local Excel Mirrors
        │
        └── data_aggregation.py
                └── Consolidated Excel Reports & Performance Metrics

```

---

## 🌐 Supported Portals

| Portal ID | Name | Notes |
| --- | --- | --- |
| **WB** | West Bengal | Standard NIC portal architecture |
| **BHEL** | BHEL | Specialized enterprise portal |
| **COAL** | Coal India | NIC-based structure |
| **NTPC** | NTPC | Includes custom alert dialog handling |

---

## 🗄 Database Schema

The system utilizes `database/multi_portal_tenders.db` with the following core structure:

### 1. `tenders` Table

* `identity_hash`: Unique SHA-256 hash to prevent duplicates.
* `portal_id`: Source identifier.
* `ai_filtered`: Boolean flag for relevance.
* `phase2_status`: Tracking for deep extraction progress.

### 2. Supporting Tables

* **`scraping_metadata`**: Stores pagination states for auto-resume.
* **`failed_urls`**: Error logging for Phase 2 retries.
* **`portal_execution_log`**: Audit trail (Start/End times, page counts, error rates).

---

## ⚙️ Installation

### 1. Clone & Setup

```bash
git clone https://github.com/your-username/multi-portal-tender-scraper.git
cd multi-portal-tender-scraper
python -m venv venv
source venv/bin/activate  # Linux/macOS
# OR
venv\Scripts\activate     # Windows

```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
playwright install chromium

```

### 3. Environment Variables

Set your Mistral API key for the AI filtering module:

```powershell
# Windows
setx MISTRAL_API_KEY "your_api_key_here"

# Linux/macOS
export MISTRAL_API_KEY="your_api_key_here"

```

---

## ▶️ Running the System

### Full Production Run

To launch all scrapers concurrently with isolated logging:

```bash
python production_orchestrator.py

```

### Generate Reports

To aggregate data from the database into a polished Excel file:

```bash
python data_aggregation.py

```

---

## 🧠 AI Filtering (Mistral)

The system utilizes the `mistral-large-latest` model to process tender titles in batches:

* **Batch Size:** 50 titles per request.
* **Logic:** Classifies tenders as "Meaningful" or "Unmeaningful" based on project scope.
* **Cache:** Each portal maintains an isolated AI cache to reduce API costs.

---

## 🛡 Production Safety Features

* ✅ **SQLite Transaction Safety:** Prevents DB corruption during concurrent writes.
* ✅ **Session Refresh:** Automatically restarts browser contexts every 10 detailed scrapes.
* ✅ **Staggered Start:** 5-second delay between portal launches to prevent CPU spikes.
* ✅ **Auto-Resume:** Detects previous crashes and picks up from the last scraped page.

---

## 📁 Project Structure

```text
.
├── production_orchestrator.py   # Main entry point
├── production_portal_scraper.py  # Core scraping logic
├── data_aggregation.py           # Reporting & Analytics
├── database/
│   └── multi_portal_tenders.db   # Central SQLite storage
├── portals/                      # Portal-specific assets
│   ├── WB/
│   ├── BHEL/
│   └── NTPC/
│       ├── logs/                 # Rotation-based log files
│       ├── excel_mirrors/        # Live-updated Excel backups
│       └── checkpoints/          # Resume state files
└── README.md

```

---

## 🏆 Comparison: Script vs. Production System

| Feature | Typical Script | This System |
| --- | --- | --- |
| **Architecture** | Single-threaded | Isolated Multiprocessing |
| **Interruption** | Data loss / Restart | Full Auto-Resume |
| **Deduplication** | Manual/None | SQL UPSERT Logic |
| **Error Handling** | Generic Try/Except | Detailed Error Tracking Table |
| **Scaling** | Limited | 5,000+ tenders per portal |

---

## 👨‍💻 Author

**Multi-Portal Scraper System** Built for scalable, fault-tolerant government tender extraction.

---

### Would you like me to generate the `requirements.txt` file or a `Dockerfile` to containerize this setup?
