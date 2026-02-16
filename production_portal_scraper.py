"""
MULTI-PORTAL TENDER SCRAPER - PRODUCTION VERSION
================================================
Features:
1. SQLite database for reliable storage
2. Automatic resume from database state
3. Excel mirror export for verification
4. Transaction safety (no data loss)
5. NO PAGE LIMITS - scrapes until end of data
6. Dynamic end-of-data detection
7. UPSERT logic to prevent duplicates
8. Stale element retry mechanism
"""

from playwright.sync_api import sync_playwright, Page, BrowserContext
import pandas as pd
import sqlite3
import time
import os
import hashlib
from datetime import datetime
import logging
from typing import Dict, List, Optional
import json
from pathlib import Path

# Try to import Mistral
try:
    from mistralai import Mistral
    MISTRAL_AVAILABLE = True
except ImportError:
    MISTRAL_AVAILABLE = False

# ======================= PORTAL CONFIGURATION =======================

class PortalConfig:
    """Configuration for each portal - immutable"""
    
    def __init__(self, portal_id: str, name: str, base_url: str, 
                 portal_url: str, pre_condition: Optional[str] = None):
        self.portal_id = portal_id
        self.name = name
        self.base_url = base_url
        self.portal_url = portal_url
        self.pre_condition = pre_condition
        
        # Isolated directories for this portal
        self.work_dir = Path(f"portals/{portal_id}")
        self.log_dir = self.work_dir / "logs"
        self.excel_dir = self.work_dir / "excel_mirrors"
        self.checkpoint_dir = self.work_dir / "checkpoints"
        
        # Create directories
        for dir_path in [self.work_dir, self.log_dir, self.excel_dir, self.checkpoint_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)
    
    def get_log_file(self) -> str:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return str(self.log_dir / f"scraper_{timestamp}.log")
    
    def get_excel_path(self, filename: str) -> str:
        return str(self.excel_dir / filename)
    
    def get_checkpoint_path(self, filename: str) -> str:
        return str(self.checkpoint_dir / filename)


# PRODUCTION Portal configurations - BEL replaced with BHEL
PORTALS = {
    'WB': PortalConfig(
        portal_id='WB',
        name='West Bengal',
        base_url='https://wbtenders.gov.in',
        portal_url='https://wbtenders.gov.in/nicgep/app'
    ),
    'BHEL': PortalConfig(
        portal_id='BHEL',
        name='BHEL',
        base_url='https://eprocurebhel.co.in',
        portal_url='https://eprocurebhel.co.in/nicgep/app'
    ),
    'COAL': PortalConfig(
        portal_id='COAL',
        name='Coal India',
        base_url='https://coalindiatenders.nic.in',
        portal_url='https://coalindiatenders.nic.in/nicgep/app'
    ),
    'NTPC': PortalConfig(
        portal_id='NTPC',
        name='NTPC',
        base_url='https://eprocurentpc.nic.in',
        portal_url='https://eprocurentpc.nic.in/nicgep/app',
        pre_condition='close_alert_dialog'  # hideDialog() on .alertbutclose
    )
}

# ======================= ISOLATED AI CHECKER =======================

class IsolatedAIChecker:
    """AI checker with ISOLATED cache per portal instance"""
    
    def __init__(self, portal_id: str, api_key: str):
        self.portal_id = portal_id
        if not api_key or not MISTRAL_AVAILABLE:
            raise ValueError(f"[{portal_id}] Mistral API not available")
        
        self.client = Mistral(api_key=api_key)
        self.cache = {}  # ISOLATED CACHE
        
    def check_titles(self, titles: List[str]) -> Dict[str, bool]:
        """Batch analyze titles with isolated cache"""
        uncached = [t for t in titles if t not in self.cache]
        
        if not uncached:
            return {t: self.cache[t] for t in titles}
        
        titles_list = "\n".join([f"{i+1}. {title}" for i, title in enumerate(uncached)])
        
        prompt = f"""Analyze these tender titles. Classify as "meaningful" or "unmeaningful".

MEANINGFUL: Has descriptive English words about the tender
UNMEANINGFUL: Only codes/IDs/brackets/dates

{titles_list}

Respond ONLY with JSON:
{{"1": "meaningful", "2": "unmeaningful", ...}}"""

        try:
            response = self.client.chat.complete(
                model="mistral-large-latest",
                messages=[{"role": "user", "content": prompt}],
                temperature=0,
                response_format={"type": "json_object"}
            )
            
            content = response.choices[0].message.content.strip()
            if "```json" in content:
                content = content.split("```json")[1].split("```")[0].strip()
            elif "```" in content:
                content = content.split("```")[1].split("```")[0].strip()
            
            results = json.loads(content)
            
            for i, title in enumerate(uncached, 1):
                is_meaningful = results.get(str(i), "meaningful") == "meaningful"
                self.cache[title] = is_meaningful
            
            return {t: self.cache[t] for t in titles}
            
        except Exception as e:
            fallback = {t: True for t in uncached}
            self.cache.update(fallback)
            return {t: self.cache.get(t, True) for t in titles}

# ======================= ISOLATED DATABASE MANAGER =======================

class IsolatedDatabaseManager:
    """Database manager with portal-specific isolation and UPSERT logic"""
    
    def __init__(self, db_path: str, portal_id: str):
        self.db_path = db_path
        self.portal_id = portal_id
        self._init_schema()
    
    def _init_schema(self):
        """Initialize database schema with portal_id tagging"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Main tenders table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tenders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                portal_id TEXT NOT NULL,
                identity_hash TEXT NOT NULL,
                portal_source TEXT,
                s_no TEXT,
                e_published_date TEXT,
                closing_date TEXT,
                opening_date TEXT,
                title TEXT,
                org_chain TEXT,
                details_url TEXT,
                work_description TEXT,
                run_date TEXT,
                phase1_status TEXT DEFAULT 'extracted',
                phase2_status TEXT DEFAULT 'pending',
                ai_filtered INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(portal_id, identity_hash)
            )
        """)
        
        # Metadata table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS scraping_metadata (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                portal_id TEXT NOT NULL,
                key TEXT NOT NULL,
                value TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(portal_id, key)
            )
        """)
        
        # Failed URLs table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS failed_urls (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                portal_id TEXT NOT NULL,
                tender_id INTEGER,
                details_url TEXT,
                failure_reason TEXT,
                retry_count INTEGER DEFAULT 0,
                last_retry_at TIMESTAMP,
                status TEXT DEFAULT 'failed',
                FOREIGN KEY (tender_id) REFERENCES tenders(id)
            )
        """)
        
        # Portal execution log
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS portal_execution_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                portal_id TEXT NOT NULL,
                execution_start TIMESTAMP,
                execution_end TIMESTAMP,
                status TEXT,
                total_extracted INTEGER DEFAULT 0,
                total_filtered INTEGER DEFAULT 0,
                total_kept INTEGER DEFAULT 0,
                phase2_success INTEGER DEFAULT 0,
                phase2_failed INTEGER DEFAULT 0,
                error_message TEXT,
                pages_scraped INTEGER DEFAULT 0
            )
        """)
        
        # Indexes
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_portal_hash ON tenders(portal_id, identity_hash)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_portal_phase2 ON tenders(portal_id, phase2_status)")
        
        conn.commit()
        conn.close()
    
    def get_connection(self):
        """Get thread-safe connection"""
        return sqlite3.connect(self.db_path, check_same_thread=False)
    
    def set_metadata(self, key: str, value: str):
        """Set portal-specific metadata"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO scraping_metadata (portal_id, key, value, updated_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
        """, (self.portal_id, key, value))
        conn.commit()
        conn.close()
    
    def get_metadata(self, key: str) -> Optional[str]:
        """Get portal-specific metadata"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT value FROM scraping_metadata 
            WHERE portal_id = ? AND key = ?
        """, (self.portal_id, key))
        result = cursor.fetchone()
        conn.close()
        return result[0] if result else None
    
    def upsert_tenders_batch(self, tenders: List[dict]) -> tuple:
        """
        UPSERT multiple tenders - insert new, update existing.
        Returns (inserted, updated) counts.
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        inserted = 0
        updated = 0
        
        try:
            for tender in tenders:
                # Check if tender exists
                cursor.execute("""
                    SELECT id FROM tenders 
                    WHERE portal_id = ? AND identity_hash = ?
                """, (self.portal_id, tender['Identity Hash']))
                
                existing = cursor.fetchone()
                
                if existing:
                    # UPDATE existing tender
                    cursor.execute("""
                        UPDATE tenders SET
                            portal_source = ?,
                            s_no = ?,
                            e_published_date = ?,
                            closing_date = ?,
                            opening_date = ?,
                            title = ?,
                            org_chain = ?,
                            details_url = ?,
                            run_date = ?,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE portal_id = ? AND identity_hash = ?
                    """, (
                        tender['Portal Source'],
                        tender['S.No.'],
                        tender['e-Published Date'],
                        tender['Bid Submission Closing Date'],
                        tender['Tender Opening Date'],
                        tender['Title and Ref.No./Tender ID'],
                        tender['Organisation Chain'],
                        tender['Details URL'],
                        tender['Run Date'],
                        self.portal_id,
                        tender['Identity Hash']
                    ))
                    if cursor.rowcount > 0:
                        updated += 1
                else:
                    # INSERT new tender
                    cursor.execute("""
                        INSERT INTO tenders (
                            portal_id, identity_hash, portal_source, s_no, 
                            e_published_date, closing_date, opening_date, 
                            title, org_chain, details_url, work_description, 
                            run_date, phase1_status, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """, (
                        self.portal_id,
                        tender['Identity Hash'],
                        tender['Portal Source'],
                        tender['S.No.'],
                        tender['e-Published Date'],
                        tender['Bid Submission Closing Date'],
                        tender['Tender Opening Date'],
                        tender['Title and Ref.No./Tender ID'],
                        tender['Organisation Chain'],
                        tender['Details URL'],
                        tender.get('Work Description', ''),
                        tender['Run Date'],
                        'extracted'
                    ))
                    if cursor.rowcount > 0:
                        inserted += 1
            
            conn.commit()
            return (inserted, updated)
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
    
    def update_work_description(self, identity_hash: str, work_desc: str, status: str = 'success'):
        """Update work description for a tender"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE tenders 
            SET work_description = ?, phase2_status = ?, updated_at = CURRENT_TIMESTAMP
            WHERE portal_id = ? AND identity_hash = ?
        """, (work_desc, status, self.portal_id, identity_hash))
        conn.commit()
        conn.close()
    
    def mark_ai_filtered(self, identity_hash: str, keep: bool):
        """Mark tender as AI filtered"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE tenders 
            SET ai_filtered = ?, updated_at = CURRENT_TIMESTAMP
            WHERE portal_id = ? AND identity_hash = ?
        """, (1 if keep else -1, self.portal_id, identity_hash))
        conn.commit()
        conn.close()
    
    def get_phase1_count(self) -> int:
        """Get count of Phase 1 extracted tenders"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM tenders WHERE portal_id = ? AND phase1_status = 'extracted'", 
                      (self.portal_id,))
        count = cursor.fetchone()[0]
        conn.close()
        return count
    
    def get_tenders_for_ai_filtering(self) -> List[dict]:
        """Get unfiltered tenders for this portal"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT identity_hash, title FROM tenders 
            WHERE portal_id = ? AND ai_filtered = 0
        """, (self.portal_id,))
        rows = cursor.fetchall()
        conn.close()
        return [{'hash': r[0], 'title': r[1]} for r in rows]
    
    def get_tenders_for_phase2(self) -> List[dict]:
        """Get tenders needing work descriptions for this portal"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, identity_hash, details_url, s_no
            FROM tenders 
            WHERE portal_id = ? 
            AND (ai_filtered = 1 OR ai_filtered = 0) 
            AND phase2_status IN ('pending', 'failed')
            AND details_url != ''
        """, (self.portal_id,))
        rows = cursor.fetchall()
        conn.close()
        return [{'id': r[0], 'hash': r[1], 'url': r[2], 's_no': r[3]} for r in rows]
    
    def add_failed_url(self, tender_id: int, url: str, reason: str):
        """Track failed URL"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO failed_urls (portal_id, tender_id, details_url, failure_reason, last_retry_at)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, (self.portal_id, tender_id, url, reason))
        conn.commit()
        conn.close()
    
    def export_to_excel(self, filepath: str, filter_kept_only: bool = False):
        """Export portal data to Excel"""
        conn = self.get_connection()
        
        query = """
            SELECT 
                portal_id as 'Portal ID',
                portal_source as 'Portal Source',
                s_no as 'S.No.',
                org_chain as 'Organisation Chain',
                title as 'Title and Ref.No./Tender ID',
                work_description as 'Work Description',
                e_published_date as 'e-Published Date',
                closing_date as 'Bid Submission Closing Date',
                opening_date as 'Tender Opening Date',
                details_url as 'Details URL',
                identity_hash as 'Identity Hash',
                run_date as 'Run Date',
                phase2_status as 'Phase 2 Status'
            FROM tenders
            WHERE portal_id = ?
        """
        
        if filter_kept_only:
            query += " AND ai_filtered = 1"
        
        query += " ORDER BY id"
        
        df = pd.read_sql_query(query, conn, params=(self.portal_id,))
        conn.close()
        
        df.to_excel(filepath, index=False, engine='openpyxl')
        return len(df)
    
    def get_statistics(self) -> dict:
        """Get statistics for this portal"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM tenders WHERE portal_id = ?", (self.portal_id,))
        total = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM tenders WHERE portal_id = ? AND ai_filtered = 1", (self.portal_id,))
        kept = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM tenders WHERE portal_id = ? AND ai_filtered = -1", (self.portal_id,))
        filtered = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM tenders WHERE portal_id = ? AND phase2_status = 'success'", (self.portal_id,))
        phase2_success = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM tenders WHERE portal_id = ? AND phase2_status = 'failed'", (self.portal_id,))
        phase2_failed = cursor.fetchone()[0]
        
        conn.close()
        
        return {
            'portal_id': self.portal_id,
            'total_extracted': total,
            'ai_kept': kept,
            'ai_filtered': filtered,
            'phase2_success': phase2_success,
            'phase2_failed': phase2_failed
        }
    
    def log_execution(self, start_time: datetime, end_time: datetime, 
                     status: str, stats: dict, pages_scraped: int, error_msg: Optional[str] = None):
        """Log portal execution with page count"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO portal_execution_log (
                portal_id, execution_start, execution_end, status,
                total_extracted, total_filtered, total_kept,
                phase2_success, phase2_failed, pages_scraped, error_message
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            self.portal_id,
            start_time.isoformat(),
            end_time.isoformat(),
            status,
            stats.get('total_extracted', 0),
            stats.get('ai_filtered', 0),
            stats.get('ai_kept', 0),
            stats.get('phase2_success', 0),
            stats.get('phase2_failed', 0),
            pages_scraped,
            error_msg
        ))
        conn.commit()
        conn.close()

# File continues...

# ======================= ISOLATED PORTAL SCRAPER CLASS =======================

class IsolatedPortalScraper:
    """
    Production scraper with unlimited pagination and dynamic end-of-data detection.
    Complete isolation for each portal instance.
    """
    
    def __init__(self, config: PortalConfig, db_path: str, api_key: str):
        self.config = config
        self.portal_id = config.portal_id
        
        # Isolated components
        self.db = IsolatedDatabaseManager(db_path, config.portal_id)
        self.ai_checker = None
        if api_key and MISTRAL_AVAILABLE:
            self.ai_checker = IsolatedAIChecker(config.portal_id, api_key)
        
        # Isolated logger
        self.logger = self._setup_logger()
        
        # Session configuration
        self.page_load_timeout = 45000
        self.pagination_delay = 1.0  # Increased for production stability
        self.phase2_delay = 2
        self.session_refresh_every = 10
        
        # Stale element retry configuration
        self.max_stale_retries = 3
        self.stale_retry_delay = 2
        
        # Page tracking
        self.pages_scraped = 0
    
    def _setup_logger(self) -> logging.Logger:
        """Setup isolated logger for this portal"""
        logger = logging.getLogger(f"Portal_{self.portal_id}")
        logger.setLevel(logging.INFO)
        logger.handlers = []
        
        fh = logging.FileHandler(self.config.get_log_file(), encoding='utf-8')
        fh.setLevel(logging.INFO)
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        
        formatter = logging.Formatter(f'[{self.portal_id}] %(asctime)s [%(levelname)s] %(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        
        logger.addHandler(fh)
        logger.addHandler(ch)
        
        return logger
    
    def clean_text(self, text: str) -> str:
        """Clean extracted text"""
        if not text:
            return ""
        return text.strip().replace('\n', ' ').replace('\t', ' ').replace('  ', ' ')
    
    def create_identity_hash(self, tender_data: dict) -> str:
        """Create unique hash for deduplication"""
        key = f"{tender_data.get('Title and Ref.No./Tender ID', '')}_{tender_data.get('e-Published Date', '')}_{tender_data.get('Organisation Chain', '')}"
        return hashlib.md5(key.encode()).hexdigest()
    
    def handle_pre_condition(self, page: Page) -> bool:
        """Handle portal-specific pre-conditions (e.g., NTPC alert dialog)"""
        if self.config.pre_condition == 'close_alert_dialog':
            try:
                self.logger.info("Handling pre-condition: Closing alert dialog...")
                # Execute hideDialog() on .alertbutclose
                page.evaluate("document.querySelector('.alertbutclose')?.click()")
                time.sleep(1)
                self.logger.info("✓ Alert dialog closed via hideDialog()")
                return True
            except Exception as e:
                self.logger.warning(f"Pre-condition handling failed: {e}")
                # Try alternative method
                try:
                    close_button = page.query_selector(".alertbutclose")
                    if close_button:
                        close_button.click()
                        time.sleep(1)
                        self.logger.info("✓ Alert dialog closed via direct click")
                        return True
                except:
                    pass
                return False
        return True
    
    def navigate_to_closing_within_7_days(self, page: Page) -> bool:
        """Navigate to 'Closing within 7 days' section"""
        try:
            self.logger.info("Navigating to portal...")
            page.goto(self.config.portal_url, wait_until="domcontentloaded", timeout=self.page_load_timeout)
            time.sleep(2)
            
            # Handle pre-conditions (e.g., NTPC alert)
            self.handle_pre_condition(page)
            
            list_url = f"{self.config.portal_url}?page=FrontEndListTendersbyDate&service=page"
            page.goto(list_url, wait_until="domcontentloaded", timeout=self.page_load_timeout)
            time.sleep(2)
            
            self.logger.info("Clicking 'Closing within 7 days' tab...")
            try:
                page.evaluate("tapestry.form.submit('ListTendersbyDate', 'LinkSubmit_0');")
                time.sleep(3)
                return True
            except:
                link = page.query_selector("a#LinkSubmit_0")
                if link:
                    link.click()
                    time.sleep(3)
                    return True
            return False
        except Exception as e:
            self.logger.error(f"Navigation error: {e}")
            return False
    
    def extract_tenders_from_page(self, page: Page) -> List[dict]:
        """Extract tenders from current page"""
        tenders = []
        try:
            rows = page.query_selector_all("tr.even, tr.odd")
            
            for row in rows:
                try:
                    cells = row.query_selector_all("td")
                    if len(cells) < 6:
                        continue
                    
                    s_no = self.clean_text(cells[0].inner_text())
                    e_published = self.clean_text(cells[1].inner_text())
                    closing_date = self.clean_text(cells[2].inner_text())
                    opening_date = self.clean_text(cells[3].inner_text())
                    
                    title_cell = cells[4]
                    link_elem = title_cell.query_selector("a")
                    if link_elem:
                        title_text = self.clean_text(link_elem.inner_text())
                        href = link_elem.get_attribute("href")
                        details_url = f"{self.config.base_url}{href}" if href and href.startswith("/") else (href or "")
                    else:
                        title_text = self.clean_text(title_cell.inner_text())
                        details_url = ""
                    
                    ref_texts = title_cell.inner_text().split('\n')
                    ref_no = self.clean_text(ref_texts[1]) if len(ref_texts) > 1 else ""
                    full_title = f"{title_text}\n{ref_no}" if ref_no else title_text
                    
                    org_chain = self.clean_text(cells[5].inner_text())
                    
                    tender_data = {
                        "Portal Source": self.config.name,
                        "S.No.": s_no,
                        "e-Published Date": e_published,
                        "Bid Submission Closing Date": closing_date,
                        "Tender Opening Date": opening_date,
                        "Title and Ref.No./Tender ID": full_title,
                        "Organisation Chain": org_chain,
                        "Details URL": details_url,
                        "Work Description": "",
                        "Run Date": datetime.now().strftime("%Y-%m-%d")
                    }
                    
                    tender_data["Identity Hash"] = self.create_identity_hash(tender_data)
                    tenders.append(tender_data)
                    
                except Exception as row_error:
                    self.logger.warning(f"Row extraction error: {row_error}")
                    continue
            
            return tenders
        except Exception as e:
            self.logger.error(f"Page extraction error: {e}")
            return []
    
    def is_next_button_available(self, page: Page) -> tuple:
        """
        Check if Next button is available and clickable.
        Returns (available: bool, reason: str)
        
        CRITICAL: Dynamic end-of-data detection
        - Checks existence, visibility, and disabled state
        - Returns False if Next link is missing, hidden, or disabled
        """
        try:
            # Wait a moment for page to stabilize
            time.sleep(self.pagination_delay)
            
            # Primary selector: a#linkFwd
            next_link = page.query_selector("a#linkFwd")
            
            if not next_link:
                self.logger.info("Next link not found (a#linkFwd) - END OF DATA")
                return (False, "next_link_not_found")
            
            # Check if visible
            if not next_link.is_visible():
                self.logger.info("Next link exists but not visible - END OF DATA")
                return (False, "next_link_hidden")
            
            # Check if disabled (CSS class or attribute)
            classes = next_link.get_attribute('class') or ''
            disabled_attr = next_link.get_attribute('disabled')
            
            if 'disabled' in classes.lower() or disabled_attr is not None:
                self.logger.info("Next link is disabled - END OF DATA")
                return (False, "next_link_disabled")
            
            # Check if link is actually clickable (enabled)
            try:
                if not next_link.is_enabled():
                    self.logger.info("Next link is not enabled - END OF DATA")
                    return (False, "next_link_not_enabled")
            except:
                pass
            
            # Check href is valid
            href = next_link.get_attribute("href")
            if not href or href == "#" or href == "javascript:void(0)":
                self.logger.info("Next link has invalid href - END OF DATA")
                return (False, "next_link_invalid_href")
            
            # All checks passed - next button is available
            return (True, "available")
            
        except Exception as e:
            self.logger.error(f"Error checking next button: {e}")
            return (False, f"check_error: {str(e)}")
    
    def get_next_page_link_with_retry(self, page: Page, page_num: int) -> Optional[str]:
        """
        Get next page link with stale element retry mechanism.
        Returns URL or None if no more pages.
        
        Implements 3-attempt retry for stale element references.
        """
        for attempt in range(1, self.max_stale_retries + 1):
            try:
                # Check if next button is available first
                available, reason = self.is_next_button_available(page)
                
                if not available:
                    if attempt == 1:  # Only log on first attempt
                        self.logger.info(f"No more pages available: {reason}")
                    return None
                
                # Get the next link
                next_link = page.query_selector("a#linkFwd")
                if not next_link:
                    return None
                
                href = next_link.get_attribute("href")
                if not href:
                    return None
                
                next_url = f"{self.config.base_url}{href}" if href.startswith("/") else href
                return next_url
                
            except Exception as e:
                error_msg = str(e).lower()
                
                # Check if it's a stale element error
                if 'stale' in error_msg or 'detached' in error_msg:
                    self.logger.warning(f"Stale element on attempt {attempt}/{self.max_stale_retries}")
                    
                    if attempt < self.max_stale_retries:
                        time.sleep(self.stale_retry_delay)
                        self.logger.info(f"Retrying after stale element...")
                        continue
                    else:
                        self.logger.error("Max stale element retries reached")
                        return None
                else:
                    # Not a stale element error, don't retry
                    self.logger.error(f"Error getting next page link: {e}")
                    return None
        
        return None
    
    def run_phase1(self, page: Page) -> int:
        """
        Phase 1: Extract tenders with UNLIMITED pagination.
        Continues until no Next button found.
        """
        self.logger.info("="*80)
        self.logger.info("PHASE 1: EXTRACTING ALL TENDERS (UNLIMITED)")
        self.logger.info("="*80)
        
        # Check if we should resume
        last_page_str = self.db.get_metadata('last_page_extracted')
        start_page = int(last_page_str) + 1 if last_page_str else 1
        
        if start_page > 1:
            self.logger.info(f"📌 RESUMING from page {start_page}")
            # TODO: Navigate to resume point
        
        page_num = start_page
        total_extracted = self.db.get_phase1_count()
        
        # PRODUCTION: Unlimited loop until end of data
        while True:
            self.logger.info(f"--- Page {page_num} ---")
            
            page_tenders = self.extract_tenders_from_page(page)
            
            if page_tenders:
                # UPSERT to database (insert new, update existing)
                inserted, updated = self.db.upsert_tenders_batch(page_tenders)
                total_extracted = self.db.get_phase1_count()
                
                self.logger.info(f"Extracted {len(page_tenders)} tenders")
                self.logger.info(f"  Inserted: {inserted}, Updated: {updated}")
                self.logger.info(f"  Total in database: {total_extracted}")
                
                self.db.set_metadata('last_page_extracted', str(page_num))
                
                # Save Excel every 10 pages
                if page_num % 10 == 0:
                    excel_file = self.config.get_excel_path(f"Phase1_Page{page_num}.xlsx")
                    self.db.export_to_excel(excel_file)
            else:
                self.logger.warning(f"No tenders on page {page_num}")
            
            self.pages_scraped = page_num
            
            # Check for next page
            next_url = self.get_next_page_link_with_retry(page, page_num)
            
            if not next_url:
                self.logger.info(f"\n{'='*80}")
                self.logger.info(f"END OF DATA REACHED AT PAGE {page_num}")
                self.logger.info(f"{'='*80}\n")
                break
            
            # Navigate to next page with retry
            success = False
            for attempt in range(1, 3 + 1):
                try:
                    self.logger.info(f"Navigating to page {page_num + 1}...")
                    page.goto(next_url, wait_until="domcontentloaded", timeout=self.page_load_timeout)
                    time.sleep(self.pagination_delay)
                    success = True
                    break
                except Exception as e:
                    self.logger.warning(f"Navigation attempt {attempt} failed: {e}")
                    if attempt < 3:
                        page.reload()
                        time.sleep(1)
            
            if not success:
                self.logger.error(f"Could not navigate to page {page_num + 1}")
                break
            
            page_num += 1
        
        self.logger.info(f"\n{'='*80}")
        self.logger.info(f"PHASE 1 COMPLETE")
        self.logger.info(f"  Pages scraped: {page_num}")
        self.logger.info(f"  Total tenders: {total_extracted}")
        self.logger.info(f"{'='*80}\n")
        
        # Final Excel
        excel_file = self.config.get_excel_path("Phase1_Complete.xlsx")
        self.db.export_to_excel(excel_file)
        
        self.db.set_metadata('phase1_complete', 'true')
        return total_extracted
    
    def run_ai_filtering(self):
        """AI filtering phase"""
        if not self.ai_checker:
            self.logger.info("AI filtering disabled")
            return
        
        self.logger.info("="*80)
        self.logger.info("AI FILTERING")
        self.logger.info("="*80)
        
        tenders_to_filter = self.db.get_tenders_for_ai_filtering()
        
        if not tenders_to_filter:
            self.logger.info("No tenders need filtering")
            return
        
        self.logger.info(f"Filtering {len(tenders_to_filter)} tenders...")
        
        batch_size = 50
        for i in range(0, len(tenders_to_filter), batch_size):
            batch = tenders_to_filter[i:i+batch_size]
            titles = [t['title'] for t in batch]
            
            self.logger.info(f"Batch {i//batch_size + 1}/{(len(tenders_to_filter)-1)//batch_size + 1}...")
            results = self.ai_checker.check_titles(titles)
            
            for tender in batch:
                is_meaningful = results.get(tender['title'], False)
                keep = not is_meaningful
                self.db.mark_ai_filtered(tender['hash'], keep)
        
        stats = self.db.get_statistics()
        self.logger.info(f"\nFiltering complete: {stats['ai_kept']} kept, {stats['ai_filtered']} filtered\n")
        
        excel_file = self.config.get_excel_path("Filtered_Kept.xlsx")
        self.db.export_to_excel(excel_file, filter_kept_only=True)
        
        self.db.set_metadata('ai_filtering_complete', 'true')
    
    def fetch_work_description(self, url: str, context: BrowserContext, retry: int = 0) -> str:
        """Fetch work description with proper extraction"""
        page = None
        try:
            page = context.new_page()
            page.set_default_timeout(self.page_load_timeout)
            
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=self.page_load_timeout)
            except Exception as e:
                if retry < 2:
                    page.close()
                    time.sleep(1)
                    return self.fetch_work_description(url, context, retry + 1)
                raise e
            
            if "CommonErrorPage" in page.url:
                return "ERROR: Session expired"
            
            time.sleep(self.phase2_delay)
            
            # Method 1: CSS selector
            try:
                page.wait_for_selector("td.td_caption", timeout=5000)
                work_desc_selector = "td.td_caption:has-text('Work Description') + td.td_field"
                raw_text = page.inner_text(work_desc_selector)
                cleaned_text = " ".join(raw_text.split())
                
                if cleaned_text:
                    return cleaned_text
            except Exception:
                pass
            
            # Method 2: Fallback
            try:
                rows = page.query_selector_all("tbody tr")
                for row in rows:
                    cells = row.query_selector_all("td")
                    if len(cells) >= 2 and "Work Description" in cells[0].inner_text():
                        work_desc = cells[1].inner_text().strip()
                        cleaned = " ".join(work_desc.split())
                        if cleaned:
                            return cleaned
            except Exception:
                pass
            
            return "WORK_DESCRIPTION_NOT_FOUND"
        except Exception as e:
            return f"FETCH_ERROR: {str(e)[:100]}"
        finally:
            if page:
                page.close()
    
    def run_phase2(self, context: BrowserContext):
        """Phase 2: Fetch work descriptions sequentially"""
        self.logger.info("="*80)
        self.logger.info("PHASE 2: WORK DESCRIPTIONS")
        self.logger.info("="*80)
        
        tenders = self.db.get_tenders_for_phase2()
        
        if not tenders:
            self.logger.info("No tenders need Phase 2")
            return
        
        self.logger.info(f"Processing {len(tenders)} tenders...")
        
        processed = 0
        success = 0
        failed = 0
        
        for idx, tender in enumerate(tenders, 1):
            # Refresh session every N tenders
            if idx % self.session_refresh_every == 1 and idx > 1:
                self.logger.info(f"\n🔄 Refreshing session...")
                try:
                    refresh_page = context.new_page()
                    refresh_page.goto(self.config.portal_url, wait_until="domcontentloaded", timeout=self.page_load_timeout)
                    time.sleep(1)
                    refresh_page.close()
                    self.logger.info("✓ Session refreshed\n")
                except Exception as e:
                    self.logger.warning(f"Session refresh warning: {e}")
            
            try:
                work_desc = self.fetch_work_description(tender['url'], context)
                
                if work_desc.startswith("ERROR") or work_desc.startswith("FETCH") or work_desc.startswith("WORK_DESCRIPTION"):
                    self.db.update_work_description(tender['hash'], work_desc, 'failed')
                    self.db.add_failed_url(tender['id'], tender['url'], work_desc)
                    failed += 1
                else:
                    self.db.update_work_description(tender['hash'], work_desc, 'success')
                    success += 1
                
                processed += 1
                
                if processed % 10 == 0:
                    self.logger.info(f"Progress: {processed}/{len(tenders)} ({success} success, {failed} failed)")
                
                if processed % 50 == 0:
                    excel_file = self.config.get_excel_path(f"Phase2_Progress_{processed}.xlsx")
                    self.db.export_to_excel(excel_file, filter_kept_only=True)
            
            except Exception as e:
                self.logger.error(f"Error processing tender {idx}: {e}")
                self.db.update_work_description(tender['hash'], f"PROCESSING_ERROR: {str(e)[:100]}", 'failed')
                failed += 1
                processed += 1
        
        self.logger.info(f"\n{'='*80}")
        self.logger.info(f"PHASE 2 COMPLETE: {success} success, {failed} failed")
        self.logger.info(f"{'='*80}\n")
        
        excel_file = self.config.get_excel_path("Phase2_Complete.xlsx")
        self.db.export_to_excel(excel_file, filter_kept_only=True)
        
        self.db.set_metadata('phase2_complete', 'true')
    
    def run(self) -> dict:
        """Main execution method - PRODUCTION VERSION"""
        start_time = datetime.now()
        stats = {'status': 'unknown', 'error': None}
        
        self.logger.info("="*80)
        self.logger.info(f"STARTING PORTAL: {self.config.name}")
        self.logger.info("PRODUCTION MODE: Unlimited pagination")
        self.logger.info("="*80)
        
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=False)
                context = browser.new_context()
                page = context.new_page()
                page.set_default_timeout(self.page_load_timeout)
                
                try:
                    # Phase 1: Unlimited pagination
                    if not self.db.get_metadata('phase1_complete'):
                        if not self.navigate_to_closing_within_7_days(page):
                            raise Exception("Navigation failed")
                        self.run_phase1(page)
                    else:
                        self.logger.info("✓ Phase 1 already complete\n")
                        # Get pages count from metadata
                        last_page = self.db.get_metadata('last_page_extracted')
                        if last_page:
                            self.pages_scraped = int(last_page)
                    
                    # AI Filtering
                    if self.ai_checker and not self.db.get_metadata('ai_filtering_complete'):
                        self.run_ai_filtering()
                    else:
                        self.logger.info("✓ AI filtering skipped\n")
                    
                    # Phase 2
                    if not self.db.get_metadata('phase2_complete'):
                        self.run_phase2(context)
                    else:
                        self.logger.info("✓ Phase 2 already complete\n")
                    
                    stats['status'] = 'success'
                    
                finally:
                    context.close()
                    browser.close()
        
        except Exception as e:
            self.logger.error(f"Fatal error: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            stats['status'] = 'error'
            stats['error'] = str(e)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        # Get final statistics
        final_stats = self.db.get_statistics()
        final_stats.update(stats)
        
        # Log execution with page count
        self.db.log_execution(start_time, end_time, stats['status'], final_stats, 
                             self.pages_scraped, stats.get('error'))
        
        self.logger.info("\n" + "="*80)
        self.logger.info("PORTAL EXECUTION COMPLETE")
        self.logger.info("="*80)
        self.logger.info(f"Status: {stats['status']}")
        self.logger.info(f"Duration: {duration/60:.1f} minutes")
        self.logger.info(f"Pages scraped: {self.pages_scraped}")
        self.logger.info(f"Total extracted: {final_stats['total_extracted']}")
        self.logger.info(f"AI kept: {final_stats['ai_kept']}")
        self.logger.info(f"Phase 2 success: {final_stats['phase2_success']}")
        self.logger.info("="*80 + "\n")
        
        return final_stats

