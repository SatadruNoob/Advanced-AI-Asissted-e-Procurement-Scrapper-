"""
PRODUCTION MULTI-PORTAL ORCHESTRATOR
=====================================
Runs all 4 portals (WB, BHEL, COAL, NTPC) with 5-second staggered start.
NO PAGE LIMITS - scrapes all available data.
"""

import multiprocessing
import time
import os
from datetime import datetime
from production_portal_scraper import IsolatedPortalScraper, PORTALS
import logging

# ======================= CONFIGURATION =======================

# Shared database for all portals
DATABASE_PATH = "database/multi_portal_tenders.db"

# Mistral API key
MISTRAL_API_KEY = os.getenv("MISTRAL_API_KEY", "")

# PRODUCTION MODE: No page limits
# (test_pages parameter removed from production version)

# Stagger delay between portal starts
PORTAL_START_DELAY = 5  # seconds

# ======================= SETUP =======================

os.makedirs("database", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='[ORCHESTRATOR] %(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(f'orchestrator_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ======================= PORTAL RUNNER =======================

def run_portal_instance(portal_id: str, db_path: str, api_key: str):
    """
    Run a single portal instance in isolated process.
    PRODUCTION MODE: Unlimited pagination.
    """
    try:
        config = PORTALS[portal_id]
        
        # Create isolated scraper instance (no test_pages parameter)
        scraper = IsolatedPortalScraper(
            config=config,
            db_path=db_path,
            api_key=api_key
        )
        
        # Run the scraper (unlimited)
        result = scraper.run()
        
        return {
            'portal_id': portal_id,
            'status': 'success',
            'stats': result
        }
    
    except Exception as e:
        import traceback
        return {
            'portal_id': portal_id,
            'status': 'error',
            'error': str(e),
            'traceback': traceback.format_exc()
        }

# ======================= ORCHESTRATOR =======================

def run_all_portals():
    """
    Orchestrate all 4 portals with staggered start.
    PRODUCTION MODE: Each portal runs until end of data.
    """
    logger.info("="*80)
    logger.info("PRODUCTION MULTI-PORTAL ORCHESTRATOR")
    logger.info("="*80)
    logger.info(f"Database: {DATABASE_PATH}")
    logger.info("Mode: PRODUCTION (Unlimited pagination)")
    logger.info(f"Portal start delay: {PORTAL_START_DELAY} seconds")
    logger.info(f"Portals: {', '.join(PORTALS.keys())}")
    logger.info("  - WB: West Bengal")
    logger.info("  - BHEL: BHEL (REPLACES BEL)")
    logger.info("  - COAL: Coal India")
    logger.info("  - NTPC: NTPC (with alert dialog handling)")
    logger.info("="*80 + "\n")
    
    processes = []
    portal_order = list(PORTALS.keys())
    
    start_time = datetime.now()
    
    # Start each portal with 5-second delay
    for idx, portal_id in enumerate(portal_order):
        logger.info(f"\n{'='*80}")
        logger.info(f"Starting Portal {idx+1}/4: {PORTALS[portal_id].name} ({portal_id})")
        logger.info(f"{'='*80}")
        
        process = multiprocessing.Process(
            target=run_portal_instance,
            args=(portal_id, DATABASE_PATH, MISTRAL_API_KEY),
            name=f"Portal_{portal_id}"
        )
        
        process.start()
        processes.append({
            'process': process,
            'portal_id': portal_id,
            'name': PORTALS[portal_id].name,
            'start_time': datetime.now()
        })
        
        logger.info(f"✓ Process started for {PORTALS[portal_id].name}")
        logger.info(f"  PID: {process.pid}")
        
        if idx < len(portal_order) - 1:
            logger.info(f"\n⏳ Waiting {PORTAL_START_DELAY} seconds before starting next portal...")
            time.sleep(PORTAL_START_DELAY)
    
    logger.info(f"\n{'='*80}")
    logger.info("ALL PORTALS STARTED")
    logger.info(f"{'='*80}")
    logger.info(f"Active processes: {len(processes)}")
    for p in processes:
        logger.info(f"  - {p['name']} ({p['portal_id']}): PID {p['process'].pid}")
    logger.info(f"{'='*80}\n")
    
    logger.info("Monitoring portal execution...")
    logger.info("(Production mode: May take 2-4 hours per portal)\n")
    
    completed = []
    failed = []
    
    while any(p['process'].is_alive() for p in processes):
        for portal_info in processes:
            process = portal_info['process']
            portal_id = portal_info['portal_id']
            
            if not process.is_alive() and portal_id not in [c['portal_id'] for c in completed + failed]:
                end_time = datetime.now()
                duration = (end_time - portal_info['start_time']).total_seconds()
                
                if process.exitcode == 0:
                    completed.append({
                        'portal_id': portal_id,
                        'name': portal_info['name'],
                        'duration': duration
                    })
                    logger.info(f"✓ {portal_info['name']} ({portal_id}) completed successfully")
                    logger.info(f"  Duration: {duration/60:.1f} minutes ({duration/3600:.1f} hours)")
                else:
                    failed.append({
                        'portal_id': portal_id,
                        'name': portal_info['name'],
                        'exitcode': process.exitcode
                    })
                    logger.error(f"✗ {portal_info['name']} ({portal_id}) failed with exit code {process.exitcode}")
        
        time.sleep(5)
    
    total_duration = (datetime.now() - start_time).total_seconds()
    
    logger.info(f"\n{'='*80}")
    logger.info("ALL PORTALS COMPLETED")
    logger.info(f"{'='*80}")
    logger.info(f"Total execution time: {total_duration/60:.1f} minutes ({total_duration/3600:.1f} hours)")
    logger.info(f"Successful: {len(completed)}/{len(processes)}")
    logger.info(f"Failed: {len(failed)}/{len(processes)}")
    
    if completed:
        logger.info("\n✓ Successful portals:")
        for portal in completed:
            logger.info(f"  - {portal['name']} ({portal['portal_id']}): {portal['duration']/60:.1f} min")
    
    if failed:
        logger.error("\n✗ Failed portals:")
        for portal in failed:
            logger.error(f"  - {portal['name']} ({portal['portal_id']}): Exit code {portal['exitcode']}")
    
    logger.info(f"{'='*80}\n")
    
    for portal_info in processes:
        portal_info['process'].join(timeout=5)
    
    return {
        'completed': completed,
        'failed': failed,
        'total_duration': total_duration
    }

# ======================= ERROR LOG ANALYSIS =======================

def analyze_portal_errors(db_path: str):
    """Analyze errors from portal execution logs"""
    import sqlite3
    
    logger.info("\n" + "="*80)
    logger.info("ERROR ANALYSIS")
    logger.info("="*80)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Get latest execution for each portal
    cursor.execute("""
        SELECT portal_id, status, error_message, 
               total_extracted, phase2_success, phase2_failed, pages_scraped
        FROM portal_execution_log
        WHERE id IN (
            SELECT MAX(id) FROM portal_execution_log GROUP BY portal_id
        )
        ORDER BY portal_id
    """)
    
    results = cursor.fetchall()
    
    if not results:
        logger.info("No execution logs found")
        conn.close()
        return
    
    for row in results:
        portal_id, status, error_msg, extracted, p2_success, p2_failed, pages = row
        
        logger.info(f"\n{portal_id}:")
        logger.info(f"  Status: {status}")
        logger.info(f"  Pages scraped: {pages}")
        logger.info(f"  Extracted: {extracted}")
        logger.info(f"  Phase 2: {p2_success} success, {p2_failed} failed")
        
        if error_msg:
            logger.error(f"  Error: {error_msg}")
    
    # Check for failed URLs per portal
    logger.info("\n" + "-"*80)
    logger.info("Failed URLs by Portal:")
    logger.info("-"*80)
    
    cursor.execute("""
        SELECT portal_id, COUNT(*) as failed_count
        FROM failed_urls
        WHERE status = 'failed'
        GROUP BY portal_id
        ORDER BY failed_count DESC
    """)
    
    failed_urls = cursor.fetchall()
    
    if failed_urls:
        for portal_id, count in failed_urls:
            logger.info(f"  {portal_id}: {count} failed URLs")
    else:
        logger.info("  No failed URLs")
    
    conn.close()
    logger.info("="*80 + "\n")

# ======================= MAIN =======================

if __name__ == "__main__":
    multiprocessing.set_start_method('spawn', force=True)
    
    try:
        # Run all portals (production mode)
        results = run_all_portals()
        
        # Analyze any errors
        analyze_portal_errors(DATABASE_PATH)
        
        # Print summary
        logger.info("\n" + "="*80)
        logger.info("EXECUTION SUMMARY")
        logger.info("="*80)
        logger.info(f"Database: {DATABASE_PATH}")
        logger.info(f"Portal logs: portals/[PORTAL_ID]/logs/")
        logger.info(f"Excel outputs: portals/[PORTAL_ID]/excel_mirrors/")
        logger.info("="*80)
        
        if results['failed']:
            logger.error("\n⚠️  Some portals failed. Check logs for details.")
            exit(1)
        else:
            logger.info("\n✓ All portals completed successfully!")
            exit(0)
    
    except KeyboardInterrupt:
        logger.warning("\n\n⚠️  Execution interrupted by user")
        logger.info("Waiting for running processes to terminate...")
        exit(130)
    
    except Exception as e:
        logger.error(f"\n✗ Fatal orchestrator error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        exit(1)
