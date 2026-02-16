"""
UNIFIED DATA AGGREGATION & REPORTING
====================================
Query and analyze combined results from all portals
"""

import sqlite3
import pandas as pd
from datetime import datetime
from pathlib import Path

DATABASE_PATH = "database/multi_portal_tenders.db"

# ======================= AGGREGATION QUERIES =======================

class MultiPortalAnalyzer:
    """Analyze combined data from all portals"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
    
    def get_connection(self):
        return sqlite3.connect(self.db_path)
    
    def get_combined_statistics(self) -> pd.DataFrame:
        """Get statistics for all portals combined"""
        query = """
        SELECT 
            portal_id as 'Portal ID',
            portal_source as 'Portal Name',
            COUNT(*) as 'Total Extracted',
            SUM(CASE WHEN ai_filtered = 1 THEN 1 ELSE 0 END) as 'AI Kept',
            SUM(CASE WHEN ai_filtered = -1 THEN 1 ELSE 0 END) as 'AI Filtered',
            SUM(CASE WHEN phase2_status = 'success' THEN 1 ELSE 0 END) as 'Phase 2 Success',
            SUM(CASE WHEN phase2_status = 'failed' THEN 1 ELSE 0 END) as 'Phase 2 Failed',
            SUM(CASE WHEN phase2_status = 'pending' THEN 1 ELSE 0 END) as 'Phase 2 Pending'
        FROM tenders
        GROUP BY portal_id, portal_source
        ORDER BY portal_id
        """
        
        conn = self.get_connection()
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        # Add totals row
        totals = pd.DataFrame([{
            'Portal ID': 'TOTAL',
            'Portal Name': 'All Portals',
            'Total Extracted': df['Total Extracted'].sum(),
            'AI Kept': df['AI Kept'].sum(),
            'AI Filtered': df['AI Filtered'].sum(),
            'Phase 2 Success': df['Phase 2 Success'].sum(),
            'Phase 2 Failed': df['Phase 2 Failed'].sum(),
            'Phase 2 Pending': df['Phase 2 Pending'].sum()
        }])
        
        df = pd.concat([df, totals], ignore_index=True)
        
        return df
    
    def get_all_kept_tenders(self) -> pd.DataFrame:
        """Get all tenders that were kept after AI filtering"""
        query = """
        SELECT 
            portal_id as 'Portal ID',
            portal_source as 'Portal Source',
            s_no as 'S.No.',
            title as 'Title',
            work_description as 'Work Description',
            e_published_date as 'e-Published Date',
            closing_date as 'Closing Date',
            opening_date as 'Opening Date',
            org_chain as 'Organisation',
            details_url as 'Details URL',
            phase2_status as 'Phase 2 Status'
        FROM tenders
        WHERE ai_filtered = 1
        ORDER BY portal_id, CAST(s_no AS INTEGER)
        """
        
        conn = self.get_connection()
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        return df
    
    def get_execution_history(self) -> pd.DataFrame:
        """Get execution history for all portals"""
        query = """
        SELECT 
            portal_id as 'Portal ID',
            datetime(execution_start) as 'Start Time',
            datetime(execution_end) as 'End Time',
            ROUND((julianday(execution_end) - julianday(execution_start)) * 24 * 60, 1) as 'Duration (min)',
            status as 'Status',
            total_extracted as 'Extracted',
            total_kept as 'Kept',
            phase2_success as 'P2 Success',
            phase2_failed as 'P2 Failed',
            error_message as 'Error'
        FROM portal_execution_log
        ORDER BY execution_start DESC
        """
        
        conn = self.get_connection()
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        return df
    
    def get_failed_urls_summary(self) -> pd.DataFrame:
        """Get summary of failed URLs by portal"""
        query = """
        SELECT 
            f.portal_id as 'Portal ID',
            COUNT(*) as 'Failed Count',
            GROUP_CONCAT(DISTINCT f.failure_reason) as 'Failure Reasons'
        FROM failed_urls f
        WHERE f.status = 'failed'
        GROUP BY f.portal_id
        ORDER BY COUNT(*) DESC
        """
        
        conn = self.get_connection()
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        return df
    
    def get_tenders_by_closing_date(self, days_ahead: int = 7) -> pd.DataFrame:
        """Get tenders closing within N days across all portals"""
        query = """
        SELECT 
            portal_id as 'Portal ID',
            portal_source as 'Portal',
            title as 'Title',
            closing_date as 'Closing Date',
            org_chain as 'Organisation',
            details_url as 'URL'
        FROM tenders
        WHERE ai_filtered = 1
        AND phase2_status = 'success'
        ORDER BY closing_date
        """
        
        conn = self.get_connection()
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        return df
    
    def export_combined_excel(self, output_path: str):
        """Export combined data to Excel with multiple sheets"""
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            # Sheet 1: Statistics
            stats_df = self.get_combined_statistics()
            stats_df.to_excel(writer, sheet_name='Statistics', index=False)
            
            # Sheet 2: All Kept Tenders
            tenders_df = self.get_all_kept_tenders()
            tenders_df.to_excel(writer, sheet_name='All Kept Tenders', index=False)
            
            # Sheet 3: Execution History
            history_df = self.get_execution_history()
            history_df.to_excel(writer, sheet_name='Execution History', index=False)
            
            # Sheet 4: Failed URLs Summary
            failed_df = self.get_failed_urls_summary()
            if not failed_df.empty:
                failed_df.to_excel(writer, sheet_name='Failed URLs', index=False)
        
        print(f"✓ Combined report exported to: {output_path}")
    
    def print_summary(self):
        """Print summary to console"""
        print("\n" + "="*80)
        print("MULTI-PORTAL SUMMARY")
        print("="*80 + "\n")
        
        # Statistics
        stats_df = self.get_combined_statistics()
        print("Statistics by Portal:")
        print(stats_df.to_string(index=False))
        
        # Execution history
        print("\n" + "-"*80)
        print("Latest Execution:")
        print("-"*80)
        history_df = self.get_execution_history()
        if not history_df.empty:
            latest = history_df.head(4)  # Show latest run for each portal
            print(latest[['Portal ID', 'Duration (min)', 'Status', 'Extracted', 'Kept']].to_string(index=False))
        
        # Failed URLs
        print("\n" + "-"*80)
        print("Failed URLs Summary:")
        print("-"*80)
        failed_df = self.get_failed_urls_summary()
        if not failed_df.empty:
            print(failed_df[['Portal ID', 'Failed Count']].to_string(index=False))
        else:
            print("No failed URLs")
        
        print("\n" + "="*80 + "\n")

# ======================= SPECIFIC QUERIES =======================

def query_portal_specific(db_path: str, portal_id: str) -> pd.DataFrame:
    """Query data for a specific portal"""
    query = f"""
    SELECT 
        s_no as 'S.No.',
        title as 'Title',
        work_description as 'Work Description',
        closing_date as 'Closing Date',
        org_chain as 'Organisation'
    FROM tenders
    WHERE portal_id = '{portal_id}'
    AND ai_filtered = 1
    ORDER BY CAST(s_no AS INTEGER)
    """
    
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    return df

def query_tenders_by_keyword(db_path: str, keyword: str) -> pd.DataFrame:
    """Search tenders across all portals by keyword"""
    query = f"""
    SELECT 
        portal_id as 'Portal',
        title as 'Title',
        work_description as 'Work Description',
        closing_date as 'Closing Date',
        details_url as 'URL'
    FROM tenders
    WHERE ai_filtered = 1
    AND (
        LOWER(title) LIKE '%{keyword.lower()}%'
        OR LOWER(work_description) LIKE '%{keyword.lower()}%'
    )
    ORDER BY portal_id, closing_date
    """
    
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    return df

def compare_portals_performance(db_path: str) -> pd.DataFrame:
    """Compare extraction and success rates across portals"""
    query = """
    SELECT 
        portal_id as 'Portal',
        COUNT(*) as 'Total Tenders',
        SUM(CASE WHEN ai_filtered = 1 THEN 1 ELSE 0 END) as 'Kept',
        ROUND(SUM(CASE WHEN ai_filtered = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as 'Keep Rate %',
        SUM(CASE WHEN phase2_status = 'success' THEN 1 ELSE 0 END) as 'Phase 2 Success',
        ROUND(SUM(CASE WHEN phase2_status = 'success' THEN 1 ELSE 0 END) * 100.0 / 
              NULLIF(SUM(CASE WHEN ai_filtered = 1 THEN 1 ELSE 0 END), 0), 1) as 'Success Rate %'
    FROM tenders
    GROUP BY portal_id
    ORDER BY portal_id
    """
    
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    return df

# ======================= MAIN =======================

if __name__ == "__main__":
    # Create analyzer
    analyzer = MultiPortalAnalyzer(DATABASE_PATH)
    
    # Print summary
    analyzer.print_summary()
    
    # Export combined report
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"combined_report_{timestamp}.xlsx"
    analyzer.export_combined_excel(output_file)
    
    # Print performance comparison
    print("\n" + "="*80)
    print("PORTAL PERFORMANCE COMPARISON")
    print("="*80)
    perf_df = compare_portals_performance(DATABASE_PATH)
    print(perf_df.to_string(index=False))
    print("="*80 + "\n")
    
    # Example: Search for specific keywords
    print("\n" + "="*80)
    print("EXAMPLE: Search for 'solar' tenders")
    print("="*80)
    solar_df = query_tenders_by_keyword(DATABASE_PATH, 'solar')
    if not solar_df.empty:
        print(f"Found {len(solar_df)} tenders containing 'solar':")
        print(solar_df[['Portal', 'Title']].head(10).to_string(index=False))
    else:
        print("No results found")
    print("="*80 + "\n")
