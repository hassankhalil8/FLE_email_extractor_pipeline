import os
import re
import asyncio
import psycopg2
from .celery_app import app
from .extractor import ProductionEmailExtractor
from pydantic import BaseModel, EmailStr, ValidationError
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig
from crawl4ai.deep_crawling import BestFirstCrawlingStrategy
from crawl4ai.deep_crawling.scorers import KeywordRelevanceScorer


extractor = ProductionEmailExtractor()

DB_URL = os.getenv("DATABASE_URL", "postgresql://admin:password123@pgdb:5432/law_leads")
EMAIL_REGEX = r'\b(?<![0-9.])(?![0-9]{3,})[a-zA-Z0-9._%+-]+(?<!\.)@[a-zA-Z0-9.-]+\.(?!png|jpg|jpeg|gif|svg|webp|pdf|scaled|circle)([a-zA-Z]{2,20})\b'

class EmailLead(BaseModel):
    email: EmailStr

async def crawl_logic(url):
    # 1. Define the Priority Scorer (Target the 'Money' pages)
    legal_scorer = KeywordRelevanceScorer(
        keywords=["attorney", "partner", "team", "contact", "lawyer", "staff", "about", "people", "profiles"],
        weight=0.9
    )

    smart_strategy = BestFirstCrawlingStrategy(
        max_depth=2,   
        max_pages=10,     
        url_scorer=legal_scorer,
        include_external=False
    )

    browser_cfg = BrowserConfig(headless=True, browser_type="chromium")
    
    # 3. Apply the Strategy to the Run Config
    run_cfg = CrawlerRunConfig(
        cache_mode="bypass",
        deep_crawl_strategy=smart_strategy,
        page_timeout=120000,
        wait_until="networkidle"    # <--- The "Brain" of the crawler
    )
    
    async with AsyncWebCrawler(config=browser_cfg) as crawler:
        # Note: Crawl4AI deep crawling returns result as a combined markdown of all 4 pages
        results = await crawler.arun(url=url, config=run_cfg)
        
        if not results or not isinstance(results, list):
            return []
        
        combined_markdown = ""
        for res in results:
            if res.success and res.markdown:
                combined_markdown += f"\n{res.markdown}"
        
        if not combined_markdown:
            return []
        
        final_leads = extractor.extract_all_emails(combined_markdown)

        return [lead['normalized'] for lead in final_leads if lead['confidence'] in ['high', 'medium']]

@app.task(bind=True, max_retries=3)
def process_firm(self, lead_row):
    """
    lead_row: Dictionary containing all Excel columns
    """
    url = lead_row.get('website')
    if not url:
        return "Skipped: No URL"

    try:
        emails_data = asyncio.run(crawl_logic(url)) 
  
        unique_emails = {e for e in emails_data}
        emails_string = ", ".join(unique_emails)

        # 3. Save to DB exactly like the Excel Sheet
        conn = psycopg2.connect(DB_URL)
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO law_leads_final (
                        apollo_id, name, website, city, state, country, 
                        full_address, phone_number, gbp_link, gbp_review_count, 
                        gbp_category, county, estimated_num_employees, 
                        processing_status, emails
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (apollo_id) DO UPDATE SET 
                        emails = EXCLUDED.emails,
                        processing_status = 'completed';
                """, (
                    lead_row.get('apollo_id'),
                    lead_row.get('name'),
                    lead_row.get('website'),
                    lead_row.get('city'),
                    lead_row.get('state'),
                    lead_row.get('country'),
                    lead_row.get('full_address'),
                    lead_row.get('phone_number'),
                    lead_row.get('gbp_link'),
                    lead_row.get('gbp_review_count'),
                    lead_row.get('gbp_category'),
                    lead_row.get('county'),
                    lead_row.get('estimated_num_employees'),
                    'completed',
                    emails_string # Comma separated list
                ))
        conn.close()
        return f"Processed {url}: {len(unique_emails)} emails found."

    except Exception as e:
        raise self.retry(exc=e, countdown=60)