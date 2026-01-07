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
        max_depth=1,   
        max_pages=4,     
        url_scorer=legal_scorer,
        include_external=False
    )

    browser_cfg = BrowserConfig(headless=True, browser_type="chromium")
    
    # 3. Apply the Strategy to the Run Config
    run_cfg = CrawlerRunConfig(
        cache_mode="bypass",
        deep_crawl_strategy=smart_strategy  # <--- The "Brain" of the crawler
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
def process_firm(self, url):
    try:
        emails = asyncio.run(crawl_logic(url))
        
        # Logic for database remains the same (it was already good)
        conn = psycopg2.connect(DB_URL)
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO law_firms (website_url) VALUES (%s) 
                    ON CONFLICT (website_url) DO UPDATE SET website_url=EXCLUDED.website_url 
                    RETURNING id
                """, (url,))
                firm_id = cur.fetchone()[0]
                
                for email in emails:
                    cur.execute("""
                        INSERT INTO extracted_emails (firm_id, email, source_page) 
                        VALUES (%s, %s, %s) ON CONFLICT DO NOTHING
                    """, (firm_id, email, url))
        conn.close()
        return f"Processed {url}: {len(emails)} leads found."
    except Exception as e:
        # If it's a database lock or network blip, retry
        raise self.retry(exc=e, countdown=60)