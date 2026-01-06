import os
import re
import asyncio
import psycopg2
from .celery_app import app
from pydantic import BaseModel, EmailStr, ValidationError
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig

DB_URL = os.getenv("DATABASE_URL", "postgresql://admin:password123@pgdb:5432/law_leads")
EMAIL_REGEX = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'

class EmailLead(BaseModel):
    email: EmailStr

async def crawl_logic(url):
    browser_cfg = BrowserConfig(headless=True)
    run_cfg = CrawlerRunConfig(max_depth=2, include_external=False)
    
    async with AsyncWebCrawler(config=browser_cfg) as crawler:
        result = await crawler.arun(url=url, config=run_cfg)
        if not result.success:
            return []
        
        raw_emails = re.findall(EMAIL_REGEX, result.markdown)
        valid_emails = []
        for e in set(raw_emails):
            try:
                # Pydantic validation: ensures it's a real email format
                valid_emails.append(EmailLead(email=e.lower()).email)
            except ValidationError:
                continue
        return valid_emails

@app.task(bind=True, max_retries=3)
def process_firm(self, url):
    try:
        emails = asyncio.run(crawl_logic(url))
        
        conn = psycopg2.connect(DB_URL)
        with conn:
            with conn.cursor() as cur:
                # Atomic Insert or Get Firm ID
                cur.execute("""
                    INSERT INTO law_firms (website_url) VALUES (%s) 
                    ON CONFLICT (website_url) DO UPDATE SET website_url=EXCLUDED.website_url 
                    RETURNING id
                """, (url,))
                firm_id = cur.fetchone()[0]
                
                # Bulk save emails with Conflict Handling
                for email in emails:
                    cur.execute("""
                        INSERT INTO extracted_emails (firm_id, email, source_page) 
                        VALUES (%s, %s, %s) ON CONFLICT DO NOTHING
                    """, (firm_id, email, url))
        conn.close()
        return f"Processed {url}: {len(emails)} found."
    except Exception as e:
        raise self.retry(exc=e, countdown=60)