CREATE EXTENSION IF NOT EXISTS citext;

CREATE TABLE IF NOT EXISTS law_leads_final (
    apollo_id TEXT PRIMARY KEY,
    name TEXT,
    website TEXT,
    city TEXT,
    state TEXT,
    country TEXT,
    full_address TEXT,
    phone_number TEXT,
    gbp_link TEXT,
    gbp_review_count TEXT,
    gbp_category TEXT,
    county TEXT,
    estimated_num_employees TEXT,
    processing_status TEXT,
    emails TEXT,
    found_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);