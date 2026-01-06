CREATE EXTENSION IF NOT EXISTS citext;

CREATE TABLE IF NOT EXISTS law_firms (
    id SERIAL PRIMARY KEY,
    website_url TEXT UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS extracted_emails (
    id SERIAL PRIMARY KEY,
    firm_id INTEGER REFERENCES law_firms(id) ON DELETE CASCADE,
    email citext NOT NULL,
    source_page TEXT,
    found_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(firm_id, email)
);