-- EnrichIQ Database Schema

CREATE TABLE routing_rules (
    id SERIAL PRIMARY KEY,
    business_key VARCHAR(50) NOT NULL,
    domain_pattern VARCHAR(255),
    company_pattern VARCHAR(255),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE linkedin_sessions (
    id SERIAL PRIMARY KEY,
    session_cookie TEXT NOT NULL,
    user_agent VARCHAR(255),
    created_at TIMESTAMP DEFAULT NOW(),
    last_used_at TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE
);
