CREATE TABLE IF NOT EXISTS customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    company_name VARCHAR(100) NOT NULL,
    subscription_tier VARCHAR(20) NOT NULL, -- e.g., Enterprise, Premium, Free
    monthly_spend DECIMAL(10, 2),
    churn_risk_score FLOAT DEFAULT 0.0,
    account_manager VARCHAR(100)
);

-- Seed highly differentiated context for our AI to reason over later
INSERT INTO customers (customer_id, company_name, subscription_tier, monthly_spend, churn_risk_score, account_manager) VALUES
('CUST-101', 'Acme Global Corp', 'Enterprise', 15000.00, 0.85, 'Sarah Jenkins'),
('CUST-102', 'BetaTech Labs', 'Premium', 2500.00, 0.12, 'Michael Chen'),
('CUST-103', 'Zeta Logistics', 'Free', 0.00, 0.90, 'Self Service');