-- file: database_writer/init.sql
CREATE TABLE IF NOT EXISTS scores (
    transaction_id VARCHAR(255) PRIMARY KEY,
    score NUMERIC(10, 8),
    fraud_flag BOOLEAN,
    received_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);