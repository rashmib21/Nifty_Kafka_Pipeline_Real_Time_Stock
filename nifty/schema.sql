
-- Command: mysql -u root -p < schema.sql

CREATE DATABASE IF NOT EXISTS stock_db;
USE stock_db;

-- Table 1: stock_ticks
-- Stores every single price tick received for all 50 stocks
-- One row = one price update for one stock
CREATE TABLE IF NOT EXISTS stock_ticks (
    id            BIGINT         AUTO_INCREMENT PRIMARY KEY,
    symbol        VARCHAR(20)    NOT NULL,
    ltp           DECIMAL(10,2)  NOT NULL,
    volume        BIGINT         DEFAULT 0,
    open          DECIMAL(10,2)  DEFAULT 0.00,
    high          DECIMAL(10,2)  DEFAULT 0.00,
    low           DECIMAL(10,2)  DEFAULT 0.00,
    exchange_time BIGINT         NOT NULL,
    created_at    TIMESTAMP      DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY no_duplicate_ticks (symbol, exchange_time)
);

-- Table 2: stock_analytics
-- Stores latest analytics for each stock
-- Only ONE row per stock - updated every tick by consumer_analytics.py
CREATE TABLE IF NOT EXISTS stock_analytics (
    id                   BIGINT         AUTO_INCREMENT PRIMARY KEY,
    symbol               VARCHAR(20)    NOT NULL UNIQUE,
    ltp                  DECIMAL(10,2)  DEFAULT 0,
    vwap                 DECIMAL(10,2)  DEFAULT 0,
    price_change_percent DECIMAL(6,2)   DEFAULT 0,
    day_high             DECIMAL(10,2)  DEFAULT 0,
    day_low              DECIMAL(10,2)  DEFAULT 0,
    tick_count           BIGINT         DEFAULT 0,
    updated_at           DATETIME       DEFAULT NULL
);

-- Table 3: stock_alerts
-- Stores every alert triggered during the day
-- One row per alert event
CREATE TABLE IF NOT EXISTS stock_alerts (
    id            BIGINT        AUTO_INCREMENT PRIMARY KEY,
    symbol        VARCHAR(20)   NOT NULL,
    alert_type    VARCHAR(50)   NOT NULL,
    alert_message VARCHAR(500)  NOT NULL,
    ltp_at_alert  DECIMAL(10,2) DEFAULT 0,
    alerted_at    DATETIME      DEFAULT NULL,
    created_at    TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
);

-- Table 4: dlq_messages
-- Stores every message that failed to process
-- Used by dlq_consumer.py for inspection and replay
CREATE TABLE IF NOT EXISTS dlq_messages (
    id            BIGINT        AUTO_INCREMENT PRIMARY KEY,
    raw_payload   TEXT          NOT NULL,
    error_reason  VARCHAR(500)  NOT NULL,
    failed_at     DATETIME      NOT NULL,
    retried       TINYINT       DEFAULT 0,
    created_at    TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
);

-- Indexes to make database queries faster
CREATE INDEX IF NOT EXISTS idx_symbol      ON stock_ticks (symbol);
CREATE INDEX IF NOT EXISTS idx_time        ON stock_ticks (exchange_time);
CREATE INDEX IF NOT EXISTS idx_symbol_time ON stock_ticks (symbol, exchange_time);
CREATE INDEX IF NOT EXISTS idx_alert_sym   ON stock_alerts (symbol);