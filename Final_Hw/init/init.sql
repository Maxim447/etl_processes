CREATE DATABASE etl_db;

\c etl_db;

CREATE TABLE IF NOT EXISTS user_sessions (
    session_id      VARCHAR(100) PRIMARY KEY,
    user_id         VARCHAR(100),
    start_time      TIMESTAMP,
    end_time        TIMESTAMP,
    pages_visited   TEXT[],
    device          VARCHAR(100),
    actions         TEXT[],
    loaded_at       TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS event_logs (
    event_id        VARCHAR(100) PRIMARY KEY,
    timestamp       TIMESTAMP,
    event_type      VARCHAR(100),
    details         TEXT,
    loaded_at       TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS support_tickets (
    ticket_id       VARCHAR(100) PRIMARY KEY,
    user_id         VARCHAR(100),
    status          VARCHAR(50),
    issue_type      VARCHAR(100),
    messages        JSONB,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,
    loaded_at       TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS user_recommendations (
    user_id                 VARCHAR(100) PRIMARY KEY,
    recommended_products    TEXT[],
    last_updated            TIMESTAMP,
    loaded_at               TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS moderation_queue (
    review_id           VARCHAR(100) PRIMARY KEY,
    user_id             VARCHAR(100),
    product_id          VARCHAR(100),
    review_text         TEXT,
    rating              INTEGER,
    moderation_status   VARCHAR(50),
    flags               TEXT[],
    submitted_at        TIMESTAMP,
    loaded_at           TIMESTAMP DEFAULT NOW()
);


CREATE TABLE IF NOT EXISTS showcase_user_activity (
    user_id                 VARCHAR(100),
    total_sessions          INTEGER,
    total_time_minutes      NUMERIC(10,2),
    avg_session_minutes     NUMERIC(10,2),
    unique_pages_visited    INTEGER,
    total_actions           INTEGER,
    most_used_device        VARCHAR(100),
    analysis_date           DATE DEFAULT CURRENT_DATE,
    updated_at              TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (user_id, analysis_date)
);

CREATE TABLE IF NOT EXISTS showcase_support_efficiency (
    issue_type              VARCHAR(100),
    status                  VARCHAR(50),
    total_tickets           INTEGER,
    avg_resolution_hours    NUMERIC(10,2),
    max_resolution_hours    NUMERIC(10,2),
    open_tickets            INTEGER,
    analysis_date           DATE DEFAULT CURRENT_DATE,
    updated_at              TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (issue_type, status, analysis_date)
);

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO airflow;
GRANT ALL PRIVILEGES ON DATABASE etl_db TO airflow;