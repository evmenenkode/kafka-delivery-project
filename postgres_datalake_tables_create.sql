CREATE SCHEMA delivery_app_info;

CREATE TABLE delvivery_app_info.users (
    user_id VARCHAR PRIMARY KEY,
    first_name VARCHAR,
    last_name VARCHAR,
    email VARCHAR,
    country VARCHAR,
    created_at TIMESTAMP
);

CREATE TABLE delvivery_app_info.products (
    product_id VARCHAR PRIMARY KEY,
    product_name VARCHAR,
    category VARCHAR,
    price NUMERIC,
    created_at TIMESTAMP
);