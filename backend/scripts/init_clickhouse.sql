-- Create the database if it doesn't exist
CREATE DATABASE IF NOT EXISTS uk;

-- Create the table for UK property prices
CREATE TABLE IF NOT EXISTS uk.uk_price_paid
(
    price UInt32,
    date Date,
    postcode1 LowCardinality(String),
    postcode2 LowCardinality(String),
    type Enum8('terraced' = 1, 'semi-detached' = 2, 'detached' = 3, 'flat' = 4, 'other' = 0),
    is_new UInt8,
    duration Enum8('freehold' = 1, 'leasehold' = 2, 'unknown' = 0),
    addr1 String,
    addr2 String,
    street LowCardinality(String),
    locality LowCardinality(String),
    town LowCardinality(String),
    district LowCardinality(String),
    county LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (postcode1, postcode2, addr1, addr2);

-- Insert sample data (small subset for testing)
INSERT INTO uk.uk_price_paid VALUES
(100000, '2023-01-01', 'SW1A', '1AA', 'terraced', 0, 'freehold', '10 Downing Street', '', 'Downing Street', 'Westminster', 'London', 'City of Westminster', 'Greater London'),
(200000, '2023-02-01', 'SW1A', '2AA', 'flat', 1, 'leasehold', 'Flat 1', 'Buckingham Palace', 'The Mall', 'Westminster', 'London', 'City of Westminster', 'Greater London'),
(300000, '2023-03-01', 'SW1A', '3AA', 'detached', 0, 'freehold', '15', 'Whitehall', 'Whitehall', 'Westminster', 'London', 'City of Westminster', 'Greater London'); 