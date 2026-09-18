CREATE DATABASE LUMI;

CREATE TABLE customers (
  customer_id          VARCHAR(7)   NOT NULL,   -- C###### (exactly 7)
  first_name           VARCHAR(15)  NOT NULL,   -- min 3
  last_name            VARCHAR(15),
  email                VARCHAR(30),             -- min 13 when present
  phone_number_enc     VARCHAR(255),            -- ENCRYPTED
  signup_date          VARCHAR(10),             -- YYYY-MM-DD text
  segment              VARCHAR(20),
  occupation           VARCHAR(30),
  annual_spend_enc     VARCHAR(255),            -- ENCRYPTED
  currency             VARCHAR(3),
  account_status       VARCHAR(13),
  referred_by          VARCHAR(7),
  is_active            VARCHAR(5),              -- "true" / "false"
  skills               VARCHAR(100),            -- joined list, "a, b, c"
  address_street       VARCHAR(60),
  address_city         VARCHAR(30),
  address_state        VARCHAR(20),
  address_postal_code  VARCHAR(6),
  address_country      VARCHAR(20),
  emergency_name       VARCHAR(30),
  emergency_relationship VARCHAR(20),
  emergency_phone_enc  VARCHAR(255),            -- ENCRYPTED
  emergency_email      VARCHAR(30),

  ingestion_timestamp   DATETIME(3) NOT NULL,
  execution_id          VARCHAR(36) NOT NULL,
  source_creation_time  DATETIME(3) NOT NULL,

  INDEX idx_execution (execution_id),
  INDEX idx_customer  (customer_id)
);