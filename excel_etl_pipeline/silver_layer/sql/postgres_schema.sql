-- postgres_schema.sql
-- PostgreSQL Schema for Silver Layer

-- Create schema
CREATE SCHEMA IF NOT EXISTS silver_layer;

-- =====================================================
-- INVOICE SUMMARY TABLE
-- =====================================================
CREATE TABLE IF NOT EXISTS silver_layer.cash_invoice_summary (
    -- Primary Key
    invoice_number VARCHAR(50) PRIMARY KEY,
    
    -- Transaction Information
    transaction_date DATE NOT NULL,
    transaction_time TIME,
    transaction_year INTEGER,
    transaction_month INTEGER,
    transaction_day INTEGER,
    transaction_hour INTEGER,
    transaction_day_of_week VARCHAR(20),
    
    -- Customer Information
    customer_code VARCHAR(50) NOT NULL,
    customer_name VARCHAR(200),
    
    -- Amount Fields
    total_amount DECIMAL(15,2) NOT NULL,
    amount_including_tax DECIMAL(15,2),
    amount_excluding_tax DECIMAL(15,2),
    tax_amount DECIMAL(15,2),
    discount_amount DECIMAL(15,2),
    net_amount DECIMAL(15,2),
    gross_amount DECIMAL(15,2),
    
    -- Calculated Percentages
    discount_percentage DECIMAL(5,2),
    tax_percentage DECIMAL(5,2),
    
    -- Branch Information
    branch_code INTEGER,
    branch_name VARCHAR(100),
    processed_by_user VARCHAR(100),
    payment_method_reference VARCHAR(50),
    
    -- Contact Information
    contact_telephone VARCHAR(200),
    contact_email VARCHAR(200),
    company_name VARCHAR(200),
    company_address VARCHAR(300),
    
    -- Business Flags
    has_discount BOOLEAN,
    has_tax BOOLEAN,
    is_cash_customer BOOLEAN,
    is_high_value_transaction BOOLEAN,
    
    -- Data Quality Flags
    amount_invalid_flag INTEGER DEFAULT 0,
    date_invalid_flag INTEGER DEFAULT 0,
    customer_missing_flag INTEGER DEFAULT 0,
    
    -- Bronze Metadata
    _bronze_source_file_path TEXT,
    _raw_processed_at TIMESTAMP,
    _bronze_data_hash VARCHAR(64),
    _bronze_ingestion_date DATE,
    
    -- Silver Metadata
    _silver_processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    _silver_batch_id VARCHAR(100),
    _silver_valid_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    _silver_valid_to TIMESTAMP,
    _silver_is_current BOOLEAN DEFAULT TRUE,
    _silver_record_version INTEGER DEFAULT 1,
    _silver_data_quality_score DECIMAL(5,2),
    _silver_processing_duration_ms INTEGER,
    
    -- Original Values (for audit)
    original_document_number VARCHAR(50),
    original_cus_code VARCHAR(50),
    original_customer_name VARCHAR(200),
    
    -- Indexes
    CONSTRAINT chk_positive_amount CHECK (total_amount > 0)
);

-- Indexes for invoice_summary
CREATE INDEX idx_invoice_transaction_date ON silver_layer.cash_invoice_summary(transaction_date);
CREATE INDEX idx_invoice_customer_code ON silver_layer.cash_invoice_summary(customer_code);
CREATE INDEX idx_invoice_branch ON silver_layer.cash_invoice_summary(branch_code, branch_name);
CREATE INDEX idx_invoice_user ON silver_layer.cash_invoice_summary(processed_by_user);
CREATE INDEX idx_invoice_current ON silver_layer.cash_invoice_summary(_silver_is_current) WHERE _silver_is_current = TRUE;

-- =====================================================
-- INVOICE DETAIL TABLE
-- =====================================================
CREATE TABLE IF NOT EXISTS silver_layer.cash_invoice_detail (
    -- Composite Primary Key
    invoice_line_id SERIAL PRIMARY KEY,
    invoice_number VARCHAR(50) NOT NULL,
    product_code VARCHAR(50) NOT NULL,
    
    -- Transaction Information
    transaction_date DATE NOT NULL,
    transaction_time TIME,
    transaction_year INTEGER,
    transaction_month INTEGER,
    
    -- Customer Information
    customer_code VARCHAR(50) NOT NULL,
    customer_name VARCHAR(200),
    
    -- Product Information
    product_description VARCHAR(500),
    quantity_sold DECIMAL(10,2) NOT NULL,
    unit_price DECIMAL(15,2) NOT NULL,
    
    -- Line Amounts
    line_total_amount DECIMAL(15,2) NOT NULL,
    line_amount_excluding_tax DECIMAL(15,2),
    line_tax_amount DECIMAL(15,2),
    line_discount_amount DECIMAL(15,2),
    line_discount_percentage DECIMAL(5,2),
    
    -- Calculated Fields
    calculated_line_total DECIMAL(15,2),
    net_line_amount DECIMAL(15,2),
    amount_variance DECIMAL(15,2),
    has_amount_variance BOOLEAN,
    
    -- Product Details
    bonus_quantity DECIMAL(10,2),
    pack_wholesale_quantity DECIMAL(10,2),
    pack_wholesale_indicator VARCHAR(10),
    transaction_type VARCHAR(20),
    product_category VARCHAR(50),
    is_high_value_item BOOLEAN,
    is_bulk_order BOOLEAN,
    
    -- Branch Information
    branch_code INTEGER,
    branch_name VARCHAR(100),
    processed_by_user VARCHAR(100),
    
    -- Data Quality Flags
    amount_invalid_flag INTEGER DEFAULT 0,
    date_invalid_flag INTEGER DEFAULT 0,
    customer_missing_flag INTEGER DEFAULT 0,
    quantity_invalid_flag INTEGER DEFAULT 0,
    price_invalid_flag INTEGER DEFAULT 0,
    
    -- Bronze Metadata
    _bronze_source_file_path TEXT,
    _raw_processed_at TIMESTAMP,
    _bronze_data_hash VARCHAR(64),
    _bronze_ingestion_date DATE,
    
    -- Silver Metadata
    _silver_processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    _silver_batch_id VARCHAR(100),
    _silver_valid_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    _silver_valid_to TIMESTAMP,
    _silver_is_current BOOLEAN DEFAULT TRUE,
    _silver_record_version INTEGER DEFAULT 1,
    _silver_data_quality_score DECIMAL(5,2),
    _silver_processing_duration_ms INTEGER,
    
    -- Original Values
    original_document_number VARCHAR(50),
    original_item_code VARCHAR(50),
    
    -- Foreign Key
    CONSTRAINT fk_invoice FOREIGN KEY (invoice_number) 
        REFERENCES silver_layer.cash_invoice_summary(invoice_number),
    
    -- Constraints
    CONSTRAINT chk_positive_quantity CHECK (quantity_sold > 0),
    CONSTRAINT chk_positive_price CHECK (unit_price > 0),
    CONSTRAINT uq_invoice_product UNIQUE (invoice_number, product_code, transaction_date)
);

-- Indexes for invoice_detail
CREATE INDEX idx_detail_invoice ON silver_layer.cash_invoice_detail(invoice_number);
CREATE INDEX idx_detail_product ON silver_layer.cash_invoice_detail(product_code);
CREATE INDEX idx_detail_date ON silver_layer.cash_invoice_detail(transaction_date);
CREATE INDEX idx_detail_customer ON silver_layer.cash_invoice_detail(customer_code);

-- =====================================================
-- CREDIT NOTE SUMMARY TABLE
-- =====================================================
CREATE TABLE IF NOT EXISTS silver_layer.credit_note_summary (
    -- Primary Key
    credit_note_number VARCHAR(50) PRIMARY KEY,
    
    -- Transaction Information
    transaction_date DATE NOT NULL,
    transaction_time TIME,
    transaction_year INTEGER,
    transaction_month INTEGER,
    
    -- Link to Original Invoice
    original_invoice_number VARCHAR(50) NOT NULL,
    
    -- Customer Information
    customer_code VARCHAR(50) NOT NULL,
    customer_name VARCHAR(200),
    
    -- Credit Amounts (stored as negative)
    credit_amount DECIMAL(15,2) NOT NULL,
    credit_amount_including_tax DECIMAL(15,2),
    credit_amount_excluding_tax DECIMAL(15,2),
    credit_tax_amount DECIMAL(15,2),
    credit_discount_amount DECIMAL(15,2),
    
    -- Credit Note Details
    credit_reason_code VARCHAR(50),
    credit_reason_description VARCHAR(500),
    approved_by_user VARCHAR(100),
    approval_date TIMESTAMP,
    refund_method VARCHAR(50),
    refund_status VARCHAR(50),
    
    -- Credit Analysis
    days_since_original_invoice INTEGER,
    refund_percentage DECIMAL(5,2),
    is_full_refund BOOLEAN,
    credit_type VARCHAR(50),
    
    -- Branch Information
    branch_code INTEGER,
    branch_name VARCHAR(100),
    processed_by_user VARCHAR(100),
    
    -- Bronze Metadata
    _bronze_source_file_path TEXT,
    _raw_processed_at TIMESTAMP,
    _bronze_data_hash VARCHAR(64),
    _bronze_ingestion_date DATE,
    
    -- Silver Metadata
    _silver_processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    _silver_batch_id VARCHAR(100),
    _silver_valid_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    _silver_valid_to TIMESTAMP,
    _silver_is_current BOOLEAN DEFAULT TRUE,
    _silver_record_version INTEGER DEFAULT 1,
    _silver_data_quality_score DECIMAL(5,2),
    
    -- Foreign Key
    CONSTRAINT fk_original_invoice FOREIGN KEY (original_invoice_number) 
        REFERENCES silver_layer.cash_invoice_summary(invoice_number),
    
    -- Constraints
    CONSTRAINT chk_negative_credit CHECK (credit_amount < 0)
);

-- Indexes for credit_note_summary
CREATE INDEX idx_credit_date ON silver_layer.credit_note_summary(transaction_date);
CREATE INDEX idx_credit_invoice ON silver_layer.credit_note_summary(original_invoice_number);
CREATE INDEX idx_credit_customer ON silver_layer.credit_note_summary(customer_code);

-- =====================================================
-- CREDIT NOTE DETAIL TABLE
-- =====================================================
CREATE TABLE IF NOT EXISTS silver_layer.credit_note_detail (
    -- Primary Key
    credit_line_id SERIAL PRIMARY KEY,
    credit_note_number VARCHAR(50) NOT NULL,
    product_code VARCHAR(50) NOT NULL,
    
    -- Transaction Information
    transaction_date DATE NOT NULL,
    transaction_time TIME,
    
    -- Link to Original Invoice
    original_invoice_number VARCHAR(50),
    
    -- Customer Information
    customer_code VARCHAR(50) NOT NULL,
    customer_name VARCHAR(200),
    
    -- Product Information
    product_description VARCHAR(500),
    quantity_returned DECIMAL(10,2),  -- Can be negative
    unit_price DECIMAL(15,2),
    
    -- Credit Line Amounts (negative)
    line_credit_amount DECIMAL(15,2),
    line_credit_tax_amount DECIMAL(15,2),
    line_credit_discount_amount DECIMAL(15,2),
    
    -- Return Details
    return_reason VARCHAR(500),
    return_to_stock_flag BOOLEAN,
    product_condition VARCHAR(50),
    
    -- Calculated Fields
    calculated_credit_line_total DECIMAL(15,2),
    
    -- Branch Information
    branch_code INTEGER,
    branch_name VARCHAR(100),
    
    -- Bronze Metadata
    _bronze_source_file_path TEXT,
    _raw_processed_at TIMESTAMP,
    _bronze_data_hash VARCHAR(64),
    
    -- Silver Metadata
    _silver_processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    _silver_batch_id VARCHAR(100),
    _silver_data_quality_score DECIMAL(5,2),
    
    -- Foreign Key
    CONSTRAINT fk_credit_note FOREIGN KEY (credit_note_number) 
        REFERENCES silver_layer.credit_note_summary(credit_note_number),
    
    CONSTRAINT uq_credit_product UNIQUE (credit_note_number, product_code)
);

-- Indexes for credit_note_detail
CREATE INDEX idx_credit_detail_note ON silver_layer.credit_note_detail(credit_note_number);
CREATE INDEX idx_credit_detail_product ON silver_layer.credit_note_detail(product_code);

-- =====================================================
-- CUSTOMER DIMENSION (SCD Type 2)
-- =====================================================
CREATE TABLE IF NOT EXISTS silver_layer.customer_dimension (
    customer_key SERIAL PRIMARY KEY,
    customer_code VARCHAR(50) NOT NULL,
    customer_name VARCHAR(200),
    contact_telephone VARCHAR(200),
    contact_email VARCHAR(200),
    
    -- SCD Type 2 Fields
    _valid_from TIMESTAMP NOT NULL,
    _valid_to TIMESTAMP,
    _is_current BOOLEAN DEFAULT TRUE,
    _record_version INTEGER DEFAULT 1,
    
    -- Metadata
    _created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX idx_customer_current ON silver_layer.customer_dimension(customer_code, _is_current) 
WHERE _is_current = TRUE;

-- =====================================================
-- DATA QUALITY LOG
-- =====================================================
CREATE TABLE IF NOT EXISTS silver_layer.data_quality_log (
    log_id SERIAL PRIMARY KEY,
    process_id VARCHAR(100),
    table_name VARCHAR(100),
    column_name VARCHAR(100),
    issue_type VARCHAR(50),
    issue_description TEXT,
    affected_rows INTEGER,
    sample_data TEXT,
    severity VARCHAR(20),
    logged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dq_process ON silver_layer.data_quality_log(process_id);
CREATE INDEX idx_dq_table ON silver_layer.data_quality_log(table_name);

-- =====================================================
-- PROCESS LOG
-- =====================================================
CREATE TABLE IF NOT EXISTS silver_layer.process_log (
    process_id VARCHAR(100) PRIMARY KEY,
    source_table VARCHAR(200),
    target_table VARCHAR(200),
    records_processed INTEGER,
    records_valid INTEGER,
    records_invalid INTEGER,
    credit_notes_separated INTEGER,
    potential_duplicates_detected INTEGER,
    whitespace_issues_found INTEGER,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    status VARCHAR(50),
    error_message TEXT
);

CREATE INDEX idx_process_status ON silver_layer.process_log(status);
CREATE INDEX idx_process_date ON silver_layer.process_log(started_at);

-- =====================================================
-- RECONCILIATION REPORT
-- =====================================================
CREATE TABLE IF NOT EXISTS silver_layer.reconciliation_report (
    reconciliation_id SERIAL PRIMARY KEY,
    report_type VARCHAR(50),  -- 'INVOICE' or 'CREDIT_NOTE'
    invoice_number VARCHAR(50),
    has_header BOOLEAN,
    has_details BOOLEAN,
    header_amount DECIMAL(15,2),
    detail_total DECIMAL(15,2),
    variance DECIMAL(15,2),
    has_variance BOOLEAN,
    reconciliation_status VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_recon_invoice ON silver_layer.reconciliation_report(invoice_number);
CREATE INDEX idx_recon_status ON silver_layer.reconciliation_report(reconciliation_status);