CREATE OR REFRESH STREAMING TABLE retail_chain_catalog.silver_schema.silver_customers_scdtype2;

-- 2. Use the STREAM function on your batch Silver table
APPLY CHANGES INTO live.silver_customers_scdtype2
FROM STREAM(retail_chain_catalog.silver_schema.silver_customers_cleaned) 
KEYS (CustomerID)
SEQUENCE BY _updated_at
STORED AS SCD TYPE 2;


CREATE OR REFRESH STREAMING TABLE retail_chain_catalog.silver_schema.silver_transactions_scdtype2;

-- 2. Apply the SCD Type 2 Logic
APPLY CHANGES INTO live.silver_transactions_scdtype2
FROM STREAM(retail_chain_catalog.silver_schema.silver_transactions_cleaned) 
KEYS (TransactionID)                             -- Unique Transaction Key
SEQUENCE BY _updated_at                          -- Chronological order
STORED AS SCD TYPE 2;


CREATE OR REFRESH STREAMING TABLE retail_chain_catalog.silver_schema.silver_accounts_scdtype2;

APPLY CHANGES INTO LIVE.silver_accounts_scdtype2
FROM STREAM(retail_chain_catalog.silver_schema.silver_accounts_cleaned)
KEYS (AccountID)
SEQUENCE BY _updated_at
STORED AS SCD TYPE 2;



CREATE OR REFRESH STREAMING TABLE retail_chain_catalog.silver_schema.silver_loans_scdtype2;

APPLY CHANGES INTO LIVE.silver_loans_scdtype2
FROM STREAM(retail_chain_catalog.silver_schema.silver_loans_cleaned)
KEYS (LoanID)
SEQUENCE BY _updated_at
STORED AS SCD TYPE 2;


CREATE OR REFRESH STREAMING TABLE retail_chain_catalog.silver_schema.silver_address_scdtype2;

APPLY CHANGES INTO LIVE.silver_address_scdtype2
FROM STREAM(retail_chain_catalog.silver_schema.silver_address_cleaned)
KEYS (AddressID)
SEQUENCE BY _updated_at
STORED AS SCD TYPE 2;

CREATE OR REFRESH STREAMING TABLE retail_chain_catalog.silver_schema.silver_branches_scdtype2;

APPLY CHANGES INTO LIVE.silver_branches_scdtype2
FROM STREAM(retail_chain_catalog.silver_schema.silver_branches_cleaned)
KEYS (BranchID)
SEQUENCE BY _updated_at
STORED AS SCD TYPE 2;



