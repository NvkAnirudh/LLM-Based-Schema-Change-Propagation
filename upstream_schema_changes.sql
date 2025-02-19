select * from bronze_fraud_transactions;

-- Alter the bronze table to add a new column for device type
ALTER TABLE bronze_fraud_transactions 
ADD COLUMN device_type VARCHAR(50);

-- Update existing records based on the transaction source
UPDATE bronze_fraud_transactions
SET device_type = CASE 
    WHEN transaction_source = 'web' THEN 'Desktop'
    WHEN transaction_source = 'mobile' THEN 'Smartphone'
    WHEN transaction_source = 'ATM' THEN 'Kiosk'
    ELSE 'Unknown'
END;
