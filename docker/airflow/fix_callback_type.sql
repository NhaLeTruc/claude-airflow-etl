-- ============================================================================
-- Airflow Database Schema Fix
-- ============================================================================
-- Issue: callback_request.callback_type column too short (20 chars)
-- Fix: Increase to 100 chars to support all Airflow callback types
-- ============================================================================

-- Check if the column exists and needs fixing
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'callback_request'
        AND column_name = 'callback_type'
        AND character_maximum_length < 100
    ) THEN
        -- Increase the column size
        ALTER TABLE callback_request
        ALTER COLUMN callback_type TYPE VARCHAR(100);

        RAISE NOTICE 'Successfully increased callback_type column to VARCHAR(100)';
    ELSE
        RAISE NOTICE 'callback_type column already has sufficient length or does not exist';
    END IF;
END $$;
