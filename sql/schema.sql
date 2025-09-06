-- SCSK Pricing ETL Database Schema
-- This file contains all table creation scripts for the pricing ETL pipeline

-- Drop table if exists (for clean deployments)
IF EXISTS (SELECT * FROM sysobjects WHERE name='products' AND xtype='U')
    DROP TABLE dbo.products;

-- Create products table
CREATE TABLE dbo.products (
    product_id INT PRIMARY KEY,
    title NVARCHAR(255) NOT NULL,
    price_usd DECIMAL(10,2) NOT NULL,
    price_gbp DECIMAL(10,2) NOT NULL,
    description NVARCHAR(MAX),
    category_name NVARCHAR(100),
    rating DECIMAL(3,2),
    rating_count INT,
    expensive BIT NOT NULL DEFAULT 0,
    price_band NVARCHAR(20),
    processing_date DATE NOT NULL,
    exchange_rate_used DECIMAL(10,6) NOT NULL,
    ingested_at DATETIME2 NOT NULL,
    created_at DATETIME2 NOT NULL DEFAULT GETDATE()
);

-- Add table description
EXEC sp_addextendedproperty 
    @name = N'MS_Description', 
    @value = N'Product pricing data with USD to GBP conversion and business logic classifications',
    @level0type = N'SCHEMA', @level0name = N'dbo',
    @level1type = N'TABLE', @level1name = N'products';

-- Add column descriptions
EXEC sp_addextendedproperty @name = N'MS_Description', @value = N'Unique product identifier', @level0type = N'SCHEMA', @level0name = N'dbo', @level1type = N'TABLE', @level1name = N'products', @level2type = N'COLUMN', @level2name = N'product_id';
EXEC sp_addextendedproperty @name = N'MS_Description', @value = N'Product title/name', @level0type = N'SCHEMA', @level0name = N'dbo', @level1type = N'TABLE', @level1name = N'products', @level2type = N'COLUMN', @level2name = N'title';
EXEC sp_addextendedproperty @name = N'MS_Description', @value = N'Original price in USD', @level0type = N'SCHEMA', @level0name = N'dbo', @level1type = N'TABLE', @level1name = N'products', @level2type = N'COLUMN', @level2name = N'price_usd';
EXEC sp_addextendedproperty @name = N'MS_Description', @value = N'Converted price in GBP', @level0type = N'SCHEMA', @level0name = N'dbo', @level1type = N'TABLE', @level1name = N'products', @level2type = N'COLUMN', @level2name = N'price_gbp';
EXEC sp_addextendedproperty @name = N'MS_Description', @value = N'Product description', @level0type = N'SCHEMA', @level0name = N'dbo', @level1type = N'TABLE', @level1name = N'products', @level2type = N'COLUMN', @level2name = N'description';
EXEC sp_addextendedproperty @name = N'MS_Description', @value = N'Product category', @level0type = N'SCHEMA', @level0name = N'dbo', @level1type = N'TABLE', @level1name = N'products', @level2type = N'COLUMN', @level2name = N'category_name';
EXEC sp_addextendedproperty @name = N'MS_Description', @value = N'Product rating (0-5 scale)', @level0type = N'SCHEMA', @level0name = N'dbo', @level1type = N'TABLE', @level1name = N'products', @level2type = N'COLUMN', @level2name = N'rating';
EXEC sp_addextendedproperty @name = N'MS_Description', @value = N'Number of ratings', @level0type = N'SCHEMA', @level0name = N'dbo', @level1type = N'TABLE', @level1name = N'products', @level2type = N'COLUMN', @level2name = N'rating_count';
EXEC sp_addextendedproperty @name = N'MS_Description', @value = N'Flag indicating if product is expensive (>100 GBP)', @level0type = N'SCHEMA', @level0name = N'dbo', @level1type = N'TABLE', @level1name = N'products', @level2type = N'COLUMN', @level2name = N'expensive';
EXEC sp_addextendedproperty @name = N'MS_Description', @value = N'Price band classification (budget/mid/premium)', @level0type = N'SCHEMA', @level0name = N'dbo', @level1type = N'TABLE', @level1name = N'products', @level2type = N'COLUMN', @level2name = N'price_band';
EXEC sp_addextendedproperty @name = N'MS_Description', @value = N'Date when data was processed', @level0type = N'SCHEMA', @level0name = N'dbo', @level1type = N'TABLE', @level1name = N'products', @level2type = N'COLUMN', @level2name = N'processing_date';
EXEC sp_addextendedproperty @name = N'MS_Description', @value = N'USD to GBP exchange rate used for conversion', @level0type = N'SCHEMA', @level0name = N'dbo', @level1type = N'TABLE', @level1name = N'products', @level2type = N'COLUMN', @level2name = N'exchange_rate_used';
EXEC sp_addextendedproperty @name = N'MS_Description', @value = N'Timestamp when data was ingested into pipeline', @level0type = N'SCHEMA', @level0name = N'dbo', @level1type = N'TABLE', @level1name = N'products', @level2type = N'COLUMN', @level2name = N'ingested_at';
EXEC sp_addextendedproperty @name = N'MS_Description', @value = N'Timestamp when record was created in database', @level0type = N'SCHEMA', @level0name = N'dbo', @level1type = N'TABLE', @level1name = N'products', @level2type = N'COLUMN', @level2name = N'created_at';

-- Add check constraints for data quality
ALTER TABLE dbo.products ADD CONSTRAINT CHK_products_price_usd_positive CHECK (price_usd >= 0);
ALTER TABLE dbo.products ADD CONSTRAINT CHK_products_price_gbp_positive CHECK (price_gbp >= 0);
ALTER TABLE dbo.products ADD CONSTRAINT CHK_products_rating_range CHECK (rating >= 0 AND rating <= 5);
ALTER TABLE dbo.products ADD CONSTRAINT CHK_products_rating_count_positive CHECK (rating_count >= 0);
ALTER TABLE dbo.products ADD CONSTRAINT CHK_products_exchange_rate_positive CHECK (exchange_rate_used > 0);
ALTER TABLE dbo.products ADD CONSTRAINT CHK_products_price_band_values CHECK (price_band IN ('budget', 'mid', 'premium'));

PRINT 'Products table created successfully with constraints and documentation';