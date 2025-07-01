-- WhatsApp Scraper Database Schema Migration
-- Complete initial schema with product deduplication and lifecycle tracking

-- Enable necessary extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Sellers table
CREATE TABLE sellers (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255) NOT NULL UNIQUE,
    city VARCHAR(255),
    contact TEXT,
    catalogue_url TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT true
);

-- Scrape jobs table for tracking scraping sessions
CREATE TABLE scrape_jobs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    status VARCHAR(50) NOT NULL DEFAULT 'pending', -- pending, running, completed, failed
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    total_items INTEGER DEFAULT 0,
    total_sellers INTEGER DEFAULT 0,
    error_message TEXT,
    job_metadata JSONB, -- Store additional data like timings, etc.
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Products table for storing all scraped product data
CREATE TABLE products (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    scrape_job_id UUID REFERENCES scrape_jobs(id) ON DELETE CASCADE,
    seller_id UUID REFERENCES sellers(id) ON DELETE CASCADE,
    
    -- Product details
    title TEXT NOT NULL,
    price VARCHAR(50),
    description TEXT,
    product_link TEXT,
    is_out_of_stock BOOLEAN DEFAULT false,
    
    -- Product lifecycle tracking
    photo_count INTEGER DEFAULT 0,
    scraped_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    last_seen_scrape_job_id UUID REFERENCES scrape_jobs(id),
    is_removed BOOLEAN DEFAULT false,
    removed_at TIMESTAMP WITH TIME ZONE,
    
    -- Metadata stored as JSONB for flexibility
    metadata JSONB,
    
    -- For future image storage
    images JSONB, -- Store image data as JSON
    
    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX idx_products_seller_id ON products(seller_id);
CREATE INDEX idx_products_scrape_job_id ON products(scrape_job_id);
CREATE INDEX idx_products_scraped_at ON products(scraped_at);
CREATE INDEX idx_products_last_seen_scrape_job ON products(last_seen_scrape_job_id);
CREATE INDEX idx_products_is_removed ON products(is_removed);
CREATE INDEX idx_products_title ON products USING gin(to_tsvector('english', title));
CREATE INDEX idx_sellers_name ON sellers(name);
CREATE INDEX idx_scrape_jobs_status ON scrape_jobs(status);
CREATE INDEX idx_scrape_jobs_created_at ON scrape_jobs(created_at);

-- Add unique constraint on product_link to prevent duplicates
CREATE UNIQUE INDEX idx_products_unique_link ON products(product_link) WHERE product_link IS NOT NULL;

-- Function to update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Triggers to automatically update updated_at timestamps
CREATE TRIGGER update_sellers_updated_at BEFORE UPDATE ON sellers
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_products_updated_at BEFORE UPDATE ON products
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Function to mark products as removed when they're not in the latest scrape
CREATE OR REPLACE FUNCTION mark_missing_products_as_removed(
    seller_ids UUID[],
    current_scrape_job_id UUID,
    current_product_links TEXT[]
)
RETURNS TABLE(
    products_marked_removed INTEGER,
    newly_removed_count INTEGER
) AS $$
DECLARE
    products_marked INTEGER := 0;
    newly_removed INTEGER := 0;
BEGIN
    -- Mark products as removed if they:
    -- 1. Belong to sellers that were just scraped
    -- 2. Were not removed before
    -- 3. Are not in the current scrape's product list
    WITH removed_products AS (
        UPDATE products 
        SET 
            is_removed = true,
            removed_at = CURRENT_TIMESTAMP,
            updated_at = CURRENT_TIMESTAMP
        WHERE 
            seller_id = ANY(seller_ids)
            AND is_removed = false
            AND (
                product_link IS NULL 
                OR product_link NOT IN (SELECT unnest(current_product_links))
            )
        RETURNING id, (removed_at IS NULL) as was_newly_removed
    )
    SELECT 
        COUNT(*)::INTEGER,
        COUNT(*) FILTER (WHERE was_newly_removed)::INTEGER
    INTO products_marked, newly_removed
    FROM removed_products;
    
    RETURN QUERY SELECT products_marked, newly_removed;
END;
$$ LANGUAGE plpgsql;

-- Function to mark products as active again if they reappear
CREATE OR REPLACE FUNCTION mark_reappeared_products_as_active(
    current_scrape_job_id UUID,
    current_product_links TEXT[]
)
RETURNS INTEGER AS $$
DECLARE
    products_reactivated INTEGER := 0;
BEGIN
    -- Mark previously removed products as active again if they reappear
    WITH reactivated_products AS (
        UPDATE products 
        SET 
            is_removed = false,
            removed_at = NULL,
            last_seen_scrape_job_id = current_scrape_job_id,
            updated_at = CURRENT_TIMESTAMP
        WHERE 
            is_removed = true
            AND product_link IS NOT NULL
            AND product_link = ANY(current_product_links)
        RETURNING id
    )
    SELECT COUNT(*)::INTEGER INTO products_reactivated FROM reactivated_products;
    
    RETURN products_reactivated;
END;
$$ LANGUAGE plpgsql;

-- Create a view for active products only (excludes removed products)
CREATE VIEW active_products AS
SELECT *
FROM products
WHERE is_removed = false OR is_removed IS NULL;

-- Create a view for latest products per seller
CREATE VIEW latest_products_per_seller AS
SELECT DISTINCT ON (seller_id, title) 
    p.*,
    s.name as seller_name_from_table,
    sj.started_at as scrape_session_time
FROM products p
JOIN sellers s ON p.seller_id = s.id
JOIN scrape_jobs sj ON p.scrape_job_id = sj.id
ORDER BY seller_id, title, p.updated_at DESC;

-- Create a view for scraping statistics
CREATE VIEW scraping_stats AS
SELECT 
    DATE(sj.started_at) as scrape_date,
    COUNT(*) as total_jobs,
    SUM(sj.total_items) as total_items,
    SUM(sj.total_sellers) as total_sellers,
    AVG(EXTRACT(EPOCH FROM (sj.completed_at - sj.started_at))/60) as avg_duration_minutes,
    COUNT(CASE WHEN sj.status = 'completed' THEN 1 END) as successful_jobs,
    COUNT(CASE WHEN sj.status = 'failed' THEN 1 END) as failed_jobs
FROM scrape_jobs sj
WHERE sj.started_at IS NOT NULL
GROUP BY DATE(sj.started_at)
ORDER BY scrape_date DESC;

-- Create a view for product lifecycle analytics
CREATE VIEW product_lifecycle_stats AS
SELECT 
    s.name as seller_name,
    s.city as seller_city,
    COUNT(*) as total_products_ever,
    COUNT(*) FILTER (WHERE p.is_removed = false OR p.is_removed IS NULL) as active_products,
    COUNT(*) FILTER (WHERE p.is_removed = true) as removed_products,
    COUNT(*) FILTER (WHERE p.removed_at >= CURRENT_DATE - INTERVAL '7 days') as removed_last_7_days,
    COUNT(*) FILTER (WHERE p.created_at >= CURRENT_DATE - INTERVAL '7 days') as added_last_7_days,
    ROUND(
        AVG(EXTRACT(EPOCH FROM (COALESCE(p.removed_at, CURRENT_TIMESTAMP) - p.created_at)) / 86400), 
        2
    ) as avg_product_lifespan_days
FROM sellers s
LEFT JOIN products p ON s.id = p.seller_id
GROUP BY s.id, s.name, s.city
ORDER BY s.name;

-- Enable Row Level Security (RLS) for Supabase
ALTER TABLE sellers ENABLE ROW LEVEL SECURITY;
ALTER TABLE scrape_jobs ENABLE ROW LEVEL SECURITY;
ALTER TABLE products ENABLE ROW LEVEL SECURITY;

-- Create policies for public access (adjust as needed for production)
-- For now, allow all operations for the scraper service
CREATE POLICY "Allow all operations on sellers" ON sellers
    FOR ALL USING (true);

CREATE POLICY "Allow all operations on scrape_jobs" ON scrape_jobs
    FOR ALL USING (true);

CREATE POLICY "Allow all operations on products" ON products
    FOR ALL USING (true);

-- Comments for documentation
COMMENT ON TABLE sellers IS 'Stores WhatsApp seller information and catalog URLs';
COMMENT ON TABLE scrape_jobs IS 'Tracks each scraping session with status and metadata';
COMMENT ON TABLE products IS 'Stores all scraped product data with complete lifecycle tracking';

COMMENT ON COLUMN products.scraped_at IS 'When this product was last successfully scraped from WhatsApp';
COMMENT ON COLUMN products.photo_count IS 'Number of photos this product has in the WhatsApp catalog';
COMMENT ON COLUMN products.last_seen_scrape_job_id IS 'ID of the last scrape job that found this product';
COMMENT ON COLUMN products.is_removed IS 'Whether this product has been removed from the sellers catalog';
COMMENT ON COLUMN products.removed_at IS 'When this product was marked as removed from the catalog';

COMMENT ON FUNCTION mark_missing_products_as_removed IS 'Marks products as removed when they are not found in the latest scrape';
COMMENT ON FUNCTION mark_reappeared_products_as_active IS 'Reactivates products that were previously removed but have reappeared';

COMMENT ON VIEW active_products IS 'Shows only products that are currently active (not removed)';
COMMENT ON VIEW latest_products_per_seller IS 'Shows the most recent version of each product per seller';
COMMENT ON VIEW scraping_stats IS 'Provides daily scraping statistics and performance metrics';
COMMENT ON VIEW product_lifecycle_stats IS 'Analytics view showing product lifecycle statistics per seller';

-- Storage policies for product images
-- Create the product-images bucket if it doesn't exist
INSERT INTO storage.buckets (id, name, public)
VALUES ('product-images', 'product-images', true)
ON CONFLICT (id) DO NOTHING;

-- Allow public uploads to the product-images bucket
CREATE POLICY "Allow public uploads to product-images" ON storage.objects
FOR INSERT WITH CHECK (bucket_id = 'product-images');

-- Allow public reads from the product-images bucket  
CREATE POLICY "Allow public reads from product-images" ON storage.objects
FOR SELECT USING (bucket_id = 'product-images');

-- Allow public updates to the product-images bucket (for upsert operations)
CREATE POLICY "Allow public updates to product-images" ON storage.objects
FOR UPDATE USING (bucket_id = 'product-images');

-- Allow public deletes from the product-images bucket (for cleanup)
CREATE POLICY "Allow public deletes from product-images" ON storage.objects
FOR DELETE USING (bucket_id = 'product-images'); 