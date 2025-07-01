-- WhatsApp Scraper Seed Data
-- This file contains sample data for testing and development

-- Insert sample sellers for testing
INSERT INTO sellers (name, city, contact, catalogue_url) VALUES
('Test Seller 1', 'Mumbai', '9876543210', 'https://wa.me/c/919876543210'),
('Test Seller 2', 'Delhi', '9876543211', 'https://wa.me/c/919876543211'),
('Sample Electronics Store', 'Bangalore', '9876543212', 'https://wa.me/c/919876543212'),
('Fashion Hub', 'Chennai', '9876543213', 'https://wa.me/c/919876543213')
ON CONFLICT (name) DO NOTHING;

-- Create initial scrape job for testing
INSERT INTO scrape_jobs (status, job_metadata) VALUES
('pending', '{"note": "Initial setup job"}')
ON CONFLICT DO NOTHING; 