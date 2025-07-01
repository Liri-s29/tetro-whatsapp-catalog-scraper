#!/usr/bin/env python3
"""
Algolia Indexer for WhatsApp Scraper
Indexes scraped catalog data to Algolia for search functionality
"""

import os
import json
import sys
from datetime import datetime
from algoliasearch.search.client import SearchClientSync

# Algolia configuration
ALGOLIA_APP_ID = 'RG9CP54HCJ'
ALGOLIA_API_KEY = 'e4c91dec494701448ebf43e69d797811'
INDEX_NAME = 'whatsapp_catalog'

def create_algolia_client():
    """Create and return Algolia search client"""
    try:
        client = SearchClientSync(ALGOLIA_APP_ID, ALGOLIA_API_KEY)
        return client
    except Exception as e:
        print(f"❌ Failed to create Algolia client: {e}")
        return None

def transform_product_for_algolia(product, seller_data, scrape_job_data):
    """Transform a product from Supabase format to Algolia format"""
    
    # Extract metadata if it exists
    metadata = product.get('metadata', {})
    
    # Create Algolia-optimized object
    algolia_product = {
        'objectID': product['id'],  # Use product ID as Algolia objectID
        
        # Product information
        'title': product['title'],
        'price': product['price'],
        'description': product['description'],
        'product_link': product.get('product_link'),
        'photo_count': metadata.get('photo_count', 0),
        
        # Seller information (from seller_data, not metadata to avoid duplication)
        'seller_id': product['seller_id'],
        'seller_name': seller_data.get('name', ''),
        'seller_city': seller_data.get('city', ''),
        'seller_contact': seller_data.get('contact', ''),
        'catalogue_url': seller_data.get('catalogue_url', ''),
        
        # Scraping metadata
        'scraped_at': metadata.get('scraped_at', product.get('scraped_at')),
        
        # Search-optimized fields
        'searchable_text': f"{product['title']} {product['description']} {seller_data.get('name', '')} {seller_data.get('city', '')}",
    }
    
    # Clean up None values to reduce index size
    algolia_product = {k: v for k, v in algolia_product.items() if v is not None}
    
    return algolia_product

def index_to_algolia(json_file, clear_index=True):
    """Index products from Supabase JSON to Algolia"""
    
    if not os.path.exists(json_file):
        print(f"❌ File not found: {json_file}")
        return False
    
    try:
        # Load JSON data
        print(f"📄 Loading data from {json_file}...")
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Validate data structure
        if 'products' not in data or 'sellers' not in data or 'scrape_job' not in data:
            print("❌ Invalid JSON structure. Expected 'products', 'sellers', and 'scrape_job' keys.")
            return False
        
        products = data['products']
        sellers = data['sellers']
        scrape_job = data['scrape_job']
        
        print(f"📊 Data summary:")
        print(f"   - Products: {len(products)}")
        print(f"   - Sellers: {len(sellers)}")
        print(f"   - Scrape Job: {scrape_job['id']}")
        
        # Create Algolia client
        client = create_algolia_client()
        if not client:
            return False
        
        # Clear index if requested
        if clear_index:
            print(f"🧹 Clearing index '{INDEX_NAME}'...")
            client.clear_objects(index_name=INDEX_NAME)
        
        # Transform products for Algolia
        print(f"🔄 Transforming {len(products)} products for Algolia...")
        algolia_products = []
        
        for product in products:
            # Find corresponding seller data
            seller_data = sellers.get(product['seller_id'], {})
            
            # If seller not found by ID, try to find by matching seller_id in sellers dict
            if not seller_data:
                for seller_key, seller_info in sellers.items():
                    if seller_info.get('id') == product['seller_id']:
                        seller_data = seller_info
                        break
            
            # Transform product
            algolia_product = transform_product_for_algolia(product, seller_data, scrape_job)
            algolia_products.append(algolia_product)
        
        # Index products to Algolia
        if algolia_products:
            print(f"🚀 Indexing {len(algolia_products)} products to Algolia...")
            
            # Index in batches for better performance
            batch_size = 100
            for i in range(0, len(algolia_products), batch_size):
                batch = algolia_products[i:i + batch_size]
                response = client.save_objects(index_name=INDEX_NAME, objects=batch)
                print(f"📦 Indexed batch {i//batch_size + 1}: {len(batch)} products")
            
            print(f"✅ Successfully indexed {len(algolia_products)} products to Algolia!")
            
            # Configure search settings
            print(f"⚙️ Configuring search settings...")
            settings = {
                'searchableAttributes': [
                    'title',
                    'description', 
                    'seller_name',
                    'seller_city',
                    'searchable_text'
                ],
                'attributesForFaceting': [
                    'seller_name',
                    'seller_city',
                    'is_out_of_stock',
                    'is_removed',
                    'photo_count'
                ],
                'customRanking': [
                    'desc(scraped_at)',
                    'desc(photo_count)',
                    'asc(is_out_of_stock)'
                ],
                'attributesToRetrieve': [
                    'title',
                    'price', 
                    'description',
                    'product_link',
                    'seller_name',
                    'seller_city',
                    'seller_contact',
                    'catalogue_url',
                    'photo_count',
                ]
            }
            client.set_settings(index_name=INDEX_NAME, index_settings=settings)
            
            print(f"🎉 Algolia indexing completed!")
            print(f"🔍 Search available at: https://{ALGOLIA_APP_ID}-dsn.algolia.net/1/indexes/{INDEX_NAME}/query")
            
            return True
        else:
            print("⚠️ No products to index")
            return True
            
    except json.JSONDecodeError as e:
        print(f"❌ Invalid JSON format: {e}")
        return False
    except Exception as e:
        print(f"❌ Error during Algolia indexing: {e}")
        return False

def main():
    """Main function for CLI usage"""
    if len(sys.argv) < 2:
        print("📖 Usage:")
        print("  python algolia_indexer.py <json_file>      # Index products from JSON")
        print("")
        print("🔧 Environment Variables:")
        print("  ALGOLIA_APP_ID     # Your Algolia App ID")
        print("  ALGOLIA_API_KEY    # Your Algolia Admin API Key")
        print("  ALGOLIA_INDEX_NAME # Index name (default: whatsapp_products)")
        return
    
    json_file = sys.argv[1]
    
    print("🔍 WhatsApp Scraper - Algolia Indexer")
    print("=" * 50)
    
    success = index_to_algolia(json_file)
    
    if success:
        print("\n✅ Algolia indexing completed successfully!")
    else:
        print("\n❌ Algolia indexing failed")
        sys.exit(1)

if __name__ == "__main__":
    main() 