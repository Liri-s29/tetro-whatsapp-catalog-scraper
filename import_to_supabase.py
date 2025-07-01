#!/usr/bin/env python3
"""
Import scraped data from JSON into Supabase
Usage: python import_to_supabase.py scraped_catalog_supabase.json
"""

import json
import sys
import os
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
import psycopg2.extras

# Supabase connection details (adjust as needed)
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://postgres:postgres@127.0.0.1:54322/postgres')

def connect_to_supabase():
    """Connect to Supabase PostgreSQL database"""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except Exception as e:
        print(f"❌ Failed to connect to Supabase: {e}")
        return None

def import_scrape_job(conn, scrape_job_data):
    """Import scrape job data"""
    cursor = conn.cursor()
    
    try:
        query = """
        INSERT INTO scrape_jobs (
            id, status, started_at, completed_at, total_items, 
            total_sellers, error_message, job_metadata
        ) VALUES (
            %(id)s, %(status)s, %(started_at)s, %(completed_at)s, 
            %(total_items)s, %(total_sellers)s, %(error_message)s, %(job_metadata)s
        )
        ON CONFLICT (id) DO UPDATE SET
            status = EXCLUDED.status,
            completed_at = EXCLUDED.completed_at,
            total_items = EXCLUDED.total_items,
            total_sellers = EXCLUDED.total_sellers,
            error_message = EXCLUDED.error_message,
            job_metadata = EXCLUDED.job_metadata
        """
        
        cursor.execute(query, {
            'id': scrape_job_data['id'],
            'status': scrape_job_data['status'],
            'started_at': scrape_job_data['started_at'],
            'completed_at': scrape_job_data.get('completed_at'),
            'total_items': scrape_job_data.get('total_items', 0),
            'total_sellers': scrape_job_data.get('total_sellers', 0),
            'error_message': scrape_job_data.get('error_message'),
            'job_metadata': json.dumps(scrape_job_data.get('job_metadata', {}))
        })
        
        conn.commit()
        print(f"✅ Imported scrape job: {scrape_job_data['id']}")
        return True
        
    except Exception as e:
        print(f"❌ Failed to import scrape job: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()

def import_sellers(conn, sellers_data):
    """Import sellers data"""
    cursor = conn.cursor()
    imported_count = 0
    
    try:
        for seller_key, seller in sellers_data.items():
            query = """
            INSERT INTO sellers (
                id, name, city, contact, catalogue_url, created_at, updated_at, is_active
            ) VALUES (
                %(id)s, %(name)s, %(city)s, %(contact)s, %(catalogue_url)s, 
                %(created_at)s, %(updated_at)s, %(is_active)s
            )
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                city = EXCLUDED.city,
                contact = EXCLUDED.contact,
                catalogue_url = EXCLUDED.catalogue_url,
                updated_at = EXCLUDED.updated_at,
                is_active = EXCLUDED.is_active
            """
            
            cursor.execute(query, {
                'id': seller['id'],
                'name': seller['name'],
                'city': seller['city'],
                'contact': seller['contact'],
                'catalogue_url': seller['catalogue_url'],
                'created_at': seller['created_at'],
                'updated_at': seller['updated_at'],
                'is_active': seller['is_active']
            })
            
            imported_count += 1
        
        conn.commit()
        print(f"✅ Imported {imported_count} sellers")
        return True
        
    except Exception as e:
        print(f"❌ Failed to import sellers: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()

def import_products(conn, products_data):
    """
    Import products data using a robust bulk insert/update strategy.
    This prevents errors from duplicate product_links within the same scrape file.
    Also handles product lifecycle tracking (marking removed products).
    """
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    if not products_data:
        print("✅ No products to import.")
        return True
        
    try:
        # Pre-process products to extract metadata and set last_seen_id
        current_scrape_job_id = products_data[0].get('scrape_job_id')
        for p in products_data:
            metadata = p.get('metadata', {})
            p['photo_count'] = metadata.get('photo_count', 0)
            p['scraped_at'] = metadata.get('scraped_at')
            p['last_seen_scrape_job_id'] = current_scrape_job_id

        # 1. Separate products with and without links
        products_with_link = [p for p in products_data if p.get('product_link')]
        products_without_link = [p for p in products_data if not p.get('product_link')]

        # 2. De-duplicate products_with_link from the source file, keeping the last seen version
        unique_products_with_link_map = {p['product_link']: p for p in products_with_link}
        unique_products_with_link = list(unique_products_with_link_map.values())
        
        # 3. Find which products already exist in the database
        if unique_products_with_link:
            links = tuple(p['product_link'] for p in unique_products_with_link)
            cursor.execute("SELECT product_link, id FROM products WHERE product_link IN %s", (links,))
            existing_products_map = {row['product_link']: row['id'] for row in cursor}
        else:
            existing_products_map = {}

        # 4. Divide into new products (to_insert) and existing products (to_update)
        to_insert = []
        to_update = []
        for p in unique_products_with_link:
            if p['product_link'] in existing_products_map:
                # This product exists, so we'll update it. Use the existing DB ID.
                p['id'] = existing_products_map[p['product_link']]
                to_update.append(p)
            else:
                # This is a new product to insert.
                to_insert.append(p)
        
        # Products without a link are always inserted as new, since they can't be de-duplicated.
        to_insert.extend(products_without_link)

        # 5. Bulk insert new products
        if to_insert:
            insert_query = """
                INSERT INTO products (
                    id, seller_id, scrape_job_id, title, price, description,
                    images, product_link, is_out_of_stock, metadata, 
                    photo_count, scraped_at, last_seen_scrape_job_id, 
                    is_removed, removed_at, created_at, updated_at
                ) VALUES %s
            """
            insert_values = [
                (
                    p['id'], p['seller_id'], p['scrape_job_id'], p['title'], p['price'],
                    p['description'], json.dumps(p.get('images', [])), p.get('product_link'),
                    p.get('is_out_of_stock', False), json.dumps(p.get('metadata', {})),
                    p.get('photo_count', 0), p.get('scraped_at'), p.get('last_seen_scrape_job_id'),
                    p.get('is_removed', False), p.get('removed_at'), p['created_at'], p['updated_at']
                ) for p in to_insert
            ]
            psycopg2.extras.execute_values(cursor, insert_query, insert_values)

        # 6. Bulk update existing products
        if to_update:
            update_query = """
                UPDATE products AS p SET
                    title = data.title,
                    price = data.price,
                    description = data.description,
                    images = data.images::jsonb,
                    is_out_of_stock = data.is_out_of_stock,
                    metadata = data.metadata::jsonb,
                    photo_count = data.photo_count,
                    scraped_at = data.scraped_at::timestamptz,
                    last_seen_scrape_job_id = data.last_seen_scrape_job_id::uuid,
                    is_removed = data.is_removed,
                    removed_at = data.removed_at::timestamptz,
                    updated_at = data.updated_at::timestamptz,
                    scrape_job_id = data.scrape_job_id::uuid,
                    seller_id = data.seller_id::uuid
                FROM (VALUES %s) AS data (
                    id, title, price, description, images, is_out_of_stock,
                    metadata, photo_count, scraped_at, last_seen_scrape_job_id,
                    is_removed, removed_at, updated_at, scrape_job_id, seller_id
                )
                WHERE p.id = data.id::uuid
            """
            update_values = [
                (
                    p['id'], p['title'], p['price'], p['description'],
                    json.dumps(p.get('images', [])), p.get('is_out_of_stock', False),
                    json.dumps(p.get('metadata', {})), p.get('photo_count', 0),
                    p.get('scraped_at'), p.get('last_seen_scrape_job_id'),
                    p.get('is_removed', False), p.get('removed_at'), p['updated_at'],
                    p['scrape_job_id'], p['seller_id']
                ) for p in to_update
            ]
            psycopg2.extras.execute_values(cursor, update_query, update_values)

        # 7. Handle product lifecycle tracking
        current_scrape_job_id = None
        seller_ids = []
        current_product_links = []
        
        if products_data:
            # Get current scrape job ID and seller IDs from the data
            current_scrape_job_id = products_data[0].get('scrape_job_id')
            seller_ids = list(set(p['seller_id'] for p in products_data))
            current_product_links = [p['product_link'] for p in products_data if p.get('product_link')]
            
            # Mark products as removed if they're not in this scrape
            if seller_ids and current_product_links:
                cursor.execute(
                    "SELECT * FROM mark_missing_products_as_removed(%s::UUID[], %s::UUID, %s)",
                    (seller_ids, current_scrape_job_id, current_product_links)
                )
                removal_result = cursor.fetchone()
                removed_count = removal_result['products_marked_removed'] if removal_result else 0
                
                # Mark previously removed products as active if they reappeared
                cursor.execute(
                    "SELECT mark_reappeared_products_as_active(%s::UUID, %s)",
                    (current_scrape_job_id, current_product_links)
                )
                reactivated_result = cursor.fetchone()
                reactivated_count = reactivated_result['mark_reappeared_products_as_active'] if reactivated_result else 0
            else:
                removed_count = 0
                reactivated_count = 0
        else:
            removed_count = 0
            reactivated_count = 0

        conn.commit()
        
        print("✅ Processing complete:")
        print(f"   - New products inserted: {len(to_insert)}")
        print(f"   - Existing products updated: {len(to_update)}")
        print(f"   - Products marked as removed: {removed_count}")
        print(f"   - Previously removed products reactivated: {reactivated_count}")
        print(f"   - Total processed: {len(products_data)} (duplicates in source file were merged)")
        return True

    except Exception as e:
        print(f"❌ Failed to import products: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()

def main():
    if len(sys.argv) != 2:
        print("Usage: python import_to_supabase.py <json_file>")
        sys.exit(1)
    
    json_file = sys.argv[1]
    
    if not os.path.exists(json_file):
        print(f"❌ File not found: {json_file}")
        sys.exit(1)
    
    print(f"📂 Loading data from {json_file}...")
    
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"❌ Failed to load JSON file: {e}")
        sys.exit(1)
    
    print(f"📊 Data summary:")
    print(f"   - Scrape Job: {data['scrape_job']['id']}")
    print(f"   - Sellers: {len(data['sellers'])}")
    print(f"   - Products: {len(data['products'])}")
    print(f"   - Status: {data['scrape_job']['status']}")
    
    # Connect to Supabase
    print(f"\n🔌 Connecting to Supabase...")
    conn = connect_to_supabase()
    if not conn:
        sys.exit(1)
    
    print("✅ Connected to Supabase")
    
    try:
        # Import data in order: scrape_job -> sellers -> products
        print(f"\n📥 Importing scrape job...")
        if not import_scrape_job(conn, data['scrape_job']):
            raise Exception("Failed to import scrape job")
        
        print(f"\n📥 Importing sellers...")
        if not import_sellers(conn, data['sellers']):
            raise Exception("Failed to import sellers")
        
        print(f"\n📥 Importing products...")
        if not import_products(conn, data['products']):
            raise Exception("Failed to import products")
        
        print(f"\n🎉 Successfully imported all data!")
        print(f"   - Scrape Job: {data['scrape_job']['id']}")
        print(f"   - Sellers: {len(data['sellers'])}")
        print(f"   - Products: {len(data['products'])}")
        
        # Show some stats
        print(f"\n📊 You can now view the data in Supabase Studio:")
        print(f"   - Dashboard: http://127.0.0.1:54323")
        print(f"   - API: http://127.0.0.1:54321")
        
    except Exception as e:
        print(f"❌ Import failed: {e}")
        sys.exit(1)
    finally:
        conn.close()

if __name__ == "__main__":
    main() 