#!/usr/bin/env python3
"""
WhatsApp Scraper - Main Orchestrator
Loads sellers from database, runs scraper, and imports results back to database
"""

import os
import sys
import time
import json
from datetime import datetime, timezone
import psycopg2
from psycopg2.extras import RealDictCursor

# Database connection
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://postgres:postgres@127.0.0.1:54322/postgres')

def connect_to_database():
    """Connect to the database"""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except Exception as e:
        print(f"❌ Failed to connect to database: {e}")
        return None

def load_active_sellers_from_db():
    """Load active sellers from the database"""
    conn = connect_to_database()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get all active sellers
        cursor.execute("""
            SELECT id, name, city, contact, catalogue_url 
            FROM sellers 
            WHERE is_active = true 
            ORDER BY name
        """)
        
        sellers = cursor.fetchall()
        
        print(f"📊 Loaded {len(sellers)} active sellers from database")
        
        # Convert to format expected by scraper
        seller_data = []
        for seller in sellers:
            seller_data.append({
                'db_id': seller['id'],  # Keep database ID for reference
                'name': seller['name'],
                'city': seller['city'] or '',
                'contact': seller['contact'] or '',
                'catalogue_link': seller['catalogue_url']
            })
        
        return seller_data
        
    except Exception as e:
        print(f"❌ Error loading sellers from database: {e}")
        return []
    finally:
        cursor.close()
        conn.close()

def create_temp_csv(sellers):
    """Create a temporary CSV file for the scraper"""
    import pandas as pd
    
    if not sellers:
        print("❌ No sellers to process")
        return None
    
    # Create DataFrame
    df = pd.DataFrame(sellers)
    
    # Save to temporary CSV
    temp_csv = "temp_sellers.csv"
    df.to_csv(temp_csv, index=False)
    
    print(f"📄 Created temporary CSV with {len(sellers)} sellers: {temp_csv}")
    return temp_csv

def run_scraper(csv_file):
    """Run the scraper script"""
    print(f"\n🚀 Starting scraper with {csv_file}...")
    
    try:
        # Import and run scraper functions directly
        from scraper_json import (
            setup_driver, handle_whatsapp_login, scrape_row, 
            scrape_session, OUTPUT_FILE
        )
        import pandas as pd
        
        # Load CSV
        df = pd.read_csv(csv_file)
        
        # Setup driver
        driver = setup_driver()
        if not driver:
            print("❌ Failed to setup selenium driver")
            return False
        
        # Handle WhatsApp login
        if not handle_whatsapp_login(driver):
            print("❌ WhatsApp login failed")
            driver.quit()
            return False
        
        print("\n--- Starting Catalog Scraping ---\n")
        
        total_start_time = time.time()
        total_items = 0
        
        # Scrape each seller
        for i, row in df.iterrows():
            scraped_count = scrape_row(driver, row, i)
            total_items += scraped_count
            print(f'Scraped count for {row["name"]}: {scraped_count}')
        
        driver.quit()
        
        # Finalize scrape job
        total_elapsed_time = time.time() - total_start_time
        scrape_session["scrape_job"]["status"] = "completed"
        scrape_session["scrape_job"]["completed_at"] = datetime.now(timezone.utc).isoformat()
        scrape_session["scrape_job"]["total_items"] = total_items
        scrape_session["scrape_job"]["total_sellers"] = len(scrape_session["sellers"])
        scrape_session["scrape_job"]["job_metadata"]["total_time_seconds"] = round(total_elapsed_time, 2)
        
        # Save to JSON file
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(scrape_session, f, indent=2, ensure_ascii=False)
        
        print(f"\n🕒 Total scraping time: {total_elapsed_time:.2f} seconds")
        print(f"✅ Scraping completed. Total items: {total_items}, Sellers: {len(scrape_session['sellers'])}")
        print(f"📄 Supabase-compatible JSON saved to {OUTPUT_FILE}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error running scraper: {e}")
        return False

def run_import(json_file):
    """Run the import script"""
    print(f"\n📥 Starting import from {json_file}...")
    
    try:
        # Import and run import functions directly
        from import_to_supabase import (
            connect_to_supabase, import_scrape_job, 
            import_sellers, import_products
        )
        
        # Load JSON data
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        print(f"📊 Data summary:")
        print(f"   - Scrape Job: {data['scrape_job']['id']}")
        print(f"   - Sellers: {len(data['sellers'])}")
        print(f"   - Products: {len(data['products'])}")
        print(f"   - Status: {data['scrape_job']['status']}")
        
        # Connect to database
        conn = connect_to_supabase()
        if not conn:
            return False
        
        # Import data in order: scrape_job -> sellers -> products
        print(f"\n📥 Importing scrape job...")
        if not import_scrape_job(conn, data['scrape_job']):
            raise Exception("Failed to import scrape job")
        
        print(f"📥 Importing sellers...")
        if not import_sellers(conn, data['sellers']):
            raise Exception("Failed to import sellers")
        
        print(f"📥 Importing products...")
        if not import_products(conn, data['products']):
            raise Exception("Failed to import products")
        
        print(f"\n🎉 Successfully imported all data!")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Import failed: {e}")
        return False

def run_algolia_indexing(json_file):
    """Run Algolia indexing"""
    print(f"\n🔍 Starting Algolia indexing from {json_file}...")
    
    try:
        # Import and run Algolia indexer
        from algolia_indexer import index_to_algolia
        
        # Index to Algolia
        success = index_to_algolia(json_file, clear_index=True)
        
        if success:
            print(f"✅ Algolia indexing completed!")
            return True
        else:
            print(f"❌ Algolia indexing failed")
            return False
            
    except ImportError as e:
        print(f"⚠️ Algolia indexing skipped: Missing dependency ({e})")
        print(f"💡 Install with: pip install 'algoliasearch>=4.0.0'")
        return True  # Don't fail the whole pipeline
    except Exception as e:
        print(f"❌ Algolia indexing error: {e}")
        return False

def cleanup_temp_files():
    """Clean up temporary files"""
    temp_files = ["temp_sellers.csv"]
    
    for file in temp_files:
        if os.path.exists(file):
            os.remove(file)
            print(f"🗑️ Cleaned up: {file}")

def main():
    """Main orchestrator function"""
    print("🚀 WhatsApp Scraper - Main Orchestrator")
    print("=" * 50)
    
    start_time = time.time()
    
    try:
        # Step 1: Load sellers from database
        print("\n📋 Step 1: Loading sellers from database...")
        sellers = load_active_sellers_from_db()
        
        if not sellers:
            print("❌ No active sellers found in database")
            return False
        
        # Step 2: Create temporary CSV for scraper
        print("\n📄 Step 2: Preparing scraper input...")
        temp_csv = create_temp_csv(sellers)
        
        if not temp_csv:
            return False
        
        # Step 3: Run scraper
        print("\n🤖 Step 3: Running scraper...")
        scraper_success = run_scraper(temp_csv)
        
        if not scraper_success:
            print("❌ Scraping failed")
            return False
        
        # Step 4: Import results to database
        print("\n📥 Step 4: Importing results to database...")
        from scraper_json import OUTPUT_FILE
        import_success = run_import(OUTPUT_FILE)
        
        if not import_success:
            print("❌ Import failed")
            return False
        
        # Step 5: Index to Algolia
        print("\n🔍 Step 5: Indexing to Algolia...")
        algolia_success = run_algolia_indexing(OUTPUT_FILE)
        
        if not algolia_success:
            print("⚠️ Algolia indexing failed, but continuing...")
        
        # Step 6: Show final summary
        total_elapsed = time.time() - start_time
        print(f"\n🎉 Pipeline completed successfully!")
        print(f"⏱️ Total time: {total_elapsed:.2f} seconds")
        print(f"✅ Database import: {'Success' if import_success else 'Failed'}")
        print(f"🔍 Algolia indexing: {'Success' if algolia_success else 'Failed/Skipped'}")
        
        # Show database stats
        print(f"\n📊 Final database stats:")
        conn = connect_to_database()
        if conn:
            cursor = conn.cursor()
            
            # Get counts
            cursor.execute("SELECT COUNT(*) FROM sellers WHERE is_active = true")
            result = cursor.fetchone()
            active_sellers = result[0] if result else 0
            
            cursor.execute("SELECT COUNT(*) FROM active_products")
            result = cursor.fetchone()
            active_products = result[0] if result else 0
            
            cursor.execute("SELECT COUNT(*) FROM products WHERE is_removed = true")
            result = cursor.fetchone()
            removed_products = result[0] if result else 0
            
            cursor.execute("SELECT COUNT(*) FROM scrape_jobs WHERE status = 'completed'")
            result = cursor.fetchone()
            completed_jobs = result[0] if result else 0
            
            print(f"   - Active sellers: {active_sellers}")
            print(f"   - Active products: {active_products}")
            print(f"   - Removed products: {removed_products}")
            print(f"   - Completed scrape jobs: {completed_jobs}")
            
            conn.close()
        
        return True
        
    except KeyboardInterrupt:
        print("\n⚠️ Process interrupted by user")
        return False
    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        return False
    finally:
        # Always cleanup temp files
        cleanup_temp_files()

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1) 