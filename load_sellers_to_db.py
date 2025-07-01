#!/usr/bin/env python3
"""
Load Sellers to Database
Utility script to load sellers from CSV file into the database
"""

import os
import sys
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timezone
import uuid

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

def load_sellers_from_csv(csv_file):
    """Load sellers from CSV file into the database"""
    
    if not os.path.exists(csv_file):
        print(f"❌ File not found: {csv_file}")
        return False
    
    try:
        # Read CSV file
        df = pd.read_csv(csv_file)
        print(f"📄 Loaded CSV with {len(df)} rows")
        
        # Validate required columns
        required_columns = ['name', 'catalogue_link']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            print(f"❌ Missing required columns: {missing_columns}")
            print(f"Available columns: {list(df.columns)}")
            return False
        
        # Connect to database
        conn = connect_to_database()
        if not conn:
            return False
        
        cursor = conn.cursor()
        loaded_count = 0
        updated_count = 0
        
        print(f"📥 Starting to load sellers...")
        
        for index, row in df.iterrows():
            try:
                # Extract seller info (handle different column name variations)
                name = row.get('name', row.get('seller_name', f'Seller_{index}'))
                city = row.get('city', row.get('seller_city', ''))
                contact = row.get('contact', row.get('seller_contact', ''))
                catalogue_url = row.get('catalogue_link', row.get('catalogue_url', ''))
                
                # Clean and validate data
                name = str(name).strip() if pd.notna(name) else f'Seller_{index}'
                city = str(city).strip() if pd.notna(city) and city else None
                contact = str(contact).strip() if pd.notna(contact) and contact else None
                catalogue_url = str(catalogue_url).strip() if pd.notna(catalogue_url) else ''
                
                if not catalogue_url:
                    print(f"⚠️ Skipping row {index}: No catalogue URL")
                    continue
                
                # Insert or update seller
                query = """
                INSERT INTO sellers (id, name, city, contact, catalogue_url, created_at, updated_at, is_active)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (name) DO UPDATE SET
                    city = EXCLUDED.city,
                    contact = EXCLUDED.contact,
                    catalogue_url = EXCLUDED.catalogue_url,
                    updated_at = EXCLUDED.updated_at,
                    is_active = EXCLUDED.is_active
                RETURNING (xmax = 0) AS inserted
                """
                
                now = datetime.now(timezone.utc)
                cursor.execute(query, (
                    str(uuid.uuid4()),
                    name,
                    city,
                    contact,
                    catalogue_url,
                    now,
                    now,
                    True
                ))
                
                result = cursor.fetchone()
                if result and result[0]:  # inserted = True
                    loaded_count += 1
                else:  # inserted = False (was an update)
                    updated_count += 1
                
                if (loaded_count + updated_count) % 10 == 0:
                    print(f"📥 Processed {loaded_count + updated_count} sellers...")
                    
            except Exception as e:
                print(f"❌ Error processing seller at row {index}: {e}")
                continue
        
        conn.commit()
        
        print(f"✅ Processing complete:")
        print(f"   - New sellers inserted: {loaded_count}")
        print(f"   - Existing sellers updated: {updated_count}")
        print(f"   - Total processed: {loaded_count + updated_count}")
        
        # Show final stats
        cursor.execute("SELECT COUNT(*) FROM sellers WHERE is_active = true")
        result = cursor.fetchone()
        total_active = result[0] if result else 0
        
        print(f"📊 Database now has {total_active} active sellers")
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        print(f"❌ Error loading sellers: {e}")
        return False

def show_database_sellers():
    """Show current sellers in the database"""
    conn = connect_to_database()
    if not conn:
        return
    
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        cursor.execute("""
            SELECT name, city, contact, catalogue_url, is_active, created_at
            FROM sellers 
            ORDER BY name
        """)
        
        sellers = cursor.fetchall()
        
        if not sellers:
            print("📊 No sellers found in database")
            return
        
        print(f"📊 Current sellers in database ({len(sellers)} total):")
        print("-" * 80)
        
        for seller in sellers:
            status = "✅ Active" if seller['is_active'] else "❌ Inactive"
            print(f"{seller['name']:<30} | {seller['city'] or 'N/A':<15} | {status}")
        
        # Show counts
        active_count = sum(1 for s in sellers if s['is_active'])
        inactive_count = len(sellers) - active_count
        
        print("-" * 80)
        print(f"Active: {active_count} | Inactive: {inactive_count} | Total: {len(sellers)}")
        
    except Exception as e:
        print(f"❌ Error fetching sellers: {e}")
    finally:
        conn.close()

def main():
    """Main function"""
    if len(sys.argv) < 2:
        print("📖 Usage:")
        print("  python load_sellers_to_db.py <csv_file>     # Load sellers from CSV")
        print("  python load_sellers_to_db.py --show         # Show current sellers")
        print("")
        print("📄 CSV Format:")
        print("  Required columns: name, catalogue_link")
        print("  Optional columns: city, contact")
        print("")
        print("📋 Example CSV:")
        print("  name,city,contact,catalogue_link")
        print("  'John Phone Shop',Mumbai,9876543210,https://wa.me/c/919876543210")
        return
    
    if sys.argv[1] == "--show":
        show_database_sellers()
        return
    
    csv_file = sys.argv[1]
    
    print("📱 WhatsApp Scraper - Seller Loader")
    print("=" * 50)
    
    success = load_sellers_from_csv(csv_file)
    
    if success:
        print(f"\n✅ Sellers loaded successfully!")
        print(f"💡 You can now run: python main.py")
    else:
        print(f"\n❌ Failed to load sellers")
        sys.exit(1)

if __name__ == "__main__":
    main() 