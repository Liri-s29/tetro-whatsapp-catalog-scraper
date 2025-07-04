#!/usr/bin/env python3
"""
WhatsApp Catalog Scraper - JSON Output Version
Based on the original scraper.py but outputs structured JSON for Supabase import
"""

import os
import time
import json
import pandas as pd
import base64
from datetime import datetime
from datetime import timezone
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
from selenium.webdriver.common.keys import Keys
from rapidfuzz import fuzz
import qrcode
import uuid
from dotenv import load_dotenv
from supabase import create_client, Client
import hashlib

# ---------------------------
# Load environment variables
# ---------------------------
load_dotenv()

# ---------------------------
# Constants
# ---------------------------
CSV_FILE = "seller_catalog_links.csv"
OUTPUT_FILE = "scraped_catalog_supabase.json"
CHROME_PROFILE_PATH = "./chrome-profile-py"
QR_SCREENSHOT_FILE = "whatsapp_qr.png"
IMAGE_STORAGE_PATH = "product_images"

# --- Supabase ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SUPABASE_BUCKET = "product-images"

# --- WhatsApp Selectors ---
WHATSAPP_URL = "https://web.whatsapp.com/"
QR_CODE_SELECTOR = 'canvas[aria-label="Scan this QR code to link a device!"]'
MAIN_CHAT_SELECTOR = '.xsknx04'

PARENT_SELECTOR = ".x1tkvqr7"
ITEM_SELECTOR = f".x1g42fcv > div"
PHOTO_CONTAINER_SELECTOR = ".x10l6tqk.x13vifvy.xu96u03.x78zum5.x6s0dn4.xh8yej3.x5yr21d.x10wlt62.xw2csxc.x1hc1fzr"
LINK_ICON_SELECTOR = 'button[title="Product link"]'
LINK_HREF_SELECTOR = '#product-link-anchor'
BACK_BUTTON_SELECTOR = 'div[aria-label="Back"]'
SEE_ALL_BUTTON_SELECTOR = '.xhmieyt'
ALL_ITEMS_HEADER_SELECTOR = '.xcgk4ki'

# Item Detail Page Selectors
DETAIL_PAGE_CONTAINER = ".x162tt16"
DETAIL_PAGE_IMAGE_SELECTOR = "._ak9n"
LIST_ITEM_TITLE_SELECTOR = 'span[title]'

IPHONE_KEYWORDS = [
    "iphone", "i phone", "apple phone",
    "iphone 13", "iphone 14", "iphone 15",
    "iphone pro"
]

# Global data structure for JSON output
scrape_session = {
    "scrape_job": {
        "id": str(uuid.uuid4()),
        "status": "running",
        "started_at": datetime.now(timezone.utc).isoformat(),
        "completed_at": None,
        "total_items": 0,
        "total_sellers": 0,
        "error_message": None,
        "job_metadata": {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "sellers_processed": []
        }
    },
    "sellers": {},  # Will be populated with seller data
    "products": []  # Will contain all scraped products
}

# ---------------------------
# Helper Functions
# ---------------------------
def url_to_id(url):
    """Convert a URL to a stable ID by hashing it"""
    if not url:
        return str(uuid.uuid4())  # Fallback to UUID if no URL
    return hashlib.md5(url.encode('utf-8')).hexdigest()

def sanitize_for_storage(text):
    """Sanitize text for use in file paths"""
    import re
    if not text:
        return "unknown"
    
    # Remove or replace characters that might cause issues in file paths
    # Keep only alphanumeric, hyphens, underscores
    sanitized = re.sub(r'[^\w\-]', '_', text.strip())
    
    # Remove multiple consecutive underscores
    sanitized = re.sub(r'_+', '_', sanitized)
    
    # Remove leading/trailing underscores
    sanitized = sanitized.strip('_')
    
    # Ensure it's not empty after sanitization
    if not sanitized:
        sanitized = "item"
    
    # Limit length to avoid filesystem issues
    return sanitized[:50]

def is_iphone_related(texts, threshold=70):
    combined = " ".join(text.lower() for text in texts if text)
    return any(fuzz.partial_ratio(keyword, combined) >= threshold for keyword in IPHONE_KEYWORDS)

def get_or_create_seller(name, city, contact, catalogue_url):
    """Get or create seller in the global data structure using catalog URL as unique identifier"""
    seller_id = url_to_id(catalogue_url)  # Use catalog URL as stable ID
    
    if seller_id not in scrape_session["sellers"]:
        scrape_session["sellers"][seller_id] = {
            "id": seller_id,
            "name": name,
            "city": city,
            "contact": contact,
            "catalogue_url": catalogue_url,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "is_active": True
        }
    
    return scrape_session["sellers"][seller_id]

def add_product(seller, product_data):
    """Add a product to the global data structure using product URL as unique identifier"""
    scraped_time = datetime.now(timezone.utc).isoformat()
    
    # Use product URL as unique identifier, fallback to title+seller if no URL
    product_url = product_data.get("product_link")
    if product_url:
        product_id = url_to_id(product_url)
    else:
        # Fallback: create ID from title + seller catalog URL for products without links
        fallback_key = f"{product_data['title']}_{seller['catalogue_url']}"
        product_id = url_to_id(fallback_key)
    
    # Check if product already exists
    existing_product = None
    for product in scrape_session["products"]:
        if product["id"] == product_id:
            existing_product = product
            break
    
    if existing_product:
        # Update existing product
        existing_product["last_seen_scrape_job_id"] = scrape_session["scrape_job"]["id"]
        existing_product["updated_at"] = scraped_time
        existing_product["is_removed"] = False
        existing_product["removed_at"] = None
        # Update other fields that might have changed
        existing_product["title"] = product_data["title"]
        existing_product["price"] = product_data["price"]
        existing_product["description"] = product_data["description"]
        existing_product["is_out_of_stock"] = product_data.get("is_out_of_stock", False)
        existing_product["photo_count"] = product_data.get("photo_count", 0)
        if product_data.get("product_link"):
            existing_product["product_link"] = product_data["product_link"]
        return existing_product
    else:
        # Create new product
        product = {
            "id": product_id,
            "seller_id": seller["id"],
            "scrape_job_id": scrape_session["scrape_job"]["id"],
            "title": product_data["title"],
            "price": product_data["price"],
            "description": product_data["description"],
            "images": [],  # Initialize with an empty list
            "product_link": product_data.get("product_link"),
            "is_out_of_stock": product_data.get("is_out_of_stock", False),
            "photo_count": product_data.get("photo_count", 0),
            "scraped_at": scraped_time,
            "last_seen_scrape_job_id": scrape_session["scrape_job"]["id"],
            "is_removed": False,
            "removed_at": None,
            "metadata": {
                "catalogue_url": product_data["catalogue_url"],
                "seller_name": product_data["seller_name"],
                "seller_city": product_data["seller_city"],
                "seller_contact": product_data["seller_contact"]
            },
            "created_at": scraped_time,
            "updated_at": scraped_time
        }
        
        scrape_session["products"].append(product)
        return product

def save_product_images(driver, product, supabase_client: Client):
    """
    Fetches product images from blob URLs, uploads them to Supabase Storage,
    and returns their public URLs. Avoids duplicate uploads by checking if images exist.
    """
    image_urls = []
    
    # First, check if product already has images stored
    if product.get('images') and len(product['images']) > 0:
        print(f"   -> Product already has {len(product['images'])} images stored, skipping upload")
        return product['images']
    
    try:
        # Set a longer timeout for async script execution
        driver.set_script_timeout(30)
        
        # Find all product images on the detail page
        img_elements = driver.find_elements(By.CSS_SELECTOR, DETAIL_PAGE_IMAGE_SELECTOR)
        
        # print(f"   -> Found {len(img_elements)} images for product '{product['title']}'")

        if not img_elements:
            return []

        # Create human-readable directory structure: sellername-id/productname-id/
        seller_name = sanitize_for_storage(product.get('metadata', {}).get('seller_name', 'unknown_seller'))
        product_title = sanitize_for_storage(product.get('title', 'unknown_product'))
        
        seller_dir = f"{seller_name}-{product['seller_id']}"
        product_dir = f"{product_title}-{product['id']}"
        base_path = f"{seller_dir}/{product_dir}"
        
        # Check if images already exist in storage
        try:
            existing_files = supabase_client.storage.from_(SUPABASE_BUCKET).list(base_path)
            if existing_files and len(existing_files) > 0:
                print(f"   -> Found {len(existing_files)} existing images, returning URLs")
                # Return URLs for existing images
                for file_obj in existing_files:
                    if file_obj['name'].isdigit() or file_obj['name'].endswith('.png'):
                        storage_path = f"{base_path}/{file_obj['name']}"
                        public_url = supabase_client.storage.from_(SUPABASE_BUCKET).get_public_url(storage_path)
                        image_urls.append(public_url)
                return sorted(image_urls)  # Sort to maintain consistent order
        except Exception as e:
            # If listing fails, proceed with upload (storage path might not exist yet)
            print(f"   -> Could not check existing images: {e}")

        for i, img in enumerate(img_elements):
            blob_url = img.get_attribute('src')
            if not blob_url or not blob_url.startswith('blob:'):
                print(f"   -> Skipping image {i+1}: Invalid blob URL")
                continue

            # Use serial number as filename (1, 2, 3, etc.)
            sno = str(i + 1)
            storage_path = f"{base_path}/{sno}"

            # Simplified JavaScript to fetch blob and convert to base64
            script = f"""
            var callback = arguments[arguments.length - 1];
            
            fetch('{blob_url}')
                .then(response => response.blob())
                .then(blob => {{
                    var reader = new FileReader();
                    reader.onload = function() {{
                        callback(reader.result);
                    }};
                    reader.onerror = function() {{
                        callback(null);
                    }};
                    reader.readAsDataURL(blob);
                }})
                .catch(error => {{
                    console.error('Error fetching blob:', error);
                    callback(null);
                }});
            """

            try:
                # print(f"   -> Processing image {i+1}/{len(img_elements)}...")
                
                # Execute with longer timeout
                data_url = driver.execute_async_script(script)
                
                if not data_url:
                    print(f"   -> Failed to fetch image {i+1}")
                    continue
                    
                header, encoded = data_url.split(",", 1)
                image_data = base64.b64decode(encoded)
                
                # print(f"   -> Uploading to Supabase: {storage_path}")
                
                # Upload to Supabase
                supabase_client.storage.from_(SUPABASE_BUCKET).upload(
                    path=storage_path,
                    file=image_data,
                    file_options={"content-type": "image/png", "upsert": "true"}
                )
                
                # Get public URL after successful upload
                public_url = supabase_client.storage.from_(SUPABASE_BUCKET).get_public_url(storage_path)
                image_urls.append(public_url)
                # print(f"   -> Successfully uploaded image {i+1}: {public_url}")

            except Exception as e:
                print(f"   -> Error processing image {i+1}: {e}")
                # Continue with next image instead of failing completely
                continue
        
        print(f"   -> Total images uploaded: {len(image_urls)}")

    except Exception as e:
        print(f"   -> Error finding or uploading product images: {e}")
    
    return image_urls

def navigate_to_all_items_page(driver, timeout=30):
    print('🔍 Looking for "All items" collection...')
    
    retries = 0
    max_retries = 5
    all_items_collection = None
    
    # Part 1: Find the "All items" collection by scrolling
    while retries < max_retries:
        try:
            collections = driver.find_elements(By.CSS_SELECTOR, ITEM_SELECTOR)
            
            for collection in collections:
                try:
                    title_element = collection.find_element(By.CSS_SELECTOR, LIST_ITEM_TITLE_SELECTOR)
                    title_text = title_element.get_attribute('title')
                    if title_text and 'all items' in title_text.lower():
                        all_items_collection = collection
                        break
                except (StaleElementReferenceException, NoSuchElementException):
                    continue

            if all_items_collection:
                print('✅ Found "All items" collection. Proceeding to click.')
                break

            print('📜 "All items" not found yet. Scrolling to load more...')
            driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.END)
            
            current_count = len(collections)
            try:
                WebDriverWait(driver, 5).until(
                    lambda d: len(d.find_elements(By.CSS_SELECTOR, ITEM_SELECTOR)) > current_count
                )
                retries = 0
            except TimeoutException:
                retries += 1
                print(f"No new collections loaded. Retry {retries}/{max_retries}...")
        
        except Exception as e:
            print(f"An error occurred while searching for 'All items' collection: {e}")
            retries += 1
            time.sleep(1)

    # Part 2: Click and verify
    if not all_items_collection:
        print('❌ Could not find "All items" collection after scrolling.')
        return False

    try:
        see_all_button = all_items_collection.find_element(By.CSS_SELECTOR, SEE_ALL_BUTTON_SELECTOR)
        
        print('🔗 Clicking "See all" button...')
        driver.execute_script("arguments[0].scrollIntoView(true);", see_all_button)
        time.sleep(0.5)
        driver.execute_script("arguments[0].click();", see_all_button)

        long_wait = WebDriverWait(driver, 10)
        header = long_wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ALL_ITEMS_HEADER_SELECTOR)))
        
        if header.is_displayed():
             print('✅ Successfully navigated to "See all" page')
             return True
        else:
            print('⚠️ Navigated, but header check failed.')
            return False

    except StaleElementReferenceException:
        print("The 'All items' collection became stale before it could be clicked. Retrying the process might help.")
        return False
    except Exception as e:
        print(f"❌ Error clicking 'See all' or verifying navigation: {e}")
        return False

    return False

def process_catalog_items(driver, seller_data, seller, supabase_client):
    print("Processing catalog items...")
    wait = WebDriverWait(driver, 5)
    index = 0
    items_scraped = 0

    while True:
        # Get a fresh list of items on each iteration to avoid stale elements
        items = driver.find_elements(By.CSS_SELECTOR, ITEM_SELECTOR)
        
        if index >= len(items):
            prev_count = len(items)
            print(f"Scrolled to item {index}, current item count: {prev_count}. Loading more...")
            
            try:
                if items:
                    driver.execute_script("arguments[0].scrollIntoView({block: 'end'});", items[-1])
                
                # Wait for more items to load
                WebDriverWait(driver, 10).until(
                    lambda d: len(d.find_elements(By.CSS_SELECTOR, ITEM_SELECTOR)) > prev_count
                )
                
                # Refresh items list after loading
                items = driver.find_elements(By.CSS_SELECTOR, ITEM_SELECTOR)
                print(f"Scrolled and loaded more items (now {len(items)})")
            except TimeoutException:
                print("No more items loaded after scrolling.")
                break
        
        if index >= len(items):
            print("Index out of bounds, no new items loaded. Exiting.")
            break

        item = items[index]
        
        try:
            # Get title from list view for filtering
            title_from_list = item.find_element(By.CSS_SELECTOR, LIST_ITEM_TITLE_SELECTOR).get_attribute('title')

            if not is_iphone_related([title_from_list]):
                index += 1
                continue

            # --- Start of single item processing ---
            wait.until(EC.element_to_be_clickable(item)).click()
            time.sleep(0.5)

            # Scrape from detail page
            detail_wait = WebDriverWait(driver, 10)
            detail_container = detail_wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, DETAIL_PAGE_CONTAINER)))

            # Find all direct children that are not buttons and do not contain buttons.
            text_elements = detail_container.find_elements(By.XPATH, "./div[not(.//button)]")

            title = None
            price = None
            description = None
            price_text = None  # To preserve original text for out-of-stock check

            num_elements = len(text_elements)

            if num_elements == 1:
                title = text_elements[0].text
            elif num_elements == 2:
                title = text_elements[0].text
                description = text_elements[1].text
                price = "Not mentioned"
            elif num_elements >= 3:
                title = text_elements[0].text
                price_text = text_elements[1].text
                description = "\n".join([el.text for el in text_elements[2:]])
            
            # Only derive price from price_text if it hasn't been set (e.g., for 2 elements)
            if price is None and price_text:
                price = price_text.split(" ")[0]

            is_out_of_stock = "out of stock" in price_text.lower() if price_text else False
            
            if is_out_of_stock:
                print(f"   -> Item '{title}' is out of stock.")
                # Go back once from detail page
                wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, BACK_BUTTON_SELECTOR))).click()
                time.sleep(0.3)
                index += 1
                continue

            product_data = {
                **seller_data,
                "title": title,
                "price": price,
                "description": description,
                "photo_count": 0,
                "product_link": None,
                "is_out_of_stock": is_out_of_stock
            }

            try:
                photo_container = detail_wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, PHOTO_CONTAINER_SELECTOR)))
                product_data["photo_count"] = len(photo_container.find_elements(By.XPATH, "./*"))
                
                # Download and save images while we're still on the detail page
                # First add product to get an ID
                temp_product = add_product(seller, product_data)
                
                # Download images while on detail page
                image_urls = save_product_images(driver, temp_product, supabase_client)
                temp_product['images'] = image_urls
                
            except TimeoutException:
                # No photos found, still add product but without images
                temp_product = add_product(seller, product_data)
                temp_product['images'] = []

            try:
                detail_wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, LINK_ICON_SELECTOR))).click()
                link_elem = detail_wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, LINK_HREF_SELECTOR)))
                product_data["product_link"] = link_elem.get_attribute("href")
                temp_product["product_link"] = product_data["product_link"]
                # Go back from link page
                wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, BACK_BUTTON_SELECTOR))).click()
                time.sleep(0.3)
            except TimeoutException:
                 pass # No link button found

            # Go back from detail page
            wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, BACK_BUTTON_SELECTOR))).click()
            time.sleep(0.3)

            items_scraped += 1
            print(f"[{index + 1}] Scraped: {title} | Price: {price}")
            # --- End of single item processing ---

        except StaleElementReferenceException:
            print(f"[{index + 1}] Stale element reference. Re-fetching and retrying...")
            continue # The loop will re-fetch the `items` list
        except Exception as e:
            print(f"[{index + 1}] Error processing item: {e}")
            try:
                # Try to recover by going back to the list
                driver.find_element(By.CSS_SELECTOR, BACK_BUTTON_SELECTOR).click()
            except:
                pass

        index += 1

    return items_scraped

def setup_driver():
    print("🚀 Setting up browser with persistent profile...")
    if not os.path.exists(CHROME_PROFILE_PATH):
        os.makedirs(CHROME_PROFILE_PATH)
        print(f"Created profile directory at: {CHROME_PROFILE_PATH}")

    # Check if profile has been used before
    is_initialized = os.path.exists(os.path.join(CHROME_PROFILE_PATH, "Default", "Preferences"))
    print(f"📁 Profile status: {'Found existing profile' if is_initialized else 'Creating new profile'}")

    chrome_options = Options()
    
    # Stealth arguments matching original scraper.py exactly
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-setuid-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-web-security")
    chrome_options.add_argument("--disable-features=VizDisplayCompositor")
    chrome_options.add_argument("--no-first-run")
    chrome_options.add_argument("--no-default-browser-check")
    chrome_options.add_argument("--disable-background-timer-throttling")
    chrome_options.add_argument("--disable-backgrounding-occluded-windows")
    chrome_options.add_argument("--disable-renderer-backgrounding")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--headless=new")  # Comment out for GUI mode
    # Open dev console on boot for debugging
    chrome_options.add_argument("--auto-open-devtools-for-tabs")
    
    # Profile settings
    chrome_options.add_argument(f"--user-data-dir={os.path.abspath(CHROME_PROFILE_PATH)}")
    chrome_options.add_argument("--profile-directory=Default")
    
    # Anti-detection measures
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    
    # Additional stealth options matching original scraper.py
    chrome_options.add_argument("--lang=en-US")
    chrome_options.add_experimental_option("prefs", {
        "profile.default_content_setting_values.notifications": 1
    })

    try:
        service = Service() # Assumes chromedriver is in PATH
        driver = webdriver.Chrome(service=service, options=chrome_options)
        
        # Set window size to match original scraper.py
        driver.maximize_window()
        driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.CONTROL + Keys.SHIFT + 'i')
        
        # Additional stealth measures
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        driver.execute_cdp_cmd('Network.setUserAgentOverride', {
            "userAgent": 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })
        
        print("✅ Browser context ready with stealth mode")
        return driver
        
    except Exception as e:
        print(f"❌ Could not start driver: {e}")
        print("💡 Make sure chromedriver is in your PATH or install via 'brew install chromedriver'")
        return None

def handle_whatsapp_login(driver):
    print("🔐 Starting WhatsApp authentication...")
    
    driver.get(WHATSAPP_URL)
    
    # Add random delay to appear more human-like
    time.sleep(1 + 1 * 0.5)
    
    try:
        # Wait for page to load with longer timeout - matching original scraper.py
        wait = WebDriverWait(driver, 15)
        
        # Try to wait for either QR code or main chat (similar to original Promise.race)
        try:
            # First try to find QR code
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, QR_CODE_SELECTOR)))
            print("QR Code container found, checking login status...")
        except TimeoutException:
            # If QR not found, try main chat
            try:
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, MAIN_CHAT_SELECTOR)))
                print("Main chat found, checking login status...")
            except TimeoutException:
                print("❌ Error: WhatsApp Web page did not load correctly or timed out.")
                return False
        
        # Check if we're already logged in (like original scraper.py)
        try:
            main_chat = driver.find_element(By.CSS_SELECTOR, MAIN_CHAT_SELECTOR)
            if main_chat.is_displayed():
                print("✅ Already logged in to WhatsApp")
                return True
        except NoSuchElementException:
            pass
        
        # Handle QR code (like original scraper.py)
        try:
            qr_canvas = driver.find_element(By.CSS_SELECTOR, QR_CODE_SELECTOR)
            qr_element = qr_canvas.find_element(By.XPATH, '..')

            if qr_element.is_displayed():
                print("📱 QR Code detected. Extracting QR code data...")

                try:
                    # Wait for QR code to fully load
                    time.sleep(3)
                    
                    # Try to get data-ref from the parent of the canvas
                    qr_data = qr_element.get_attribute('data-ref')

                    # Display QR code based on data format (like original scraper.py)
                    if qr_data and qr_data.startswith('data:'):
                        # For data URLs, show message instead (like original scraper.py)
                        print('\n' + '=' * 50)
                        print('📱 QR CODE READY - Please scan with your phone')
                        print('=' * 50)
                        print('⚠️  Cannot display QR as ASCII (data URL format)')
                        print('💡 QR code is visible in WhatsApp Web')
                        print('⏳ Waiting for scan... (2 minutes timeout)')
                        print('=' * 50 + '\n')
                    elif qr_data and len(qr_data) > 10:
                        # Display QR code as ASCII in terminal
                        print('\n' + '=' * 50)
                        print('📱 QR CODE - Please scan with your phone')
                        print('=' * 50)
                        try:
                            qr = qrcode.QRCode()
                            qr.add_data(qr_data)
                            qr.make(fit=True)
                            qr.print_ascii()
                        except Exception as qr_error:
                            print(f"Error creating QR display: {qr_error}")
                            print("💡 QR code is visible in WhatsApp Web browser window")
                        print('=' * 50 + '\n')
                    else:
                        # Fallback: Take screenshot as backup method
                        print('📸 QR data extraction failed, taking screenshot as backup...')
                        qr_element.screenshot(QR_SCREENSHOT_FILE)
                        print(f"📸 QR Code screenshot saved to: {QR_SCREENSHOT_FILE}")
                        print('👆 Please scan the QR code from the file above!')

                except Exception as error:
                    print(f'⚠️  Error extracting QR code data: {error}')
                    print('📸 Taking screenshot as fallback...')
                    try:
                        qr_element.screenshot(QR_SCREENSHOT_FILE)
                        print(f"📸 QR Code screenshot saved to: {QR_SCREENSHOT_FILE}")
                    except Exception as ss_error:
                         print(f"❌ Failed to take screenshot: {ss_error}")

                # Wait for successful login (QR disappears, main chat appears) - like original scraper.py
                print("⏳ Waiting for authentication... (2 minutes timeout)")
                WebDriverWait(driver, 120).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, MAIN_CHAT_SELECTOR))
                )
                print("✅ Successfully logged in to WhatsApp!")
                print("💾 Session will be automatically saved to Chrome profile")

                # Clean up QR code screenshot if it was created
                if os.path.exists(QR_SCREENSHOT_FILE):
                    os.remove(QR_SCREENSHOT_FILE)
                
                # Add delay to let WhatsApp fully initialize (like original scraper.py)  
                time.sleep(3)
                
                return True
                
        except Exception as e:
            print(f"❌ Failed during QR code authentication process: {e}")
            return False
            
    except Exception as error:
        print(f"❌ Authentication failed: {error}")
        return False

def scrape_row(driver, row, index, supabase_client):
    start_time = time.time()
    seller_name = row["name"]

    seller_data = {
        "catalogue_url": row["catalogue_link"],
        "seller_name": seller_name,
        "seller_city": row["city"],
        "seller_contact": str(row["contact"])
    }

    print(f"\n[{index}] Scraping: {seller_name} ({seller_data['seller_city']})")

    # Get or create seller
    seller = get_or_create_seller(
        name=seller_name,
        city=seller_data['seller_city'],
        contact=seller_data['seller_contact'],
        catalogue_url=seller_data['catalogue_url']
    )

    try:
        driver.get(seller_data["catalogue_url"])

        wait = WebDriverWait(driver, 30)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ITEM_SELECTOR)))

        # Navigate to "All items" page. If it fails, it will still try to process items on the page.
        navigated = navigate_to_all_items_page(driver)
        if not navigated:
            print(f"⚠️ Could not navigate to 'All items' page for {seller_name}. Attempting to scrape current page.")

        count = process_catalog_items(driver, seller_data, seller, supabase_client)

        if count > 0:
            scrape_session["scrape_job"]["job_metadata"]["sellers_processed"].append(seller_name)

        print(f"✅ Scraped {count} items from {seller_name}")

    except Exception as e:
        print(f"❌ Timeout or error while scraping {seller_name}: {e}")
        count = 0  # Set count to 0 on failure

    elapsed = time.time() - start_time
    print(f"⏱️ Time taken for {seller_name}: {elapsed:.2f} seconds")

    return count

# ---------------------------
# Main Entry Point
# ---------------------------
if __name__ == "__main__":
    if not os.path.exists(CSV_FILE):
        print(f"❌ File not found: {CSV_FILE}")
        exit(1)

    total_start_time = time.time()

    # --- Initialize Supabase Client ---
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("❌ Supabase URL or Key not found. Please check your .env file.")
        exit(1)
    
    try:
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("✅ Successfully connected to Supabase.")
    except Exception as e:
        print(f"❌ Failed to connect to Supabase: {e}")
        exit(1)

    # --- Load Seller Data ---
    try:
        df = pd.read_csv(CSV_FILE)
        
        driver = setup_driver()
        if not driver:
            print("❌ Failed to setup selenium driver. Exiting.")
            exit(1)

        if not handle_whatsapp_login(driver):
            print("❌ WhatsApp login failed. Exiting.")
            driver.quit()
            exit(1)
        
        print("\n\n--- Starting Catalog Scraping ---\n")

        total_items = 0
        for i, row in df.iterrows():
            scraped_count = scrape_row(driver, row, i, supabase)
            total_items += scraped_count
            print('Scraped count: ', scraped_count)

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
        print(f"✅ Finished. Total items: {total_items}, Sellers: {len(scrape_session['sellers'])}")
        print(f"📄 Supabase-compatible JSON saved to {OUTPUT_FILE}")
        
        # Print summary for easy import
        print(f"\n📊 Summary for Supabase import:")
        print(f"   - Scrape Job ID: {scrape_session['scrape_job']['id']}")
        print(f"   - Sellers: {len(scrape_session['sellers'])}")
        print(f"   - Products: {len(scrape_session['products'])}")
        print(f"   - Status: {scrape_session['scrape_job']['status']}")
    except Exception as e:
        print(f"\n❌ An error occurred while scraping: {e}")
        exit(1)

    print(f"\n🕒 Total scraping time: {total_elapsed_time:.2f} seconds")
    print(f"✅ Finished. Total items: {total_items}, Sellers: {len(scrape_session['sellers'])}")
    print(f"📄 Supabase-compatible JSON saved to {OUTPUT_FILE}")
    
    # Print summary for easy import
    print(f"\n📊 Summary for Supabase import:")
    print(f"   - Scrape Job ID: {scrape_session['scrape_job']['id']}")
    print(f"   - Sellers: {len(scrape_session['sellers'])}")
    print(f"   - Products: {len(scrape_session['products'])}")
    print(f"   - Status: {scrape_session['scrape_job']['status']}") 