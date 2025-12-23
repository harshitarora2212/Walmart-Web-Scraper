"""
Universal Walmart Product Scraper
==================================
Multi-threaded web scraper for Walmart.com product data extraction.

Usage:
    python script.py [ZIP_CODES] [MODULE]
    
    ZIP_CODES: Comma-separated (e.g., "11581,95829") or "ALL"
    MODULE: DETAILS | REVIEWS | VARIATION | ALL | PARALLEL
    
Examples:
    python script.py 11581,95829 PARALLEL
    python script.py ALL DETAILS
    python script.py 10001 REVIEWS
"""

from bs4 import BeautifulSoup
import undetected_chromedriver as uc
import pandas as pd
import os
from datetime import datetime
import json
import re
import time
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
import glob
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# ============================================================================
# CONFIGURATION - Modify these for your environment
# ============================================================================

# Directory configuration
OUTPUT_DIR = r"C:\Walmart_Scraper"  # Main output directory
OUTPUT_DIR_TEMP = os.path.join(OUTPUT_DIR, "temp")
OUTPUT_DIR_FINAL = os.path.join(OUTPUT_DIR, "output")
CHROME_PROFILES_DIR = os.path.join(OUTPUT_DIR, "chrome_profiles")

# Input file: CSV with 'web_pid' column containing product IDs to scrape
INPUT_FILE = os.path.join(OUTPUT_DIR, "products_to_scrape.csv")

# Scraping parameters
MAX_RETRIES = 2  # Retry attempts for failed pages
REVIEW_PAGES = 2  # Number of review pages to scrape per product
MAX_THREADS = 3  # Concurrent browser instances

# Timestamps
TIMESTAMP = datetime.now().strftime("%Y-%m-%d")
TIMESTAMP_FILE = datetime.now().strftime("%d%m%Y")

# Create directories
for dir_path in [OUTPUT_DIR, OUTPUT_DIR_TEMP, OUTPUT_DIR_FINAL, CHROME_PROFILES_DIR]:
    os.makedirs(dir_path, exist_ok=True)

# Thread locks
file_lock = threading.Lock()
driver_lock = threading.Lock()

# ============================================================================
# CHROME DRIVER SETUP
# ============================================================================

def create_driver(headless=True, port=0, zip_code=None, module=None):
    """
    Create Chrome driver instance with bot detection avoidance.
    
    Args:
        headless: Run without UI
        port: Port offset for multiple instances
        zip_code: ZIP code for persistent profile
        module: Module name for profile separation
    """
    options = uc.ChromeOptions()
    
    # Anti-detection settings
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument(f"--remote-debugging-port={9222 + port}")
    options.add_argument("--disable-dev-shm-usage")
    
    # Use persistent profile to maintain session
    if zip_code and module:
        profile_dir = os.path.join(CHROME_PROFILES_DIR, f"{zip_code}_{module}")
        os.makedirs(profile_dir, exist_ok=True)
        options.add_argument(f"--user-data-dir={profile_dir}")
    
    if headless:
        options.add_argument("--headless=new")
    
    driver = uc.Chrome(options=options)
    driver.maximize_window()
    return driver

def create_driver_safe(headless=True, port=0, zip_code=None, module=None):
    """Thread-safe driver creation"""
    with driver_lock:
        return create_driver(headless, port, zip_code, module)

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def clean_text(text):
    """Remove special characters"""
    if not isinstance(text, str):
        return ""
    return re.sub(r"[^\w\s\.,!?;:\-]", "", text)

def extract_price(text):
    """Extract price from text (e.g., '$19.64')"""
    if not text:
        return "NA"
    match = re.search(r'\$[\d,]+\.?\d*', text)
    return match.group() if match else "NA"

def safe_csv_write(df, filepath, write_header):
    """Thread-safe CSV writing"""
    with file_lock:
        df.to_csv(filepath, mode='a', header=write_header, index=False)

def cleanup_old_files(directory):
    """Remove files not matching today's timestamp"""
    if not os.path.exists(directory):
        return
    
    for file in glob.glob(os.path.join(directory, "*")):
        if os.path.isfile(file) and TIMESTAMP not in os.path.basename(file):
            try:
                os.remove(file)
                print(f"Removed old file: {os.path.basename(file)}")
            except Exception as e:
                print(f"Error removing {file}: {e}")

# ============================================================================
# PRODUCT DETAILS SCRAPER
# ============================================================================

def scrape_details(zip_code, driver, thread_id):
    """
    Scrape product details and seller information.
    
    Extracts:
    - Title, brand, prices, ratings, reviews
    - Availability, shipping options
    - Seller information
    - Multiple seller offers
    """
    print(f"[{thread_id}] Starting details scraper for ZIP: {zip_code}")
    
    # File paths
    output_file = os.path.join(OUTPUT_DIR_TEMP, f"details_{zip_code}_{TIMESTAMP}.csv")
    sellers_file = os.path.join(OUTPUT_DIR_TEMP, f"sellers_{zip_code}_{TIMESTAMP}.csv")
    container_file = os.path.join(OUTPUT_DIR_TEMP, f"container_details_{zip_code}_{TIMESTAMP}.csv")
    
    # Initialize or load container (tracks progress)
    if os.path.exists(container_file):
        df_todo = pd.read_csv(container_file)
    else:
        with file_lock:
            df_input = pd.read_csv(INPUT_FILE)
            df_input.to_csv(container_file, index=False)
        df_todo = pd.read_csv(container_file)
    
    header_written = os.path.exists(output_file) and os.path.getsize(output_file) > 0
    seller_header = os.path.exists(sellers_file) and os.path.getsize(sellers_file) > 0
    
    # Process each product
    for _, row in df_todo.iterrows():
        web_pid = row['web_pid']
        url = f"https://www.walmart.com/product/{web_pid}"
        
        for attempt in range(MAX_RETRIES):
            try:
                driver.get(url)
                time.sleep(4)
                
                # Check for bot detection
                if "Robot or human" in driver.page_source:
                    print(f"[{thread_id}] Bot detected - stopping")
                    return
                
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                
                # Extract product data
                data = {
                    'web_pid': web_pid,
                    'url': url,
                    'title': 'NA',
                    'brand': 'NA',
                    'price_regular': 'NA',
                    'price_sale': 'NA',
                    'rating': 'NA',
                    'review_count': 'NA',
                    'availability': '2',  # 0=unavailable, 1=available, 2=not listed
                    'seller': 'NA',
                    'shipping': 'NA',
                    'zip_code': zip_code,
                    'scraped_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                
                # Title
                title_el = soup.find('h1', {'id': 'main-title'})
                if title_el:
                    data['title'] = clean_text(title_el.text)
                
                # Brand
                brand_el = soup.find('a', {'data-dca-name': 'ItemBrandLink'})
                if brand_el:
                    brand_text = brand_el.text.strip()
                    if brand_text.startswith("Visit the") and brand_text.endswith("Store"):
                        data['brand'] = brand_text[9:-5].strip()
                    else:
                        data['brand'] = brand_text
                
                # Prices
                price_reg = soup.find('span', {'data-seo-id': 'strike-through-price'})
                if price_reg:
                    data['price_regular'] = extract_price(price_reg.text)
                
                price_sale = soup.find('span', {'data-seo-id': 'hero-price'})
                if price_sale:
                    data['price_sale'] = extract_price(price_sale.text.replace('Now', ''))
                
                # Rating and reviews
                reviews_div = soup.find('div', {'data-testid': 'reviews-and-ratings'})
                if reviews_div:
                    rating_span = reviews_div.find('span', class_='w_iUH7')
                    if rating_span:
                        match = re.search(r'(\d+\.?\d*)\s+stars.*?(\d+)\s+reviews', rating_span.text)
                        if match:
                            data['rating'] = match.group(1)
                            data['review_count'] = match.group(2)
                
                # Availability
                if soup.find('span', string='Add to cart', attrs={'data-pcss-hide': 'true'}):
                    data['availability'] = '1'
                elif soup.find(string=re.compile('Not Available', re.I)):
                    data['availability'] = '0'
                
                # Seller
                seller_el = soup.find_all('span', {'data-testid': 'product-seller-info'})
                if seller_el:
                    seller_text = seller_el[-1].text
                    if "Walmart.com" in seller_text:
                        data['seller'] = "Walmart.com"
                    elif "by" in seller_text:
                        data['seller'] = clean_text(seller_text.rsplit('by', 1)[-1])
                
                # Shipping
                ship_el = soup.find('div', {'data-seo-id': re.compile('fulfillment.*shipping', re.I)})
                if ship_el:
                    data['shipping'] = clean_text(ship_el.text)
                
                # Save main data
                safe_csv_write(pd.DataFrame([data]), output_file, not header_written)
                header_written = True
                
                # Scrape additional sellers
                sellers = [{'web_pid': web_pid, 'seller': data['seller'], 
                           'price': data['price_sale'], 'zip_code': zip_code}]
                
                try:
                    # Click "Compare sellers" button
                    btn = WebDriverWait(driver, 5).until(
                        EC.element_to_be_clickable((
                            By.XPATH, 
                            '//button[contains(@aria-label, "Compare all")]'
                        ))
                    )
                    driver.execute_script("arguments[0].click();", btn)
                    time.sleep(3)
                    
                    # Extract other sellers
                    soup2 = BeautifulSoup(driver.page_source, 'html.parser')
                    for card in soup2.find_all('div', {'data-testid': 'ip-more-sellers-panel-offers-card-wrapper'}):
                        seller_name = 'NA'
                        price = 'NA'
                        
                        name_el = card.find('span', {'data-testid': 'product-seller-info'})
                        if name_el:
                            seller_name = clean_text(name_el.text.rsplit('by', 1)[-1])
                        
                        price_el = card.find(lambda t: t.name == 'span' and '$' in t.text)
                        if price_el:
                            price = extract_price(price_el.text)
                        
                        if seller_name != data['seller']:
                            sellers.append({
                                'web_pid': web_pid,
                                'seller': seller_name,
                                'price': price,
                                'zip_code': zip_code
                            })
                except:
                    pass  # No additional sellers
                
                # Save sellers
                safe_csv_write(pd.DataFrame(sellers), sellers_file, not seller_header)
                seller_header = True
                
                print(f"[{thread_id}] ✓ Scraped {web_pid}")
                break  # Success - exit retry loop
                
            except Exception as e:
                print(f"[{thread_id}] Error on {web_pid} (attempt {attempt+1}): {e}")
                if attempt == MAX_RETRIES - 1:
                    print(f"[{thread_id}] Failed {web_pid} after {MAX_RETRIES} attempts")
        
        # Remove from container
        with file_lock:
            df_todo = pd.read_csv(container_file)
            df_todo = df_todo[df_todo['web_pid'] != web_pid]
            df_todo.to_csv(container_file, index=False)
    
    # Cleanup container if empty
    if len(df_todo) == 0 and os.path.exists(container_file):
        os.remove(container_file)
    
    print(f"[{thread_id}] Details scraper complete")

# ============================================================================
# REVIEWS SCRAPER
# ============================================================================

def scrape_reviews(zip_code, driver, thread_id):
    """
    Scrape product reviews (runs only on Mondays).
    
    Extracts:
    - Review title, text, rating, date
    - Reviewer name
    - Response availability
    """
    print(f"[{thread_id}] Starting reviews scraper for ZIP: {zip_code}")
    
    # Only run on Mondays
    if datetime.today().weekday() != 0:
        print(f"[{thread_id}] Not Monday - skipping reviews")
        return
    
    output_file = os.path.join(OUTPUT_DIR_TEMP, f"reviews_{zip_code}_{TIMESTAMP}.csv")
    container_file = os.path.join(OUTPUT_DIR_TEMP, f"container_reviews_{zip_code}_{TIMESTAMP}.csv")
    
    # Initialize container
    if os.path.exists(container_file):
        df_todo = pd.read_csv(container_file)
    else:
        with file_lock:
            df_input = pd.read_csv(INPUT_FILE)
            df_input.to_csv(container_file, index=False)
        df_todo = pd.read_csv(container_file)
    
    header_written = os.path.exists(output_file) and os.path.getsize(output_file) > 0
    
    for _, row in df_todo.iterrows():
        web_pid = row['web_pid']
        url = f"https://www.walmart.com/reviews/product/{web_pid}"
        
        try:
            driver.get(url)
            time.sleep(5)
            
            # Check if reviews exist
            if "does not have any reviews" in driver.page_source:
                print(f"[{thread_id}] No reviews for {web_pid}")
                continue
            
            # Sort by most recent
            try:
                driver.find_element(By.XPATH, '//button[contains(., "Most relevant")]').click()
                time.sleep(1)
                driver.find_element(By.XPATH, '//span[text()="Most recent"]/parent::label').click()
                time.sleep(3)
            except:
                pass
            
            # Scrape multiple pages
            for page_num in range(REVIEW_PAGES):
                driver.refresh()
                time.sleep(3)
                
                # Extract from JSON data
                script = driver.find_element(By.ID, '__NEXT_DATA__').get_attribute('innerHTML')
                data = json.loads(script)
                reviews_data = data['props']['pageProps']['initialData']['data']['reviews']['customerReviews']
                
                reviews = []
                for review in reviews_data:
                    reviews.append({
                        'web_pid': web_pid,
                        'rating': review.get('rating', 'NA'),
                        'title': clean_text(review.get('reviewTitle', 'NA')),
                        'text': clean_text(review.get('reviewText', 'NA')),
                        'date': review.get('reviewSubmissionTime', 'NA'),
                        'review_id': review.get('reviewId', 'NA'),
                        'reviewer': review.get('userNickname', 'Anonymous'),
                        'has_response': 'Yes' if review.get('clientResponses') else 'No',
                        'scraped_at': datetime.now().strftime("%Y-%m-%d")
                    })
                
                safe_csv_write(pd.DataFrame(reviews), output_file, not header_written)
                header_written = True
                
                # Next page
                if page_num < REVIEW_PAGES - 1:
                    try:
                        next_btn = driver.find_element(By.XPATH, '//a[@data-testid="NextPage"]')
                        if 'disabled' in next_btn.get_attribute('class'):
                            break
                        driver.execute_script("arguments[0].click();", next_btn)
                        time.sleep(4)
                    except:
                        break
            
            print(f"[{thread_id}] ✓ Scraped reviews for {web_pid}")
            
        except Exception as e:
            print(f"[{thread_id}] Error scraping reviews for {web_pid}: {e}")
        
        # Remove from container
        with file_lock:
            df_todo = pd.read_csv(container_file)
            df_todo = df_todo[df_todo['web_pid'] != web_pid]
            df_todo.to_csv(container_file, index=False)
    
    if len(df_todo) == 0 and os.path.exists(container_file):
        os.remove(container_file)
    
    print(f"[{thread_id}] Reviews scraper complete")

# ============================================================================
# VARIATIONS SCRAPER
# ============================================================================

def scrape_variations(zip_code, driver, thread_id):
    """
    Scrape product variations (size, color, flavor).
    
    Extracts:
    - Variant ASINs and URLs
    - Variant prices and sellers
    - Size, color, style information
    """
    print(f"[{thread_id}] Starting variations scraper for ZIP: {zip_code}")
    
    output_file = os.path.join(OUTPUT_DIR_TEMP, f"variations_{zip_code}_{TIMESTAMP}.csv")
    container_file = os.path.join(OUTPUT_DIR_TEMP, f"container_variations_{zip_code}_{TIMESTAMP}.csv")
    
    if os.path.exists(container_file):
        df_todo = pd.read_csv(container_file)
    else:
        with file_lock:
            df_input = pd.read_csv(INPUT_FILE)
            df_input.to_csv(container_file, index=False)
        df_todo = pd.read_csv(container_file)
    
    header_written = os.path.exists(output_file) and os.path.getsize(output_file) > 0
    
    for _, row in df_todo.iterrows():
        master_pid = row['web_pid']
        url = f"https://www.walmart.com/product/{master_pid}"
        
        try:
            driver.get(url)
            time.sleep(4)
            
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            
            # Find all variant URLs
            variant_urls = [url]  # Include master
            variant_group = soup.find('div', {'data-testid': 'variantGroup'})
            if variant_group:
                for link in variant_group.find_all('a', href=True):
                    full_url = f"https://www.walmart.com{link['href']}"
                    if full_url not in variant_urls:
                        variant_urls.append(full_url)
            
            variations = []
            for var_url in variant_urls:
                try:
                    driver.get(var_url)
                    time.sleep(3)
                    
                    soup = BeautifulSoup(driver.page_source, 'html.parser')
                    variant_pid = var_url.rstrip('/').split('/')[-1]
                    
                    # Extract data
                    var_data = {
                        'master_asin': master_pid,
                        'variant_asin': variant_pid,
                        'seller': 'NA',
                        'price': 'NA',
                        'size': 'NA',
                        'color': 'NA',
                        'style': 'NA',
                        'zip_code': zip_code
                    }
                    
                    # Seller
                    seller_el = soup.find('span', {'data-testid': 'product-seller-info'})
                    if seller_el:
                        seller_text = seller_el.text
                        if "Walmart.com" in seller_text:
                            var_data['seller'] = "Walmart.com"
                        elif "by" in seller_text:
                            var_data['seller'] = clean_text(seller_text.rsplit('by', 1)[-1])
                    
                    # Price
                    price_el = soup.find('span', {'data-seo-id': 'hero-price'})
                    if price_el:
                        var_data['price'] = extract_price(price_el.text)
                    
                    # Size
                    for label in soup.find_all('span', class_='b'):
                        if 'pack size' in label.text.lower():
                            sibling = label.find_next_sibling('span')
                            if sibling:
                                var_data['size'] = sibling.text.strip()
                    
                    variations.append(var_data)
                    print(f"[{thread_id}] ✓ Variant {variant_pid}")
                    
                except Exception as e:
                    print(f"[{thread_id}] Error on variant {var_url}: {e}")
            
            safe_csv_write(pd.DataFrame(variations), output_file, not header_written)
            header_written = True
            
        except Exception as e:
            print(f"[{thread_id}] Error scraping variations for {master_pid}: {e}")
        
        # Remove from container
        with file_lock:
            df_todo = pd.read_csv(container_file)
            df_todo = df_todo[df_todo['web_pid'] != master_pid]
            df_todo.to_csv(container_file, index=False)
    
    if len(df_todo) == 0 and os.path.exists(container_file):
        os.remove(container_file)
    
    print(f"[{thread_id}] Variations scraper complete")

# ============================================================================
# DATA CONSOLIDATION
# ============================================================================

def consolidate_to_excel(zip_codes, modules):
    """Combine all CSV data into Excel files"""
    print("Consolidating data to Excel...")
    
    for zip_code in zip_codes:
        excel_file = os.path.join(OUTPUT_DIR_FINAL, f"walmart_{zip_code}_{TIMESTAMP_FILE}.xlsx")
        
        with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
            # Details
            if 'details' in modules:
                details_csv = os.path.join(OUTPUT_DIR_TEMP, f"details_{zip_code}_{TIMESTAMP}.csv")
                if os.path.exists(details_csv):
                    df = pd.read_csv(details_csv).fillna('NA')
                    df.drop_duplicates(subset=['web_pid'], inplace=True)
                    df.to_excel(writer, sheet_name='Details', index=False)
                    os.remove(details_csv)
                
                sellers_csv = os.path.join(OUTPUT_DIR_TEMP, f"sellers_{zip_code}_{TIMESTAMP}.csv")
                if os.path.exists(sellers_csv):
                    df = pd.read_csv(sellers_csv).fillna('NA')
                    df.drop_duplicates(subset=['web_pid', 'seller'], inplace=True)
                    df.to_excel(writer, sheet_name='Sellers', index=False)
                    os.remove(sellers_csv)
            
            # Reviews
            if 'reviews' in modules:
                reviews_csv = os.path.join(OUTPUT_DIR_TEMP, f"reviews_{zip_code}_{TIMESTAMP}.csv")
                if os.path.exists(reviews_csv):
                    df = pd.read_csv(reviews_csv).fillna('NA')
                    df.drop_duplicates(subset=['review_id'], inplace=True)
                    df.to_excel(writer, sheet_name='Reviews', index=False)
                    os.remove(reviews_csv)
            
            # Variations
            if 'variations' in modules:
                var_csv = os.path.join(OUTPUT_DIR_TEMP, f"variations_{zip_code}_{TIMESTAMP}.csv")
                if os.path.exists(var_csv):
                    df = pd.read_csv(var_csv).fillna('NA')
                    df.drop_duplicates(subset=['variant_asin'], inplace=True)
                    df.to_excel(writer, sheet_name='Variations', index=False)
                    os.remove(var_csv)
        
        print(f"Created: {excel_file}")

# ============================================================================
# THREAD MANAGEMENT
# ============================================================================

def run_module_thread(zip_code, module, func, port):
    """Run scraper module in thread with dedicated driver"""
    driver = None
    thread_id = f"{module}-{zip_code}"
    try:
        print(f"[{thread_id}] Starting")
        driver = create_driver_safe(headless=False, port=port, zip_code=zip_code, module=module)
        func(zip_code, driver, thread_id)
        return f"[{thread_id}] Success"
    except Exception as e:
        return f"[{thread_id}] Failed: {e}"
    finally:
        if driver:
            try:
                driver.quit()
            except:
                pass

def run_parallel(zip_codes, modules):
    """Execute scraping with max 3 concurrent threads"""
    print(f"Starting parallel execution for {zip_codes}")
    
    # Build task queue
    tasks = []
    module_funcs = {
        'details': scrape_details,
        'reviews': scrape_reviews,
        'variations': scrape_variations
    }
    
    for zip_code in zip_codes:
        for module in modules:
            if module in module_funcs:
                tasks.append((zip_code, module, module_funcs[module]))
    
    print(f"Task queue: {len(tasks)} tasks")
    
    # Execute with thread pool
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = {}
        task_idx = 0
        
        # Start first 3 tasks
        for i in range(min(MAX_THREADS, len(tasks))):
            zip_code, module, func = tasks[task_idx]
            future = executor.submit(run_module_thread, zip_code, module, func, i)
            futures[future] = (zip_code, module, task_idx)
            task_idx += 1
        
        # As tasks complete, start new ones
        while futures:
            for done_future in as_completed(futures.keys()):
                zip_code, module, idx = futures[done_future]
                
                try:
                    result = done_future.result()
                    print(f"COMPLETE: {result}")
                except Exception as e:
                    print(f"FAILED: {zip_code}-{module}: {e}")
                
                del futures[done_future]
                
                # Start next task if available
                if task_idx < len(tasks):
                    zip_code, module, func = tasks[task_idx]
                    port = len(futures)  # Use available port
                    new_future = executor.submit(run_module_thread, zip_code, module, func, port)
                    futures[new_future] = (zip_code, module, task_idx)
                    task_idx += 1
                
                break
    
    print("✓ All tasks complete")

# ============================================================================
# MAIN EXECUTION
# ============================================================================

if __name__ == "__main__":
    start_time = datetime.now()
    
    # Parse arguments
    if len(sys.argv) < 3:
        print("Usage: python script.py [ZIP_CODES] [MODULE]")
        print("ZIP_CODES: Comma-separated or 'ALL'")
        print("MODULE: DETAILS | REVIEWS | VARIATION | ALL | PARALLEL")
        sys.exit(1)
    
    # Parse ZIP codes
    raw_zip = sys.argv[1]
    if raw_zip.upper() == 'ALL':
        zip_codes = ['11581', '95829']  # Default ZIP codes
    else:
        zip_codes = [z.strip() for z in raw_zip.split(',')]
    
    # Parse modules
    module_input = sys.argv[2].upper()
    module_map = {
        'ALL': ['details', 'reviews', 'variations'],
        'PARALLEL': ['details', 'reviews', 'variations'],
        'DETAILS': ['details'],
        'REVIEWS': ['reviews'],
        'VARIATION': ['variations']
    }
    
    modules = module_map.get(module_input)
    if not modules:
        print(f"Invalid module: {module_input}")
        sys.exit(1)
    
    print(f"ZIP codes: {zip_codes}")
    print(f"Modules: {modules}")
    
    # Cleanup old files
    cleanup_old_files(OUTPUT_DIR_TEMP)
    
    # Check if already complete today
    final_file = os.path.join(OUTPUT_DIR_FINAL, f"walmart_{zip_codes[0]}_{TIMESTAMP_FILE}.xlsx")
    if os.path.exists(final_file):
        print("Already complete for today")
        sys.exit(0)
    
    # Run scraping
    try:
        # Parallel execution option
        if module_input == 'PARALLEL':
            run_parallel(zip_codes, modules)
        else:
            # Sequential execution: call each module per ZIP code
            module_funcs = {
                'details': scrape_details,
                'reviews': scrape_reviews,
                'variations': scrape_variations
            }
            for z in zip_codes:
                for m in modules:
                    func = module_funcs.get(m)
                    if not func:
                        continue
                    print(f"Running {m} for {z}")
                    # reuse run_module_thread to create/cleanup driver safely
                    result = run_module_thread(z, m, func, port=0)
                    print(result)
    
        # After sequential/parallel run consolidate results
        consolidate_to_excel(zip_codes, modules)

    except KeyboardInterrupt:
        print("Interrupted by user")
    except Exception as e:
        print(f"Fatal error: {e}")
    finally:
        elapsed = datetime.now() - start_time
        print(f"Elapsed: {elapsed}")
        print("Done")
