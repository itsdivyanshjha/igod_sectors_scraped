"""
iGOD Portal Scraper - Complete Final Version (Captures ALL rows)
Scrapes all sectors and their associated portals from igod.gov.in
Features:
- Handles lazy loading with scrolling
- Checks link status (working/dead)
- Skips "Details" buttons but marks them
- Captures EVERYTHING including invalid/empty rows
- Comprehensive logging
"""

import pandas as pd
import os
import time
from datetime import datetime
import re
from urllib.parse import urljoin
import requests

from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager


class IGODSectorsScraper:
    def __init__(self):
        self.base_url = "https://igod.gov.in"
        self.sectors_url = "https://igod.gov.in/sectors"
        self.all_data = []
        self.driver = None
        self.s_no = 1
        
        # Statistics tracking
        self.stats = {
            'total_sectors': 0,
            'total_rows': 0,
            'total_portals': 0,
            'working_links': 0,
            'dead_links': 0,
            'details_buttons': 0,
            'empty_rows': 0,
            'no_url': 0
        }
        
        # Create output directory
        os.makedirs('output', exist_ok=True)
        
        self.setup_selenium()
    
    def setup_selenium(self):
        """Initialize Selenium WebDriver"""
        chrome_options = Options()
        # Enable headless mode for production
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.page_load_strategy = 'normal'
        
        try:
            print("🔄 Setting up Selenium WebDriver...")
            download_path = ChromeDriverManager().install()
            download_dir = os.path.dirname(download_path)
            
            # Find and set executable permissions
            for file in os.listdir(download_dir):
                file_path = os.path.join(download_dir, file)
                if file == 'chromedriver' and os.path.isfile(file_path):
                    if not os.access(file_path, os.X_OK):
                        os.chmod(file_path, 0o755)
                    actual_driver = file_path
                    break
            
            self.driver = webdriver.Chrome(
                service=ChromeService(actual_driver),
                options=chrome_options
            )
            self.driver.set_page_load_timeout(90)
            self.driver.maximize_window()
            print("✓ Selenium WebDriver initialized successfully\n")
            
        except Exception as e:
            print(f"⚠ Selenium Error: {e}")
            self.driver = None
    
    def check_url_status(self, url):
        """Check if URL is working or dead"""
        if not url:
            return "No URL"
        
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            response = requests.head(url, headers=headers, timeout=10, allow_redirects=True)
            
            if response.status_code < 400:
                return "Working"
            elif response.status_code == 404:
                return "Dead (404)"
            elif response.status_code == 403:
                return "Error (403)"
            elif response.status_code >= 500:
                return "Server Error"
            else:
                return f"Error ({response.status_code})"
        except requests.exceptions.Timeout:
            return "Timeout"
        except requests.exceptions.ConnectionError:
            return "Connection Failed"
        except requests.exceptions.TooManyRedirects:
            return "Too Many Redirects"
        except Exception as e:
            return f"Error: {str(e)[:30]}"
    
    def fetch_sectors_page(self):
        """Load the sectors page"""
        if not self.driver:
            print("✗ Driver not initialized")
            return False
            
        try:
            print(f"{'='*70}")
            print("📥 LOADING SECTORS PAGE")
            print(f"{'='*70}\n")
            
            self.driver.get(self.sectors_url)
            
            # Wait for the page to load
            WebDriverWait(self.driver, 30).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            # Wait for sector boxes to appear
            print("⏳ Waiting for sector boxes to load...")
            WebDriverWait(self.driver, 30).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "a.sector-box"))
            )
            
            # Additional wait for dynamic content
            time.sleep(5)
            
            print("✓ Sectors page loaded successfully\n")
            return True
            
        except Exception as e:
            print(f"✗ Error loading page: {e}")
            return False
    
    def get_sector_urls(self):
        """Extract all sector URLs first"""
        sector_data = []
        
        try:
            sector_elements = self.driver.find_elements(By.CSS_SELECTOR, "a.sector-box")
            
            print(f"{'='*70}")
            print(f"📊 FOUND {len(sector_elements)} SECTORS")
            print(f"{'='*70}\n")
            
            self.stats['total_sectors'] = len(sector_elements)
            
            for idx, elem in enumerate(sector_elements, 1):
                try:
                    sector_url = elem.get_attribute("href")
                    
                    try:
                        h4_elem = elem.find_element(By.TAG_NAME, "h4")
                        sector_name = h4_elem.text.strip()
                    except:
                        sector_name = elem.get_attribute("title") or elem.text.strip() or "Unknown"
                    
                    if sector_url and sector_name:
                        sector_data.append({
                            'name': sector_name,
                            'url': sector_url
                        })
                        print(f"   [{idx:2d}] {sector_name}")
                        
                except Exception as e:
                    print(f"   [✗] Error extracting sector: {e}")
                    continue
            
            print()
            return sector_data
            
        except Exception as e:
            print(f"✗ Error getting sector URLs: {e}")
            return []
    
    def scroll_to_load_all_results(self):
        """Scroll down the page to load all lazy-loaded results"""
        last_height = self.driver.execute_script("return document.body.scrollHeight")
        scroll_attempts = 0
        max_attempts = 20
        
        while scroll_attempts < max_attempts:
            current_results = len(self.driver.find_elements(By.CLASS_NAME, "search-result-row"))
            
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(3)
            
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            new_results = len(self.driver.find_elements(By.CLASS_NAME, "search-result-row"))
            
            if new_results == current_results and new_height == last_height:
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight - 500);")
                time.sleep(1)
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(3)
                
                final_results = len(self.driver.find_elements(By.CLASS_NAME, "search-result-row"))
                
                if final_results == new_results:
                    return final_results
            
            last_height = new_height
            scroll_attempts += 1
        
        self.driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(1)
        return len(self.driver.find_elements(By.CLASS_NAME, "search-result-row"))
    
    def extract_all_sectors(self):
        """Extract all sectors and their portals"""
        if not self.driver:
            return
        
        sector_data = self.get_sector_urls()
        
        if not sector_data:
            print("✗ No sectors found!")
            return
        
        total_sectors = len(sector_data)
        
        print(f"{'='*70}")
        print(f"🔍 STARTING EXTRACTION OF {total_sectors} SECTORS")
        print(f"{'='*70}\n")
        
        for idx, sector in enumerate(sector_data, 1):
            sector_name = sector['name']
            sector_url = sector['url']
            
            try:
                print(f"{'─'*70}")
                print(f"[{idx}/{total_sectors}] SECTOR: {sector_name}")
                print(f"{'─'*70}")
                
                self.driver.get(sector_url)
                time.sleep(4)
                
                try:
                    WebDriverWait(self.driver, 15).until(
                        EC.presence_of_element_located((By.CLASS_NAME, "search-result-row"))
                    )
                    
                    print("   ⏳ Scrolling to load all results...")
                    total_rows = self.scroll_to_load_all_results()
                    print(f"   ✓ Loaded {total_rows} result rows")
                    
                    portal_count = self.extract_sector_portals(sector_name)
                    
                    print(f"   ✅ Captured {portal_count} rows from this sector\n")
                    
                except Exception as e:
                    print(f"   ❌ No results found: {e}\n")
                
                time.sleep(2)
                
            except Exception as e:
                print(f"   ❌ Error processing sector: {e}\n")
                continue
        
        print(f"\n{'='*70}")
        print(f"✅ EXTRACTION COMPLETE")
        print(f"{'='*70}\n")
        self.print_statistics()
    
    def extract_sector_portals(self, sector_name):
        """Extract ALL portals/rows from current sector page - NO SKIPPING"""
        portal_count = 0
        
        try:
            time.sleep(2)
            result_rows = self.driver.find_elements(By.CLASS_NAME, "search-result-row")
            
            print(f"   📋 Processing {len(result_rows)} rows...")
            
            self.stats['total_rows'] += len(result_rows)
            
            for idx, row in enumerate(result_rows, 1):
                try:
                    # Find the main portal link (a.search-title)
                    link = row.find_element(By.CSS_SELECTOR, "a.search-title")
                    
                    # Get portal URL
                    portal_url = link.get_attribute("href")
                    
                    # Get the innerHTML
                    inner_html = link.get_attribute("innerHTML")
                    
                    # Extract portal name
                    portal_name = None
                    if inner_html:
                        text_before_span = inner_html.split('<span')[0]
                        portal_name = re.sub('<[^<]+?>', '', text_before_span)
                        portal_name = ' '.join(portal_name.split()).strip()
                    
                    # Determine status
                    status = None
                    
                    # Check if it's a Details button (internal link)
                    if portal_url and "igod.gov.in/organization" in portal_url:
                        status = "Details Button (Skipped)"
                        self.stats['details_buttons'] += 1
                        print(f"      [{idx:3d}] Details button (internal link)... ⊘ Skipped")
                    
                    # Check if portal name is empty
                    elif not portal_name or len(portal_name) < 2:
                        status = "Empty/No Name"
                        self.stats['empty_rows'] += 1
                        portal_name = portal_name or "(Empty)"
                        print(f"      [{idx:3d}] Empty row... ⚠ No Name")
                    
                    # Check if no URL
                    elif not portal_url:
                        status = "No URL"
                        self.stats['no_url'] += 1
                        print(f"      [{idx:3d}] {portal_name[:50]}... ⚠ No URL")
                    
                    # Valid portal - check status
                    else:
                        print(f"      [{idx:3d}] Checking: {portal_name[:50]}...", end=" ")
                        status = self.check_url_status(portal_url)
                        
                        if status == "Working":
                            print("✓ Working")
                            self.stats['working_links'] += 1
                            self.stats['total_portals'] += 1
                        else:
                            print(f"✗ {status}")
                            self.stats['dead_links'] += 1
                            self.stats['total_portals'] += 1
                    
                    # Add EVERYTHING to data
                    self.all_data.append({
                        'S.No': self.s_no,
                        'Sector': sector_name,
                        'Portal Name': portal_name or "(Empty)",
                        'Portal URL': portal_url or "(No URL)",
                        'Status': status
                    })
                    
                    self.s_no += 1
                    portal_count += 1
                
                except Exception as e:
                    # Even if there's an error, add a row
                    print(f"      [{idx:3d}] Error extracting row... ✗ {str(e)[:40]}")
                    self.all_data.append({
                        'S.No': self.s_no,
                        'Sector': sector_name,
                        'Portal Name': f"(Error: {str(e)[:30]})",
                        'Portal URL': "(Error)",
                        'Status': f"Extraction Error"
                    })
                    self.s_no += 1
                    portal_count += 1
                    continue
            
        except Exception as e:
            print(f"   ❌ Error extracting portals: {e}")
        
        return portal_count
    
    def print_statistics(self):
        """Print final statistics"""
        print("📊 FINAL STATISTICS")
        print(f"{'='*70}")
        print(f"   Total Sectors Processed:     {self.stats['total_sectors']}")
        print(f"   Total Rows Captured:         {self.stats['total_rows']}")
        print(f"   Valid Portals:               {self.stats['total_portals']}")
        print(f"   Working Links:               {self.stats['working_links']} ({self.stats['working_links']*100//max(1,self.stats['total_portals'])}%)")
        print(f"   Dead/Error Links:            {self.stats['dead_links']} ({self.stats['dead_links']*100//max(1,self.stats['total_portals'])}%)")
        print(f"   Details Buttons:             {self.stats['details_buttons']}")
        print(f"   Empty/No Name Rows:          {self.stats['empty_rows']}")
        print(f"   Rows with No URL:            {self.stats['no_url']}")
        print(f"{'='*70}\n")
    
    def save_to_csv(self):
        """Save extracted data to CSV"""
        if not self.all_data:
            print("⚠️  No data to save")
            return
        
        try:
            df = pd.DataFrame(self.all_data)
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            csv_file = f"output/igod_sectors_portals_{timestamp}.csv"
            
            df.to_csv(csv_file, index=False, encoding='utf-8-sig')
            
            print(f"{'='*70}")
            print("💾 DATA SAVED SUCCESSFULLY!")
            print(f"{'='*70}")
            print(f"✓ File: {csv_file}")
            print(f"✓ Total Records: {len(self.all_data)}")
            print(f"✓ Unique Sectors: {df['Sector'].nunique()}")
            print(f"{'='*70}\n")
            
            # Sector breakdown
            print("📊 BREAKDOWN BY SECTOR:")
            print(f"{'─'*70}")
            sector_counts = df['Sector'].value_counts().sort_index()
            for sector, count in sector_counts.items():
                working = len(df[(df['Sector'] == sector) & (df['Status'] == 'Working')])
                print(f"   • {sector:40s}: {count:3d} rows ({working} working)")
            print()
            
            # Status breakdown
            print("📊 BREAKDOWN BY STATUS:")
            print(f"{'─'*70}")
            status_counts = df['Status'].value_counts()
            for status, count in status_counts.items():
                print(f"   • {status:30s}: {count:3d} ({count*100//len(df)}%)")
            print()
            
            # Preview
            print("📋 DATA PREVIEW (First 10 rows):")
            print(f"{'─'*70}")
            print(df.head(10).to_string(index=False))
            print()
            
        except Exception as e:
            print(f"❌ Error saving CSV: {e}")
    
    def scrape(self):
        """Main scraping function"""
        print("\n" + "="*70)
        print("🌐 iGOD SECTORS & PORTALS SCRAPER - CAPTURES EVERYTHING")
        print("="*70 + "\n")
        
        start_time = time.time()
        
        if not self.driver:
            print("❌ Cannot proceed without Selenium driver")
            return
        
        try:
            if not self.fetch_sectors_page():
                return
            
            self.extract_all_sectors()
            self.save_to_csv()
            
            elapsed = time.time() - start_time
            print(f"⏱️  Total time: {elapsed//60:.0f}m {elapsed%60:.0f}s\n")
            
        except KeyboardInterrupt:
            print("\n\n⚠️  Scraping interrupted by user!")
            print(f"   Scraped {len(self.all_data)} entries before interruption")
            
            if self.all_data:
                print("   Saving partial data...")
                self.save_to_csv()
        
        except Exception as e:
            print(f"\n❌ Scraping error: {e}")
        
        finally:
            if self.driver:
                self.driver.quit()
                print("✓ Browser closed\n")


if __name__ == "__main__":
    scraper = IGODSectorsScraper()
    scraper.scrape()
