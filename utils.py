import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from lxml import html
import logging
import json
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pickle
import time
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
import logging.handlers
from selenium_stealth import stealth  # Add this import
import concurrent.futures
import psutil  # Add this import
import tempfile
from filelock import FileLock
import hashlib
from concurrent.futures import ThreadPoolExecutor
import signal
import atexit
import sys
import portalocker  # Add this import for better cross-process file locking
import contextlib

def setup_logger(debug=False):
    """Setup logger with file and console output"""
    class CleanFormatter(logging.Formatter):
        def format(self, record):
            # Remove selenium debugging noise
            if 'selenium' in record.name.lower() and record.levelno < logging.WARNING:
                return ""
            if 'urllib3' in record.name.lower() and record.levelno < logging.WARNING:
                return ""
                
            # Clean up common noise in messages
            msg = record.getMessage()
            if 'http://localhost' in msg:
                return ""
            if 'Remote response' in msg:
                return ""
            if 'Finished Request' in msg:
                return ""
            
            # Format timestamp without milliseconds
            record.asctime = self.formatTime(record, "%Y-%m-%d %H:%M:%S")
            return f"{record.asctime} - {record.levelname} - {record.getMessage()}"

    logger = logging.getLogger(__name__)
    logger.handlers = []  # Clear existing handlers

    file_handler = logging.handlers.RotatingFileHandler(
        'crawler.log',
        maxBytes=1024*1024,  # 1MB
        backupCount=3,
        encoding='utf-8'
    )
    file_handler.setFormatter(CleanFormatter())

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(CleanFormatter())

    if debug:
        logger.setLevel(logging.DEBUG)
        console_handler.setLevel(logging.DEBUG)
        file_handler.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.WARNING)  # Change from ERROR to WARNING
        console_handler.setLevel(logging.WARNING)  # Change from ERROR to WARNING 
        file_handler.setLevel(logging.WARNING)  # Change from ERROR to WARNING

    # Set third party loggers to higher level
    logging.getLogger('selenium').setLevel(logging.ERROR)  # Increase severity
    logging.getLogger('urllib3').setLevel(logging.ERROR)  # Increase severity
    logging.getLogger('requests').setLevel(logging.ERROR)  # Increase severity

    return logger

def save_debug_html(url, content, folder="debug"):
    """Save HTML content for debugging"""
    if not os.path.exists(folder):
        os.makedirs(folder)
    
    safe_url = url.split('//')[-1].replace('/', '_')
    filepath = os.path.join(folder, f"{safe_url}.html")
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    logging.debug(f"Saved HTML content to {filepath}")

class DownloadStatus:
    def __init__(self):
        self.successful = []
        self.failed = []

    def add_success(self, url, filepath):
        self.successful.append((url, filepath))

    def add_failure(self, url, error):
        self.failed.append((url, str(error)))

    def get_summary(self):
        return {
            'successful': self.successful,
            'failed': self.failed
        }

def download_worker(args):
    """Modified worker function to support retry mode"""
    url, filename, folder, retry_mode = args
    success, error = download_file(url, filename, folder, retry_mode)
    return (url, filename, folder, success, error)

def get_optimal_workers():
    """Calculate optimal number of download workers based on system resources"""
    try:
        cpu_count = os.cpu_count() or 2
        memory = psutil.virtual_memory()
        # Use 75% of CPU cores, minimum 2, maximum 8
        cpu_optimal = max(2, min(8, int(cpu_count * 0.75)))
        # Reduce workers if memory usage is high (>80%)
        if (memory.percent > 80):
            return max(2, cpu_optimal - 2)
        return cpu_optimal
    except:
        return 4  # Default fallback

def get_user_workers():
    """Get number of workers from user input"""
    print("\nWorker Configuration:")
    print("1. Auto-detect optimal workers")
    print("2. Manually specify workers")
    choice = input("Enter choice (1/2): ").strip()
    
    if choice == "1":
        optimal = get_optimal_workers()
        print(f"\nDetected optimal workers: {optimal}")
        return optimal
    else:
        while True:
            try:
                workers = int(input("\nEnter number of workers (2-8 recommended): "))
                if workers > 12:
                    print("Warning: High number of workers may cause issues!")
                    confirm = input("Continue anyway? (y/n): ").lower()
                    if confirm != 'y':
                        continue
                if workers > 0:
                    return workers
            except ValueError:
                print("Please enter a valid number")

class BatchProcessor:
    def __init__(self, batch_size=5, max_workers=None):
        self.batch_size = batch_size
        self.max_workers = max_workers or get_optimal_workers()
        
    def process_batches(self, items):
        """Process items in batches"""
        for i in range(0, len(items), self.batch_size):
            batch = items[i:i + self.batch_size]
            yield batch

def hide_prints():
    """Context manager to hide print statements"""
    class DummyFile:
        def write(self, x): pass
        def flush(self): pass
    
    @contextlib.contextmanager
    def silent_prints():
        save_stdout = sys.stdout
        sys.stdout = DummyFile()
        try:
            yield
        finally:
            sys.stdout = save_stdout
    
    return silent_prints()

def download_files_parallel(urls, filenames, folders, max_workers=None, batch_size=5, retry_mode=False):
    """Download multiple files in parallel with batching and deduplication"""
    # Only get user input if max_workers is None and we haven't asked before
    if max_workers is None and not hasattr(download_files_parallel, 'cached_workers'):
        download_files_parallel.cached_workers = get_user_workers()
    
    if max_workers is None:
        max_workers = download_files_parallel.cached_workers
    
    processor = BatchProcessor(batch_size=batch_size, max_workers=max_workers)
    status = DownloadStatus()
    
    # Create batches of download tasks
    tasks = [(url, filename, folder, retry_mode) 
            for url, filename, folder in zip(urls, filenames, folders)]
    
    print(f"Using {max_workers} workers")

    results = []
    with hide_prints():  # Hide detailed download messages
        for batch in processor.process_batches(tasks):
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                batch_results = list(executor.map(download_worker, batch))
                results.extend(batch_results)
                
                for url, filename, folder, success, error in batch_results:
                    filepath = os.path.join(folder, filename)
                    if success:
                        status.add_success(url, filepath)
                    else:
                        status.add_failure(url, error)
                    
        # Small delay between batches
        time.sleep(0.5)
    
    # Print summary after all batches
    if status.successful:
        print("\nSuccessfully downloaded:")
        for url, filepath in status.successful:
            print(f"✓ {os.path.basename(filepath)} from {url}")
    
    if status.failed:
        print("\nFailed downloads:")
        for url, error in status.failed:
            print(f"✗ {url} - Error: {error}")
            
    return [r[3] for r in results], status

# Add global lock tracking
active_locks = set()

def cleanup_locks():
    """Clean up any remaining lock files"""
    for lock_file in active_locks:
        try:
            if os.path.exists(lock_file):
                os.unlink(lock_file)
        except:
            pass

# Register cleanup function
atexit.register(cleanup_locks)

def download_file(url, filename, folder="downloads", retry_mode=False):
    """Thread-safe and process-safe file download with robust locking"""
    lock_file = os.path.join(folder, f"{filename}.lock")
    
    try:
        if not os.path.exists(folder):
            os.makedirs(folder, exist_ok=True)
        
        filepath = os.path.join(folder, filename)
        
        # Skip locking in retry mode
        if retry_mode:
            return _do_download(url, filepath)
        
        # Use portalocker for cross-process locking
        with portalocker.Lock(lock_file, timeout=60):
            if os.path.exists(filepath):  # Check again after acquiring lock
                return True, None
                
            result = _do_download(url, filepath)
            return result
            
    except portalocker.exceptions.LockException:
        # If we timeout waiting for lock, skip this file
        return False, "File locked by another process"
    except Exception as e:
        return False, str(e)
    finally:
        try:
            if os.path.exists(lock_file):
                os.unlink(lock_file)
        except:
            pass

def _do_download(url, filepath):
    """Process-safe download implementation"""
    temp_dir = tempfile.mkdtemp()
    temp_file = os.path.join(temp_dir, 'temp_download')
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        response = requests.get(url, headers=headers, stream=True)
        
        if response.status_code == 200:
            # Download to temporary location first
            with open(temp_file, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                f.flush()
                os.fsync(f.fileno())  # Ensure all data is written
            
            # Atomic move to final location
            os.replace(temp_file, filepath)
            return True, None
            
        return False, f"HTTP {response.status_code}"
        
    finally:
        # Clean up temporary directory
        try:
            shutil.rmtree(temp_dir, ignore_errors=True)
        except:
            pass

class LawVNSession:
    BASE_URL = "https://luatvietnam.vn"
    LOGIN_URL = f"{BASE_URL}/user/dang-nhap.html"
    SESSION_DURATION = 3600 * 12  # 12 hours in seconds

    def __init__(self, cookies_file='lawvn_cookies.pkl', debug=False):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })
        self.cookies_file = cookies_file
        self.logged_in = False
        self.session_data = {
            'cookies': None,
            'timestamp': None
        }
        self.debug = debug
        self.debug_dir = "debug_output"
        if self.debug and not os.path.exists(self.debug_dir):
            os.makedirs(self.debug_dir)

    def _save_debug_info(self, driver, stage_name):
        """Save debug information at various stages"""
        if not self.debug:
            return

        timestamp = time.strftime("%H%M%S")  # Shorter timestamp
        prefix = f"{stage_name}_{timestamp}"
        
        # Only save important debug info
        if stage_name in ['login_error', 'google_login', 'verification_failed']:
            screenshot_path = os.path.join(self.debug_dir, f"{prefix}_screen.png")
            driver.save_screenshot(screenshot_path)
            logging.debug(f"Saved {stage_name} screenshot")
            
            source_path = os.path.join(self.debug_dir, f"{prefix}_source.html")
            with open(source_path, 'w', encoding='utf-8') as f:
                f.write(driver.page_source)

    def _interactive_debug(self, driver, message="Paused for debugging"):
        """Interactive debugging pause"""
        if not self.debug:
            return
            
        print("\n=== DEBUG PAUSE ===")
        print(message)
        print("Current URL:", driver.current_url)
        print("Available commands:")
        print("  source - Print page source")
        print("  screenshot - Save screenshot")
        print("  cookies - Print cookies")
        print("  continue - Continue execution")
        print("  quit - Exit script")
        
        while True:
            cmd = input("Debug command > ").strip().lower()
            if cmd == 'source':
                print(driver.page_source[:1000])
                print("...")
            elif cmd == 'screenshot':
                timestamp = time.strftime("%Y%m%d-%H%M%S")
                path = os.path.join(self.debug_dir, f"manual_debug_{timestamp}.png")
                driver.save_screenshot(path)
                print(f"Screenshot saved to {path}")
            elif cmd == 'cookies':
                print(driver.get_cookies())
            elif cmd == 'continue':
                break
            elif cmd == 'quit':
                driver.quit()
                sys.exit(0)

    def _is_session_valid(self):
        """Check if current session is still valid"""
        if not self.session_data['timestamp']:
            return False
            
        current_time = time.time()
        session_age = current_time - self.session_data['timestamp']
        return session_age < self.SESSION_DURATION

    def save_cookies(self, selenium_driver):
        """Save cookies with timestamp"""
        try:
            cookies = selenium_driver.get_cookies()
            self.session_data = {
                'cookies': cookies,
                'timestamp': time.time()
            }
            
            with open(self.cookies_file, 'wb') as f:
                pickle.dump(self.session_data, f)
            
            # Set cookies in requests session
            for cookie in cookies:
                self.session.cookies.set(cookie['name'], cookie['value'])
                
        except Exception as e:
            logging.error(f"Error saving cookies: {str(e)}")

    def load_cookies(self):
        """Load saved cookies if they exist and are valid"""
        try:
            if os.path.exists(self.cookies_file):
                with open(self.cookies_file, 'rb') as f:
                    self.session_data = pickle.load(f)
                
                # Set cookies in session without validation first
                for cookie in self.session_data['cookies']:
                    self.session.cookies.set(cookie['name'], cookie['value'])
                
                # Simple check if cookies work
                response = self.session.get(self.BASE_URL)
                if 'Đăng xuất' in response.text:
                    logging.info("Cookies loaded and verified")
                    return True
                    
                logging.info("Cookies exist but are invalid")
            return False
                    
        except Exception as e:
            logging.error(f"Error loading cookies: {str(e)}")
            return False

    def refresh_session(self):
        """Attempt to refresh session without full login"""
        try:
            response = self.session.get(self.BASE_URL)
            if 'Đăng xuất' in response.text:
                self.session_data['timestamp'] = time.time()
                with open(self.cookies_file, 'wb') as f:
                    pickle.dump(self.session_data, f)
                return True
        except Exception as e:
            logging.error(f"Error refreshing session: {str(e)}")
        return False

    def ensure_login(self):
        """Ensure valid session exists, create or use cookies"""
        if os.path.exists(self.cookies_file):
            logging.info("Found existing cookies, attempting to use them...")
            if self.load_cookies():
                logging.info("Successfully logged in using existing cookies")
                self.logged_in = True
                return True
            else:
                logging.warning("Existing cookies are invalid or expired")
        
        logging.info("No valid cookies found, starting new login...")
        if self.login():
            logging.info("Login successful, cookies saved for future use")
            self.logged_in = True
            return True
            
        return False

    def verify_login(self):
        """Verify if current session is logged in"""
        try:
            if not self._is_session_valid():
                return False

            # Warm up session before verification
            if not self._warm_up_session():
                return False
                
            response = self.session.get(
                f"{self.BASE_URL}/",
                headers={'Referer': self.BASE_URL},
                allow_redirects=True
            )
            
            if response.status_code != 200:
                logging.error(f"Got error {response.status_code}. URL: {response.url}")
                return False
                
            return 'Đăng xuất' in response.text
        except Exception as e:
            logging.error(f"Error verifying login: {str(e)}")
            return False

    def _warm_up_session(self):
        """Warm up the session by visiting the homepage first"""
        try:
            # First visit homepage without any cookies
            response = self.session.get(
                self.BASE_URL,
                headers={'Referer': 'https://www.google.com/'},
                allow_redirects=True
            )
            time.sleep(1)
            
            # Then visit homepage again with cookies
            response = self.session.get(
                self.BASE_URL,
                headers={'Referer': self.BASE_URL},
                allow_redirects=True
            )
            return response.status_code == 200
        except Exception as e:
            logging.error(f"Error warming up session: {str(e)}")
            return False

    def login(self):
        """Handle Google OAuth login with manual input and automatic detection"""
        if self.load_cookies():
            logging.info("Successfully logged in using saved cookies")
            self.logged_in = True
            return True

        driver = None
        try:
            options = webdriver.ChromeOptions()
            # Enhanced security and compatibility options
            options.add_argument('--start-maximized')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-web-security')
            options.add_argument('--allow-running-insecure-content')
            options.add_argument('--disable-blink-features=AutomationControlled')
            options.add_argument('--ignore-certificate-errors')
            options.add_argument('--ignore-ssl-errors')
            options.add_argument('--disable-features=IsolateOrigins,site-per-process')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-browser-side-navigation')
            options.add_argument('--disable-gpu')
            options.add_argument('--disable-infobars')
            options.add_argument('--disable-notifications')
            options.add_argument('--enable-automation')
            options.add_argument('--dns-prefetch-disable')
            
            # Add recommended user agent
            options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
            
            # Add experimental options
            options.add_experimental_option('excludeSwitches', ['enable-automation', 'enable-logging'])
            options.add_experimental_option('useAutomationExtension', False)
            options.add_experimental_option('prefs', {
                'credentials_enable_service': True,
                'profile.default_content_setting_values.notifications': 2,
                'profile.password_manager_enabled': True,
                'profile.managed_default_content_settings.images': 1,
                'profile.managed_default_content_settings.javascript': 1
            })
            
            driver = webdriver.Chrome(options=options)
            stealth(driver,
                languages=["en-US", "en"],
                vendor="Google Inc.",
                platform="Win32",
                webgl_vendor="Intel Inc.",
                renderer="Intel Iris OpenGL Engine",
                fix_hairline=True,
            )
            
            # Rest of the login code remains the same...
            # Initial navigation
            driver.get(self.BASE_URL)
            time.sleep(2)

            # Click login button
            login_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "(//span[contains(text(),'/ Đăng nhập')])[1]"))
            )
            driver.execute_script("arguments[0].click();", login_button)
            time.sleep(2)

            # Click Google login button
            google_btn = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, 'a.login-google.social-cr.google-login'))
            )
            driver.execute_script("arguments[0].click();", google_btn)
            
            print("\nGoogle login window opened.")
            print("Please complete the login process manually.")
            print("Script will try to auto-detect login success...")

            # Try multiple detection methods
            detection_timeout = 300  # 5 minutes
            start_time = time.time()
            success = False

            while time.time() - start_time < detection_timeout:
                try:
                    # Method 1: Check for profile link
                    try:
                        profile = driver.find_element(By.XPATH, "//div[@title='nxuanan024@gmail.com']//a[@title='Trang cá nhân']")
                        success = True
                        break
                    except:
                        pass

                    # Method 2: Check for logout button
                    try:
                        logout = driver.find_element(By.XPATH, "//a[contains(text(), 'Đăng xuất')]")
                        success = True
                        break
                    except:
                        pass

                    # Method 3: Check URL and content
                    if ('luatvietnam.vn' in driver.current_url and 
                        'dang-nhap' not in driver.current_url and
                        'Đăng xuất' in driver.page_source):
                        success = True
                        break

                    # Ask user if auto-detection is taking too long, only in debug mode
                    if time.time() - start_time > 30:  # After 30 seconds
                        if self.debug:
                            print("\nAuto-detection ongoing. Are you logged in? (y/n/wait)")
                            print("y = Yes, logged in successfully")
                            print("n = No, login failed")
                            print("wait = Keep trying auto-detection")
                            response = input().lower()
                            
                            if response == 'y':
                                success = True
                                break
                            elif response == 'n':
                                break
                        else:
                            time.sleep(2)
                            continue

                    time.sleep(2)

                except Exception as e:
                    logging.debug(f"Detection attempt error: {str(e)}")
                    time.sleep(2)

            if success:
                print("\nLogin successful!")
                # Save cookies
                cookies = driver.get_cookies()
                self.session_data = {
                    'cookies': cookies,
                    'timestamp': time.time()
                }
                
                with open(self.cookies_file, 'wb') as f:
                    pickle.dump(self.session_data, f)
                
                for cookie in cookies:
                    self.session.cookies.set(cookie['name'], cookie['value'])
                
                self.logged_in = True
                print("Cookies saved successfully.")
                print("\nPress Enter to close the browser...")
                input()
                driver.quit()
                return True
            else:
                print("\nLogin detection failed or timed out.")
                print("Press Enter to close browser and try again...")
                input()
                driver.quit()
                return False

        except Exception as e:
            logging.error(f"Login error: {str(e)}")
            if driver:
                print("\nAn error occurred.")
                print("Press Enter to close browser...")
                input()
                driver.quit()
            return False

    def check_login(self):
        """Check if we have valid cookies"""
        try:
            # Load cookies if not already loaded
            if not self.logged_in:
                return self.load_cookies()
                
            # Verify current session
            response = self.session.get(self.BASE_URL)
            return 'Đăng xuất' in response.text
                
        except Exception as e:
            logging.error(f"Error checking login: {str(e)}")
            return False

def find_document_links(url, debug=False, session=None):
    """Find document download links in a page"""
    logger = setup_logger(debug)
    # Append "#taive" if missing to move to the download tab
    if "#taive" not in url:
        url += "#taive"
        if debug:
            logger.debug("Appended #taive to url to move to the download tab")
    max_retries = 3
    retry_delay = 2
    
    def debug_log(msg):
        """Only log if debug is enabled"""
        if debug:
            logger.debug(msg)
    
    # Ensure logged in before attempting to fetch documents
    if session and not session.check_login():
        logger.error("Not logged in. Please login first")
        return []
    
    for attempt in range(max_retries):
        try:
            log_url = url.split('#')[0]
            debug_log(f"Processing URL: {log_url}")
            
            requests_session = session.session if session else requests.Session()
            response = requests_session.get(url, allow_redirects=True)
            
            if response.status_code == 404:
                logger.error(f"Got 404 error on attempt {attempt + 1}. URL: {url}")
                if session and attempt < max_retries - 1:
                    debug_log("Attempting to refresh session...")
                    session.login()
                    time.sleep(retry_delay)
                    continue
                    
            if debug:
                save_debug_html(log_url, response.text)
            
            if 'dang-nhap' in response.url:
                logger.error("Redirected to login page - session may have expired")
                if session:
                    session.login()
                    continue
                return []
            
            soup = BeautifulSoup(response.text, 'lxml')
            links = []
            
            # First try finding links in the document entry section
            download_section = soup.find('div', class_='the-document-entry')
            if download_section:
                debug_log("Found document entry container")
                
                vn_doc = download_section.find('div', class_='vn-doc')
                if vn_doc:
                    debug_log("Found Vietnamese document section")
                    for a in vn_doc.find_all('a', href=True):
                        href = a.get('href')
                        if href and ('.doc' in href.lower() or '.pdf' in href.lower()):
                            links.append(urljoin(url, href))
                            debug_log(f"Added document link: {href}")
            
            # Check list-download divs if no links found yet
            if not links:
                debug_log("Checking list-download divs")
                list_downloads = soup.find_all('div', class_='list-download')
                for list_download in list_downloads:
                    for a in list_download.find_all('a', href=True):
                        href = a.get('href')
                        if href and ('.doc' in href.lower() or '.pdf' in href.lower()):
                            links.append(urljoin(url, href))
                            debug_log(f"Added document link: {href}")
            
            # If still no links, try finding any download links in the page
            if not links:
                debug_log("Searching for any download links")
                for a in soup.find_all('a', href=True):
                    href = a.get('href')
                    if href and ('.doc' in href.lower() or '.pdf' in href.lower()):
                        links.append(urljoin(url, href))
                        debug_log(f"Added document link: {href}")
            
            if links:
                debug_log(f"Found {len(links)} document links")
            else:
                debug_log("No document links found")
                
            return links
            
        except Exception as e:
            logger.exception(f"Error processing {url}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                continue
            return []

# ...existing code...
