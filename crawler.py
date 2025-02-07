import os
import requests
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
import logging
from pathlib import Path
import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox, filedialog
from threading import Thread, Event
import time
import random
import hashlib
from typing import Optional, Set, Dict, List
from dataclasses import dataclass
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from playwright.sync_api import sync_playwright
import redis  # For distributed scraping
from twocaptcha import TwoCaptcha  # For CAPTCHA solving
from prometheus_client import start_http_server, Counter, Gauge  # For monitoring
import sentry_sdk  # For error tracking
from scrapinghub import ScrapinghubClient  # For cloud-based scraping
from datetime import datetime  # For date-based subdirectories
from queue import Queue  # For local queue

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Redis connection for distributed scraping
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
redis_client = None  # Initialize as None, will be set based on checkbox

# Initialize Prometheus metrics
PAGES_CRAWLED = Counter('pages_crawled', 'Total pages crawled')
FILES_DOWNLOADED = Counter('files_downloaded', 'Total files downloaded')
ERRORS_ENCOUNTERED = Counter('errors_encountered', 'Total errors encountered')
BYTES_DOWNLOADED = Gauge('bytes_downloaded', 'Total bytes downloaded')

# Initialize Sentry for error tracking
SENTRY_DSN = os.getenv('SENTRY_DSN')
if SENTRY_DSN:
    sentry_sdk.init(SENTRY_DSN)


@dataclass
class CrawlerStats:
    """Class to track crawler statistics"""
    pages_crawled: int = 0
    files_downloaded: int = 0
    errors_encountered: int = 0
    bytes_downloaded: int = 0


class WebsiteCrawler:
    def __init__(self, base_url, output_dir, max_depth=3, max_file_size=10, timeout=10, rate_limit=1, blacklist=None, proxies=None, captcha_api_key=None, scrapy_cloud_api_key=None, use_redis=True):
        self.base_url = base_url
        self.output_dir = Path(output_dir)
        self.max_depth = max_depth
        self.max_file_size = max_file_size * 1024 * 1024  # Convert MB to bytes
        self.timeout = timeout
        self.rate_limit = rate_limit
        self.blacklist = blacklist or []
        self.proxies = proxies or []
        self.captcha_api_key = captcha_api_key
        self.scrapy_cloud_api_key = scrapy_cloud_api_key
        self.visited_urls = set()
        self.session = self._setup_session()
        self.playwright = None  # Headless browser instance
        self.pause_event = Event()
        self.pause_event.set()
        self.crawling_active = True
        self.stats = CrawlerStats()
        self.use_redis = use_redis  # Whether to use Redis or local queue
        self.local_queue = Queue()  # Local queue for non-Redis mode

        # Initialize CAPTCHA solver
        self.captcha_solver = TwoCaptcha(captcha_api_key) if captcha_api_key else None

        # Initialize Scrapy Cloud client
        self.scrapy_client = ScrapinghubClient(scrapy_cloud_api_key) if scrapy_cloud_api_key else None

        # User-Agent rotation
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
            'Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1',
            'Mozilla/5.0 (Linux; Android 10; SM-G975F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.120 Mobile Safari/537.36'
        ]

        # File type mappings
        self.file_types = {
            'images': ('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp'),
            'documents': ('.pdf', '.doc', '.docx', '.xls', '.xlsx', '.txt'),
            'media': ('.mp3', '.mp4', '.wav', '.avi', '.mov'),
            'archives': ('.zip', '.rar', '.7z', '.tar', '.gz')
        }

        # Create base directory structure
        self.dirs = {
            'html': self.output_dir / 'html',
            'images': self.output_dir / 'images',
            'documents': self.output_dir / 'documents',
            'other': self.output_dir / 'other'
        }

        for dir_path in self.dirs.values():
            dir_path.mkdir(parents=True, exist_ok=True)

        # Start Prometheus server
        start_http_server(8000)

    def _setup_session(self):
        """Configure requests session with retries and timeouts"""
        session = requests.Session()
        retries = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retries)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        return session

    def _get_proxy(self):
        """Rotate proxies to avoid IP blocking"""
        if self.proxies:
            return random.choice(self.proxies)
        return None

    def _fetch_with_playwright(self, url):
        """Fetch dynamic content using Playwright"""
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(url)
            content = page.content()
            browser.close()
            return content

    def solve_captcha(self, sitekey, url):
        """Solve CAPTCHA using 2Captcha"""
        if not self.captcha_solver:
            logger.warning("CAPTCHA solver not configured. Skipping CAPTCHA.")
            return None

        try:
            result = self.captcha_solver.recaptcha(sitekey=sitekey, url=url)
            return result['code']
        except Exception as e:
            logger.error(f"Failed to solve CAPTCHA: {str(e)}")
            return None

    def is_valid_url(self, url):
        """Check if URL belongs to the target domain and is valid"""
        parsed = urlparse(url)
        return parsed.scheme in ('http', 'https') and parsed.netloc == urlparse(self.base_url).netloc

    def get_absolute_url(self, link):
        """Convert relative URL to absolute"""
        return urljoin(self.base_url, link)

    def get_file_hash(self, content):
        """Generate SHA-256 hash of file content to prevent duplicates"""
        return hashlib.sha256(content).hexdigest()

    def save_content(self, url, content, content_type, date=None):
        """Save content to appropriate directory and push to Scrapy Cloud"""
        try:
            content_hash = self.get_file_hash(content)
            parsed_url = urlparse(url)
            original_filename = os.path.basename(parsed_url.path) or f"page_{content_hash[:8]}.html"

            # Determine file category
            category = 'other'
            for cat, exts in self.file_types.items():
                if any(original_filename.lower().endswith(ext) for ext in exts):
                    category = cat
                    break

            if 'text/html' in content_type:
                category = 'html'

            # Create date-based subdirectory
            date_str = date.strftime('%Y-%m-%d') if date else 'no_date'
            category_dir = self.dirs[category] / date_str
            category_dir.mkdir(parents=True, exist_ok=True)

            # Save file
            file_path = category_dir / f"{content_hash[:8]}_{original_filename}"
            if not file_path.exists():
                with open(file_path, 'wb') as f:
                    f.write(content)
                self.stats.files_downloaded += 1
                self.stats.bytes_downloaded += len(content)
                logger.info(f"Saved: {file_path}")

                # Push to Scrapy Cloud
                if self.scrapy_client:
                    project = self.scrapy_client.get_project('YOUR_PROJECT_ID')
                    project.jobs.run(spider='website_crawler', url=url, content=content)

            else:
                logger.info(f"Skipped duplicate file: {file_path}")

            return True

        except Exception as e:
            logger.error(f"Error saving {url}: {str(e)}")
            self.stats.errors_encountered += 1
            return False

    def process_page(self, url, depth=0):
        """Process a single page"""
        if not self.crawling_active or url in self.visited_urls or not self.is_valid_url(url) or depth > self.max_depth:
            return

        self.visited_urls.add(url)
        logger.info(f"Processing: {url} (Depth: {depth})")

        try:
            # Wait if crawling is paused
            self.pause_event.wait()

            # Respect rate limit
            time.sleep(self.rate_limit)

            # Rotate User-Agent and proxy
            self.session.headers.update({'User-Agent': random.choice(self.user_agents)})
            proxy = self._get_proxy()
            if proxy:
                self.session.proxies.update({'http': proxy, 'https': proxy})

            # Fetch content (use Playwright for dynamic content)
            if 'javascript' in self.session.headers.get('Accept', ''):
                content = self._fetch_with_playwright(url)
                content_type = 'text/html'
            else:
                response = self.session.get(url, timeout=self.timeout)
                if response.status_code == 429:  # CAPTCHA detected
                    logger.info("CAPTCHA detected. Attempting to solve...")
                    sitekey = self.extract_captcha_sitekey(response.text)
                    if sitekey:
                        captcha_response = self.solve_captcha(sitekey, url)
                        if captcha_response:
                            response = self.session.post(url, data={'g-recaptcha-response': captcha_response})
                response.raise_for_status()
                content = response.content
                content_type = response.headers.get('Content-Type', '').split(';')[0]

            self.stats.pages_crawled += 1
            PAGES_CRAWLED.inc()  # Update Prometheus metric

            # Save content
            if len(content) <= self.max_file_size:
                if self.save_content(url, content, content_type):
                    FILES_DOWNLOADED.inc()  # Update Prometheus metric
                    BYTES_DOWNLOADED.set(self.stats.bytes_downloaded)  # Update Prometheus metric
            else:
                logger.warning(f"Skipping large file: {url} ({len(content)} bytes)")

            # Process HTML content
            if 'text/html' in content_type:
                soup = BeautifulSoup(content, 'html.parser')
                for link in soup.find_all(['a', 'img', 'script', 'link']):
                    href = link.get('href') or link.get('src')
                    if href:
                        absolute_url = self.get_absolute_url(href)
                        if self.is_valid_url(absolute_url):
                            if self.use_redis:
                                redis_client.lpush('crawler_queue', absolute_url)  # Add to Redis queue
                            else:
                                self.local_queue.put(absolute_url)  # Add to local queue

        except requests.exceptions.RequestException as e:
            logger.error(f"Network error processing {url}: {str(e)}")
            self.stats.errors_encountered += 1
            ERRORS_ENCOUNTERED.inc()  # Update Prometheus metric
        except Exception as e:
            logger.error(f"Error processing {url}: {str(e)}")
            self.stats.errors_encountered += 1
            ERRORS_ENCOUNTERED.inc()  # Update Prometheus metric
            sentry_sdk.capture_exception(e)  # Send error to Sentry

    def extract_captcha_sitekey(self, html):
        """Extract CAPTCHA sitekey from HTML"""
        soup = BeautifulSoup(html, 'html.parser')
        captcha_div = soup.find('div', class_='g-recaptcha')
        if captcha_div and captcha_div.get('data-sitekey'):
            return captcha_div['data-sitekey']
        return None

    def crawl(self):
        """Start crawling process"""
        start_time = time.time()
        if self.use_redis:
            redis_client.lpush('crawler_queue', self.base_url)  # Add base URL to Redis queue
        else:
            self.local_queue.put(self.base_url)  # Add base URL to local queue

        while (self.use_redis and redis_client.llen('crawler_queue') > 0) or (not self.use_redis and not self.local_queue.empty()):
            if not self.crawling_active:
                break

            if self.use_redis:
                url = redis_client.rpop('crawler_queue').decode('utf-8')
            else:
                url = self.local_queue.get()

            self.process_page(url)

        duration = time.time() - start_time
        self._log_crawl_summary(duration)

    def _log_crawl_summary(self, duration):
        """Log crawling statistics summary"""
        logger.info("\n=== Crawling Summary ===")
        logger.info(f"Total pages crawled: {self.stats.pages_crawled}")
        logger.info(f"Total files downloaded: {self.stats.files_downloaded}")
        logger.info(f"Total data downloaded: {self.stats.bytes_downloaded / 1024 / 1024:.2f} MB")
        logger.info(f"Errors encountered: {self.stats.errors_encountered}")
        logger.info(f"Total duration: {duration:.2f} seconds")
        logger.info("=====================")


class CrawlerApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Advanced Website Crawler")
        self.root.geometry("900x700")
        self.root.configure(bg='#2E3440')

        # Configuration Settings Frame
        self.config_frame = ttk.LabelFrame(root, text="Configuration Settings")
        self.config_frame.pack(fill=tk.X, padx=10, pady=10)

        # Redis Host
        ttk.Label(self.config_frame, text="Redis Host:").grid(row=0, column=0, padx=5, pady=5)
        self.redis_host = tk.StringVar(value=REDIS_HOST)
        ttk.Entry(self.config_frame, textvariable=self.redis_host, width=20).grid(row=0, column=1, padx=5, pady=5)

        # Redis Port
        ttk.Label(self.config_frame, text="Redis Port:").grid(row=0, column=2, padx=5, pady=5)
        self.redis_port = tk.StringVar(value=str(REDIS_PORT))
        ttk.Entry(self.config_frame, textvariable=self.redis_port, width=10).grid(row=0, column=3, padx=5, pady=5)

        # Disable Redis Checkbox
        self.use_redis = tk.BooleanVar(value=True)
        ttk.Checkbutton(self.config_frame, text="Use Redis", variable=self.use_redis).grid(row=0, column=4, padx=5, pady=5)

        # Save Configuration Button
        ttk.Button(self.config_frame, text="Save Configuration", command=self.save_config).grid(row=0, column=5, padx=5, pady=5)

        # Dark Mode Toggle
        self.dark_mode = tk.BooleanVar(value=True)
        ttk.Checkbutton(root, text="Dark Mode", variable=self.dark_mode, command=self.toggle_dark_mode).pack(pady=5)

        # URL Input
        ttk.Label(root, text="Enter Website URL:", background='#2E3440', foreground='white').pack(pady=5)
        self.url_entry = ttk.Entry(root, width=80)
        self.url_entry.pack(pady=5)

        # Output Directory Selection
        ttk.Label(root, text="Select Output Directory:", background='#2E3440', foreground='white').pack(pady=5)
        self.output_dir = tk.StringVar()
        ttk.Entry(root, textvariable=self.output_dir, width=80, state='readonly').pack(pady=5)
        ttk.Button(root, text="Browse", command=self.choose_output_dir).pack(pady=5)

        # Settings Frame
        self.settings_frame = ttk.LabelFrame(root, text="Settings")
        self.settings_frame.pack(fill=tk.X, padx=10, pady=10)

        # Max Depth
        ttk.Label(self.settings_frame, text="Max Depth:").grid(row=0, column=0, padx=5, pady=5)
        self.max_depth = tk.IntVar(value=3)
        ttk.Entry(self.settings_frame, textvariable=self.max_depth, width=10).grid(row=0, column=1, padx=5, pady=5)

        # Max File Size (MB)
        ttk.Label(self.settings_frame, text="Max File Size (MB):").grid(row=0, column=2, padx=5, pady=5)
        self.max_file_size = tk.IntVar(value=10)
        ttk.Entry(self.settings_frame, textvariable=self.max_file_size, width=10).grid(row=0, column=3, padx=5, pady=5)

        # Timeout (seconds)
        ttk.Label(self.settings_frame, text="Timeout (seconds):").grid(row=0, column=4, padx=5, pady=5)
        self.timeout = tk.IntVar(value=10)
        ttk.Entry(self.settings_frame, textvariable=self.timeout, width=10).grid(row=0, column=5, padx=5, pady=5)

        # Rate Limit (seconds)
        ttk.Label(self.settings_frame, text="Rate Limit (seconds):").grid(row=1, column=0, padx=5, pady=5)
        self.rate_limit = tk.IntVar(value=1)
        ttk.Entry(self.settings_frame, textvariable=self.rate_limit, width=10).grid(row=1, column=1, padx=5, pady=5)

        # Blacklist URLs
        ttk.Label(self.settings_frame, text="Blacklist URLs (comma-separated):").grid(row=1, column=2, padx=5, pady=5)
        self.blacklist = tk.StringVar()
        ttk.Entry(self.settings_frame, textvariable=self.blacklist, width=30).grid(row=1, column=3, columnspan=3, padx=5, pady=5)

        # Statistics Frame
        self.stats_frame = ttk.LabelFrame(root, text="Statistics")
        self.stats_frame.pack(fill=tk.X, padx=10, pady=5)
        
        self.stats_labels = {
            'pages': tk.StringVar(value="Pages: 0"),
            'files': tk.StringVar(value="Files: 0"),
            'errors': tk.StringVar(value="Errors: 0"),
            'data': tk.StringVar(value="Data: 0 MB")
        }
        
        for i, (key, var) in enumerate(self.stats_labels.items()):
            ttk.Label(self.stats_frame, textvariable=var).grid(row=0, column=i, padx=10)

        # Control Buttons
        self.control_frame = ttk.Frame(root)
        self.control_frame.pack(pady=10)

        self.start_button = ttk.Button(self.control_frame, text="Start", command=self.start_crawler)
        self.start_button.grid(row=0, column=0, padx=5)

        self.pause_button = ttk.Button(self.control_frame, text="Pause", command=self.pause_crawler)
        self.pause_button.grid(row=0, column=1, padx=5)

        self.stop_button = ttk.Button(self.control_frame, text="Stop", command=self.stop_crawler)
        self.stop_button.grid(row=0, column=2, padx=5)

        # Log Console
        self.log_console = scrolledtext.ScrolledText(root, width=110, height=20, state='disabled')
        self.log_console.pack(pady=10)

        # Initialize crawler
        self.crawler = None
        self.crawler_thread = None

    def toggle_dark_mode(self):
        """Toggle between dark and light mode"""
        if self.dark_mode.get():
            self.root.configure(bg='#2E3440')
            self.log_console.configure(bg='#3B4252', fg='#ECEFF4')
        else:
            self.root.configure(bg='#ECEFF4')
            self.log_console.configure(bg='#FFFFFF', fg='#000000')

    def choose_output_dir(self):
        """Choose output directory using file dialog"""
        directory = filedialog.askdirectory()
        if directory:
            self.output_dir.set(directory)

    def save_config(self):
        """Save configuration settings from the GUI"""
        try:
            # Update Redis connection
            global REDIS_HOST, REDIS_PORT, redis_client
            REDIS_HOST = self.redis_host.get()
            REDIS_PORT = int(self.redis_port.get())
            redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)

            # Show success message
            messagebox.showinfo("Success", "Configuration saved successfully!")
        except Exception as e:
            logger.error(f"Error saving configuration: {str(e)}")
            messagebox.showerror("Error", f"Failed to save configuration: {str(e)}")

    def start_crawler(self):
        """Start the crawler"""
        if not self.url_entry.get() or not self.output_dir.get():
            messagebox.showerror("Error", "Please enter a URL and select an output directory.")
            return

        # Initialize crawler
        self.crawler = WebsiteCrawler(
            base_url=self.url_entry.get(),
            output_dir=self.output_dir.get(),
            max_depth=self.max_depth.get(),
            max_file_size=self.max_file_size.get(),
            timeout=self.timeout.get(),
            rate_limit=self.rate_limit.get(),
            blacklist=self.blacklist.get().split(',') if self.blacklist.get() else None,
            use_redis=self.use_redis.get()  # Pass the Redis checkbox value
        )

        # Start crawler in a separate thread
        self.crawler_thread = Thread(target=self.crawler.crawl)
        self.crawler_thread.start()

        # Update UI
        self.start_button.config(state='disabled')
        self.pause_button.config(state='normal')
        self.stop_button.config(state='normal')

    def pause_crawler(self):
        """Pause or resume the crawler"""
        if self.crawler:
            if self.crawler.pause_event.is_set():
                self.crawler.pause_event.clear()
                self.pause_button.config(text="Resume")
            else:
                self.crawler.pause_event.set()
                self.pause_button.config(text="Pause")

    def stop_crawler(self):
        """Stop the crawler"""
        if self.crawler:
            self.crawler.crawling_active = False
            self.crawler.pause_event.set()  # Ensure crawler is not paused
            if self.crawler_thread:
                self.crawler_thread.join(timeout=1)  # Wait for the thread to finish with a timeout

            # Update UI
            self.start_button.config(state='normal')
            self.pause_button.config(state='disabled')
            self.stop_button.config(state='disabled')
            self.pause_button.config(text="Pause")

            # Show summary
            messagebox.showinfo("Crawling Complete", "Crawling has been stopped.")

    def update_stats(self):
        """Update statistics in the GUI"""
        if self.crawler:
            self.stats_labels['pages'].set(f"Pages: {self.crawler.stats.pages_crawled}")
            self.stats_labels['files'].set(f"Files: {self.crawler.stats.files_downloaded}")
            self.stats_labels['errors'].set(f"Errors: {self.crawler.stats.errors_encountered}")
            self.stats_labels['data'].set(f"Data: {self.crawler.stats.bytes_downloaded / 1024 / 1024:.2f} MB")
        self.root.after(1000, self.update_stats)  # Schedule next update

    def log_message(self, message):
        """Log messages to the console"""
        self.log_console.config(state='normal')
        self.log_console.insert(tk.END, message + "\n")
        self.log_console.config(state='disabled')
        self.log_console.yview(tk.END)


# Redirect logging to the GUI console
class GUILogHandler(logging.Handler):
    def __init__(self, app):
        super().__init__()
        self.app = app

    def emit(self, record):
        log_entry = self.format(record)
        self.app.log_message(log_entry)


# Main execution
if __name__ == "__main__":
    root = tk.Tk()
    app = CrawlerApp(root)

    # Redirect logging to GUI
    gui_handler = GUILogHandler(app)
    gui_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(gui_handler)

    # Start stats update loop
    app.update_stats()

    # Run the application
    root.mainloop()
