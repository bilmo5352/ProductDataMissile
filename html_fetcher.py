"""
HTML Fetcher - Product Extraction System
==========================================
Fetches rendered HTML from Chrome Worker API in batches and triggers parsing.

Features:
- Batch processing (10 URLs per request)
- HTML caching for debugging
- Retry logic with exponential backoff
- Progress tracking and logging
- Automatic parser invocation
"""

import requests
import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any
import logging
from urllib.parse import urlparse
import hashlib

# Import parser
from html_parser import HTMLProductParser

# Import config
from config import (
    CHROME_WORKER_API_URL,
    BATCH_SIZE,
    MAX_RETRIES,
    RETRY_DELAY,
    HTML_CACHE_DIR,
    RESULTS_DIR,
    LOGS_DIR,
    SAVE_HTML_CACHE,
    SAVE_RESULTS,
    LOG_TO_FILE,
    SAVE_LOGS
)


class HTMLFetcher:
    """Fetches HTML content from Chrome Worker API and manages batch processing."""
    
    def __init__(self):
        """Initialize fetcher with logging and directory setup."""
        self._setup_directories()
        self._setup_logging()
        self.parser = HTMLProductParser()
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'User-Agent': 'ProductExtractor/1.0'
        })
        
    def _setup_directories(self):
        """Create necessary directories if they don't exist."""
        directories = []
        if HTML_CACHE_DIR:
            directories.append(HTML_CACHE_DIR)
        if RESULTS_DIR:
            directories.append(RESULTS_DIR)
        if LOGS_DIR:
            directories.append(LOGS_DIR)
        
        for directory in directories:
            Path(directory).mkdir(parents=True, exist_ok=True)
            
    def _setup_logging(self):
        """Configure logging with file and console output."""
        handlers = []
        
        # Add console handler
        console_handler = logging.StreamHandler()
        # Set UTF-8 encoding for console handler (Windows compatibility)
        if hasattr(console_handler.stream, 'reconfigure'):
            try:
                console_handler.stream.reconfigure(encoding='utf-8', errors='replace')
            except (AttributeError, ValueError):
                pass  # Fallback to default if reconfigure not available
        handlers.append(console_handler)
        
        # Add file handler only if LOG_TO_FILE is True and LOGS_DIR exists
        if LOG_TO_FILE and LOGS_DIR:
            log_file = LOGS_DIR / f"extraction_{datetime.now().strftime('%Y%m%d')}.log"
            file_handler = logging.FileHandler(log_file, encoding='utf-8')
            handlers.append(file_handler)
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=handlers
        )
        self.logger = logging.getLogger(__name__)
        
    def _generate_filename(self, url: str, extension: str = 'html') -> str:
        """
        Generate unique filename from URL.
        
        Args:
            url: Source URL
            extension: File extension (html or json)
            
        Returns:
            Filename string with timestamp
        """
        parsed = urlparse(url)
        domain = parsed.netloc.replace('www.', '').split('.')[0]
        
        # Create short hash of full URL for uniqueness
        url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        return f"{domain}_{url_hash}_{timestamp}.{extension}"
    
    def _save_html(self, url: str, html_content: str) -> str:
        """
        Save HTML content to cache directory (only if SAVE_HTML_CACHE is True).
        
        Args:
            url: Source URL
            html_content: HTML string to save
            
        Returns:
            Path to saved file or empty string if not saved
        """
        if not SAVE_HTML_CACHE or not HTML_CACHE_DIR:
            return ""
        
        filename = self._generate_filename(url, 'html')
        filepath = HTML_CACHE_DIR / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(html_content)
            
        self.logger.info(f"Saved HTML: {filename} ({len(html_content)} bytes)")
        return str(filepath)
    
    def fetch_batch(self, urls: List[str]) -> List[Dict[str, Any]]:
        """
        Fetch HTML for a batch of URLs from Chrome Worker API.
        
        Args:
            urls: List of URLs to fetch (max BATCH_SIZE)
            
        Returns:
            List of dicts with url, html, success status
        """
        if len(urls) > BATCH_SIZE:
            self.logger.warning(f"Batch size {len(urls)} exceeds limit {BATCH_SIZE}. Truncating.")
            urls = urls[:BATCH_SIZE]
        
        self.logger.info(f"Fetching batch of {len(urls)} URLs...")
        
        payload = {
            "urls": urls,
        }
        
        for attempt in range(MAX_RETRIES):
            try:
                response = self.session.post(
                    CHROME_WORKER_API_URL,
                    json=payload,
                    timeout=120  # 2 minute timeout for batch
                )
                response.raise_for_status()
                
                data = response.json()
                results = []
                
                # Parse API response
                if isinstance(data, dict) and 'results' in data:
                    # Format: {"results": [{"url": "...", "html": "..."}, ...]}
                    results = data['results']
                elif isinstance(data, list):
                    # Format: [{"url": "...", "html": "..."}, ...]
                    results = data
                else:
                    self.logger.error(f"Unexpected API response format: {type(data)}")
                    return []
                
                # Debug: Log first response structure to understand format
                if results:
                    first_result = results[0]
                    self.logger.info(f"Sample API response keys: {list(first_result.keys())}")
                    self.logger.info(f"Sample response status: {first_result.get('status', 'N/A')}")
                    self.logger.info(f"Sample response has 'html': {'html' in first_result}")
                    if 'html' in first_result:
                        html_len = len(str(first_result.get('html', '')))
                        self.logger.info(f"Sample HTML length: {html_len} characters")
                
                self.logger.info(f"Successfully fetched {len(results)} HTML responses")
                return results
                
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Attempt {attempt + 1}/{MAX_RETRIES} failed: {e}")
                
                if attempt < MAX_RETRIES - 1:
                    delay = RETRY_DELAY * (2 ** attempt)  # Exponential backoff
                    self.logger.info(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
                else:
                    self.logger.error("All retry attempts exhausted")
                    return []
        
        return []
    
    def process_urls(self, urls: List[str]) -> Dict[str, Any]:
        """
        Process all URLs in batches.
        
        Args:
            urls: List of all URLs to process
            
        Returns:
            Summary dict with statistics
        """
        self.logger.info(f"Starting processing of {len(urls)} URLs")
        start_time = time.time()
        
        all_results = []
        successful = 0
        failed = 0
        total_products = 0
        
        # Process in batches
        for i in range(0, len(urls), BATCH_SIZE):
            batch_urls = urls[i:i + BATCH_SIZE]
            batch_num = (i // BATCH_SIZE) + 1
            total_batches = (len(urls) + BATCH_SIZE - 1) // BATCH_SIZE
            
            self.logger.info(f"\n{'='*60}")
            self.logger.info(f"Processing Batch {batch_num}/{total_batches}")
            self.logger.info(f"{'='*60}")
            
            # Fetch HTML
            html_responses = self.fetch_batch(batch_urls)
            
            # Process each response
            for response in html_responses:
                url = response.get('url', '')
                # API returns 'status' field (string: 'success' or error status)
                status = response.get('status', '')
                html = response.get('html', '')
                
                # Check if status is 'success' and HTML is present
                if status != 'success' or not html or (isinstance(html, str) and len(html.strip()) == 0):
                    error_msg = response.get('error', 'No HTML content')
                    error_type = response.get('errorType', '')
                    if error_type:
                        error_msg = f"{error_msg} (Type: {error_type})"
                    
                    self.logger.warning(f"Failed to fetch: {url} - Status: {status}, Error: {error_msg}")
                    failed += 1
                    all_results.append({
                        'url': url,
                        'success': False,
                        'error': error_msg,
                        'error_type': error_type,
                        'num_products': 0
                    })
                    continue
                
                try:
                    # Save HTML to cache for debugging/reference (only if enabled)
                    html_path = self._save_html(url, html)
                    
                    # Parse HTML directly from API response (not reading from file)
                    self.logger.info(f"Parsing: {url}")
                    result = self.parser.parse_html(html, url)
                    
                    # Save result to JSON (only if enabled)
                    if SAVE_RESULTS and RESULTS_DIR:
                        result_filename = self._generate_filename(url, 'json')
                        result_path = RESULTS_DIR / result_filename
                        
                        with open(result_path, 'w', encoding='utf-8') as f:
                            json.dump(result, f, indent=2, ensure_ascii=False)
                        
                        self.logger.info(f"  Saved to: {result_filename}")
                    
                    successful += 1
                    num_products = result.get('num_products', 0)
                    total_products += num_products
                    
                    self.logger.info(f"[OK] Success: {num_products} products found")
                    
                    all_results.append(result)
                    
                except Exception as e:
                    self.logger.error(f"Error processing {url}: {e}", exc_info=True)
                    failed += 1
                    all_results.append({
                        'url': url,
                        'success': False,
                        'error': str(e),
                        'num_products': 0
                    })
            
            # Small delay between batches
            if i + BATCH_SIZE < len(urls):
                time.sleep(2)
        
        # Calculate summary
        duration = time.time() - start_time
        summary = {
            'batch_id': f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            'total_urls': len(urls),
            'successful': successful,
            'failed': failed,
            'success_rate': f"{(successful/len(urls)*100):.1f}%" if urls else "0%",
            'total_products': total_products,
            'avg_products_per_url': round(total_products / successful, 2) if successful > 0 else 0,
            'total_duration_seconds': round(duration, 2),
            'timestamp': datetime.now().isoformat(),
            'results': all_results
        }
        
        # Save batch summary (only if enabled)
        if SAVE_RESULTS and RESULTS_DIR:
            summary_path = RESULTS_DIR / f"batch_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(summary_path, 'w', encoding='utf-8') as f:
                json.dump(summary, f, indent=2, ensure_ascii=False)
            self.logger.info(f"Summary saved to: {summary_path.name}")
        
        self.logger.info(f"\n{'='*60}")
        self.logger.info("BATCH SUMMARY")
        self.logger.info(f"{'='*60}")
        self.logger.info(f"Total URLs: {summary['total_urls']}")
        self.logger.info(f"Successful: {summary['successful']}")
        self.logger.info(f"Failed: {summary['failed']}")
        self.logger.info(f"Success Rate: {summary['success_rate']}")
        self.logger.info(f"Total Products: {summary['total_products']}")
        self.logger.info(f"Avg Products/URL: {summary['avg_products_per_url']}")
        self.logger.info(f"Duration: {summary['total_duration_seconds']}s")
        
        return summary


def load_test_urls(filepath: str = 'test_urls.json') -> List[str]:
    """
    Load URLs from test file.
    
    Args:
        filepath: Path to JSON file with URLs
        
    Returns:
        List of URLs
    """
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
            return data.get('urls', [])
    except FileNotFoundError:
        print(f"Error: {filepath} not found")
        return []
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON in {filepath}")
        return []


def main():
    """Main execution function."""
    print("\n" + "="*60)
    print("Product Extraction System - HTML Fetcher")
    print("="*60 + "\n")
    
    # Load test URLs
    urls = load_test_urls()
    
    if not urls:
        print("No URLs to process. Please add URLs to test_urls.json")
        print("\nExample format:")
        print("""{
  "urls": [
    "https://www.amazon.in/s?k=laptop",
    "https://www.flipkart.com/search?q=laptop"
  ]
}""")
        return
    
    print(f"Loaded {len(urls)} URLs from test_urls.json\n")
    
    # Initialize fetcher and process
    fetcher = HTMLFetcher()
    summary = fetcher.process_urls(urls)
    
    print("\n" + "="*60)
    print("[OK] Processing Complete!")
    print("="*60)
    if SAVE_RESULTS and RESULTS_DIR:
        print(f"\nResults saved to: {RESULTS_DIR}/")
    if SAVE_HTML_CACHE and HTML_CACHE_DIR:
        print(f"HTML cache saved to: {HTML_CACHE_DIR}/")
    if SAVE_LOGS and LOGS_DIR:
        print(f"Logs saved to: {LOGS_DIR}/")


if __name__ == "__main__":
    main()