"""
Product Extraction Worker - Continuous Processing System
=========================================================
Continuously fetches URLs from Supabase, processes them, and saves results.

Features:
- Fetches batches of 100 URLs from product_page_urls table
- Claims URLs to prevent duplicate processing
- Fetches HTML via Railway private networking
- Extracts products and saves to r_product_data table
- Updates processing status in product_page_urls table
- Runs continuously in infinite loop
"""

import os
import time
import requests
import logging
import socket
import uuid
from datetime import datetime
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Semaphore, Lock
from dotenv import load_dotenv
from supabase import create_client, Client

# Load environment variables first
load_dotenv()

# Import parser after env is loaded
try:
    from html_parser import HTMLProductParser
except ImportError as e:
    print(f"CRITICAL: Failed to import html_parser: {e}")
    print("Make sure html_parser.py exists in the same directory")
    raise

# Configure logging - ensure it outputs to stdout/stderr for Railway
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()  # Explicitly add StreamHandler for Railway logs
    ],
    force=True  # Force reconfiguration
)
logger = logging.getLogger(__name__)

# Log startup
logger.info("=" * 60)
logger.info("Product Extraction Worker Starting...")
logger.info("=" * 60)

# Initialize parser
try:
    parser = HTMLProductParser()
    logger.info("HTML parser initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize HTML parser: {e}", exc_info=True)
    raise

# Initialize Supabase client
SUPABASE_URL = os.getenv('SUPABASE_URL', '')
SUPABASE_KEY = os.getenv('SUPABASE_KEY', '')
supabase: Optional[Client] = None

if not SUPABASE_URL or not SUPABASE_KEY:
    error_msg = "Supabase credentials not provided. Set SUPABASE_URL and SUPABASE_KEY environment variables."
    logger.error(error_msg)
    logger.error(f"SUPABASE_URL present: {bool(SUPABASE_URL)}")
    logger.error(f"SUPABASE_KEY present: {bool(SUPABASE_KEY)}")
    raise ValueError(error_msg)

try:
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    logger.info("Supabase client initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize Supabase client: {e}", exc_info=True)
    raise

# Railway URL-to-HTML service (public HTTPS API)
URLTOHTML_URL = os.getenv(
    "URLTOHTML_URL",
    "https://urltohtml-production.up.railway.app/api/v1/fetch-batch"
)

# Worker configuration
BATCH_SIZE = int(os.getenv('WORKER_BATCH_SIZE', '100'))
WORKER_ID = os.getenv('WORKER_ID', socket.gethostname() or str(uuid.uuid4())[:8])
POLL_INTERVAL = int(os.getenv('POLL_INTERVAL', '5'))  # seconds between batches
MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))
RETRY_DELAY = int(os.getenv('RETRY_DELAY', '10'))  # seconds

# Parallel processing configuration
# Use MAX_WORKERS if set (for backward compatibility), otherwise use EXTRACTION_WORKERS
# This controls how many URLs are processed in parallel for product extraction
# Railway Pro supports higher worker counts - recommended: 20-50 for Pro accounts
EXTRACTION_WORKERS = int(os.getenv('MAX_WORKERS', os.getenv('EXTRACTION_WORKERS', '50')))

# Database connection throttling - limit concurrent Supabase operations
# With 50 workers, we need to throttle DB operations to avoid connection pool exhaustion
MAX_CONCURRENT_DB_OPS = int(os.getenv('MAX_CONCURRENT_DB_OPS', '10'))  # Max concurrent DB operations
db_semaphore = Semaphore(MAX_CONCURRENT_DB_OPS)  # Semaphore to limit concurrent DB operations
db_lock = Lock()  # Lock for thread-safe operations

# Create requests session for URL-to-HTML service
session = requests.Session()
session.headers.update({
    'Content-Type': 'application/json',
    'User-Agent': 'ProductWorker/1.0'
})


def fetch_pending_urls(batch_size: int = BATCH_SIZE) -> List[Dict[str, Any]]:
    """
    Fetch a batch of pending URLs from Supabase and claim them.
    
    Args:
        batch_size: Number of URLs to fetch
        
    Returns:
        List of URL records with id, product_type_id, product_page_url
    """
    if not supabase:
        logger.error("Supabase client not initialized")
        return []
    
    try:
        # First, check total pending count for logging
        count_response = supabase.table('product_page_urls').select(
            'id', count='exact'
        ).eq('processing_status', 'pending').execute()
        
        total_pending = count_response.count if hasattr(count_response, 'count') else None
        
        # Fetch pending URLs ordered by ID for consistent batching
        response = supabase.table('product_page_urls').select(
            'id, product_type_id, product_page_url, retry_count'
        ).eq('processing_status', 'pending').order('id', desc=False).limit(batch_size).execute()
        
        if not response.data:
            if total_pending is not None:
                logger.info(f"No pending URLs found (Total pending in DB: {total_pending})")
            else:
                logger.info("No pending URLs found")
            return []
        
        urls = response.data
        
        # Extract ID range for logging
        url_ids = [url['id'] for url in urls]
        min_id = min(url_ids)
        max_id = max(url_ids)
        
        # Log detailed batch information
        logger.info("=" * 60)
        logger.info(f"FETCHED BATCH FROM SUPABASE")
        logger.info(f"  Total pending URLs in DB: {total_pending if total_pending is not None else 'unknown'}")
        logger.info(f"  Fetched: {len(urls)} URLs")
        logger.info(f"  ID Range: {min_id} to {max_id} (span: {max_id - min_id + 1} IDs)")
        logger.info(f"  Sample URLs:")
        for i, url_record in enumerate(urls[:3]):  # Show first 3 URLs
            logger.info(f"    [{url_record['id']}] {url_record['product_page_url'][:80]}...")
        if len(urls) > 3:
            logger.info(f"    ... and {len(urls) - 3} more")
        logger.info("=" * 60)
        
        # Claim the URLs by updating their status
        claim_timestamp = datetime.utcnow().isoformat()
        
        # Update all URLs to 'processing' status and set claim info
        update_response = supabase.table('product_page_urls').update({
            'processing_status': 'processing',
            'claimed_by': WORKER_ID,
            'claimed_at': claim_timestamp
        }).in_('id', url_ids).execute()
        
        logger.info(f"✓ Claimed {len(url_ids)} URLs for processing (IDs: {min_id}-{max_id})")
        
        return urls
        
    except Exception as e:
        logger.error(f"Error fetching pending URLs: {e}", exc_info=True)
        logger.error(f"Error details: {type(e).__name__}: {str(e)}")
        return []


def fetch_html_from_railway(urls: List[str]) -> List[Dict[str, Any]]:
    """
    Fetch HTML content from Railway URL-to-HTML service via HTTPS API.
    Matches the exact API usage pattern from the example.
    
    Args:
        urls: List of URLs to fetch HTML for
        
    Returns:
        List of dicts with 'url', 'html', 'status', 'method', and optionally 'error' keys
    """
    if not urls:
        return []
    
    logger.info(f"Sending {len(urls)} URLs to API...")
    logger.info(f"API: {URLTOHTML_URL}")
    
    payload = {"urls": urls}
    
    for attempt in range(MAX_RETRIES):
        try:
            # Make the request exactly as shown in the example
            response = session.post(
                URLTOHTML_URL,
                json=payload,
                timeout=3600  # 1 hour timeout (as per example)
            )
            
            # Check if request was successful (status code 200)
            if response.status_code == 200:
                data = response.json()
                
                # Parse summary exactly as in the example
                summary = data.get("summary", {})
                logger.info("=" * 60)
                logger.info("API RESULTS")
                logger.info("=" * 60)
                logger.info(f"Total URLs: {summary.get('total', 0)}")
                logger.info(f"Successful: {summary.get('success', 0)}")
                logger.info(f"Failed: {summary.get('failed', 0)}")
                logger.info(f"Success Rate: {summary.get('success_rate', 0):.2f}%")
                logger.info(f"Total Time: {summary.get('total_time', 0):.2f} seconds")
                
                # Print results by method
                by_method = summary.get('by_method', {})
                if by_method:
                    logger.info("Results by Method:")
                    for method, count in by_method.items():
                        logger.info(f"  {method}: {count}")
                
                # Get results array
                results = data.get("results", [])
                
                # Show successful URLs
                successful = [r for r in results if r.get("status") == "success"]
                if successful:
                    logger.info(f"Successful URLs ({len(successful)}):")
                    for result in successful[:5]:  # Show first 5
                        html_size = len(result.get("html", ""))
                        logger.info(f"  ✓ {result['url']}")
                        logger.info(f"    Method: {result.get('method', 'unknown')}, Size: {html_size:,} bytes")
                    if len(successful) > 5:
                        logger.info(f"    ... and {len(successful) - 5} more successful")
                
                # Show failed URLs
                failed = [r for r in results if r.get("status") == "failed"]
                if failed:
                    logger.info(f"Failed URLs ({len(failed)}):")
                    for result in failed[:5]:  # Show first 5
                        logger.info(f"  ✗ {result['url']}")
                        logger.info(f"    Error: {result.get('error', 'Unknown error')[:100]}")
                    if len(failed) > 5:
                        logger.info(f"    ... and {len(failed) - 5} more failed")
                
                logger.info("=" * 60)
                
                return results
            else:
                # Handle non-200 status codes
                error_text = response.text[:500]  # Limit error text length
                logger.error(f"Error: API returned status {response.status_code}")
                logger.error(f"Response: {error_text}")
                
                # For rate limiting (429), wait longer before retry
                if response.status_code == 429:
                    wait_time = RETRY_DELAY * (2 ** attempt) * 2  # Double wait for rate limits
                    logger.warning(f"Rate limited. Waiting {wait_time} seconds before retry...")
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(wait_time)
                        continue
                
                # For other errors, try to parse response if possible
                try:
                    error_data = response.json()
                    error_msg = error_data.get('error', error_data.get('message', f'HTTP {response.status_code}'))
                except:
                    error_msg = f'HTTP {response.status_code}: {error_text}'
                
                if attempt < MAX_RETRIES - 1:
                    delay = RETRY_DELAY * (2 ** attempt)
                    logger.info(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
                    continue
                else:
                    # Return failed results for all URLs
                    return [{'url': url, 'html': '', 'status': 'failed', 'error': error_msg} for url in urls]
            
        except requests.exceptions.Timeout as e:
            logger.error(f"Attempt {attempt + 1}/{MAX_RETRIES} timed out: {e}")
            if attempt < MAX_RETRIES - 1:
                delay = RETRY_DELAY * (2 ** attempt)
                logger.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                logger.error("All retry attempts exhausted due to timeout")
                return [{'url': url, 'html': '', 'status': 'failed', 'error': f'Request timeout: {str(e)}'} for url in urls]
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Attempt {attempt + 1}/{MAX_RETRIES} failed: {e}")
            
            if attempt < MAX_RETRIES - 1:
                delay = RETRY_DELAY * (2 ** attempt)  # Exponential backoff
                logger.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                logger.error("All retry attempts exhausted for HTML fetching")
                # Return empty results with error info
                return [{'url': url, 'html': '', 'status': 'failed', 'error': str(e)} for url in urls]
        except Exception as e:
            logger.error(f"Unexpected error fetching HTML: {e}", exc_info=True)
            return [{'url': url, 'html': '', 'status': 'failed', 'error': str(e)} for url in urls]
    
    return []


def extract_products_from_html(html_content: str, source_url: str, product_type_id: int) -> Dict[str, Any]:
    """
    Extract products from HTML content.
    
    Args:
        html_content: Raw HTML string
        source_url: Source URL of the HTML
        product_type_id: Product type ID for database storage
        
    Returns:
        Dict with products list and metadata
    """
    try:
        result = parser.parse_html(html_content, source_url, max_items=100)
        
        # Format products to match database schema
        formatted_products = []
        for product in result.get('products', []):
            formatted_product = {
                'product_name': product.get('title', ''),
                'product_url': product.get('product_url', ''),
                'image_url': product.get('image_url', ''),
                'cost': product.get('price'),
                'currency': product.get('currency', 'USD'),
                'rating': product.get('rating'),
                'review_count': product.get('review_count'),
                'brand': product.get('brand'),
                'in_stock': product.get('in_stock', True),
                'description': product.get('description', ''),
                'original_price': product.get('original_price') or product.get('price'),
            }
            # Only include products with at least a name or URL
            if formatted_product['product_name'] or formatted_product['product_url']:
                formatted_products.append(formatted_product)
        
        return {
            'success': result.get('success', False),
            'num_products': len(formatted_products),
            'products': formatted_products,
            'extraction_strategy': result.get('extraction_strategy', 'none'),
            'error': result.get('error'),
        }
    except Exception as e:
        logger.error(f"Error extracting products from {source_url}: {e}", exc_info=True)
        return {
            'success': False,
            'num_products': 0,
            'products': [],
            'error': f"{type(e).__name__}: {str(e)}",
        }


def save_products_to_supabase(products: List[Dict], platform_url: str, product_type_id: int) -> int:
    """
    Save extracted products to Supabase r_product_data table.
    Uses batch insert with connection throttling to handle high concurrency.
    
    Args:
        products: List of product dictionaries
        platform_url: Platform URL
        product_type_id: Product type ID
        
    Returns:
        Number of products successfully saved
    """
    if not supabase:
        logger.warning("Supabase client not initialized. Skipping database save.")
        return 0
    
    if not products:
        return 0
    
    # Acquire semaphore to limit concurrent database operations
    # This prevents connection pool exhaustion with 50 parallel workers
    db_semaphore.acquire()
    try:
        # Prepare all records first
        db_records = []
        for product in products:
            try:
                # Prepare data for Supabase table
                db_record = {
                    'platform_url': platform_url,
                    'product_name': product.get('product_name', ''),
                    'product_url': product.get('product_url', ''),
                    'product_image_url': product.get('image_url') or None,
                    'original_price': str(product.get('original_price', '')) if product.get('original_price') else None,
                    'current_price': float(product.get('cost')) if product.get('cost') else None,
                    'product_type_id': product_type_id,
                    'rating': float(product.get('rating')) if product.get('rating') else None,
                    'reviews': int(product.get('review_count')) if product.get('review_count') else None,
                    'brand': product.get('brand') or None,
                    'in_stock': 'Yes' if product.get('in_stock', True) else 'No',
                    'description': product.get('description') or None,
                    'category_id': None,
                    'searched_product_id': None,
                }
                
                # Only save if we have required fields
                if db_record['product_name'] and db_record['product_url']:
                    db_records.append(db_record)
            except Exception as e:
                logger.debug(f"Error preparing product record: {e}")
                continue
        
        if not db_records:
            return 0
        
        # Use batch insert with retry logic
        saved_count = 0
        max_retries = 3
        # Increase batch size for better performance (Supabase supports up to 1000 rows per insert)
        batch_size = 100  # Insert in batches to avoid connection issues
        
        for i in range(0, len(db_records), batch_size):
            batch = db_records[i:i + batch_size]
            
            for attempt in range(max_retries):
                try:
                    # Batch insert with thread-safe operation
                    with db_lock:
                        result = supabase.table('r_product_data').insert(batch).execute()
                    
                    if result.data:
                        saved_count += len(result.data)
                        break  # Success, move to next batch
                    else:
                        if attempt < max_retries - 1:
                            wait_time = 0.5 * (2 ** attempt)  # Exponential backoff
                            logger.warning(f"Batch insert returned no data, retrying in {wait_time}s...")
                            time.sleep(wait_time)
                        else:
                            logger.warning(f"Failed to save batch of {len(batch)} products after {max_retries} attempts")
                            
                except Exception as e:
                    error_type = type(e).__name__
                    error_msg = str(e)
                    
                    # Check if it's a connection error
                    if 'RemoteProtocolError' in error_type or 'Server disconnected' in error_msg or 'Connection' in error_type:
                        if attempt < max_retries - 1:
                            wait_time = 1.0 * (2 ** attempt)  # Longer wait for connection errors
                            logger.warning(f"Connection error (attempt {attempt + 1}/{max_retries}): {error_type}. Retrying in {wait_time}s...")
                            time.sleep(wait_time)
                            continue
                        else:
                            logger.error(f"Connection error after {max_retries} attempts: {error_type}: {error_msg}")
                            # Try individual inserts as fallback
                            saved_count += _save_products_individually(batch, platform_url, product_type_id)
                            break
                    else:
                        # Other errors - log and continue
                        logger.error(f"Error saving batch to Supabase: {error_type}: {error_msg}")
                        if attempt < max_retries - 1:
                            time.sleep(0.5 * (2 ** attempt))
                        else:
                            break
        
        if saved_count > 0:
            logger.debug(f"Successfully saved {saved_count}/{len(products)} products to Supabase")
        
        return saved_count
        
    finally:
        # Always release semaphore, even on error
        db_semaphore.release()


def _save_products_individually(db_records: List[Dict], platform_url: str, product_type_id: int) -> int:
    """
    Fallback: Save products one by one with retries.
    Used when batch insert fails.
    Uses semaphore to limit concurrent operations.
    """
    saved_count = 0
    
    for db_record in db_records:
        db_semaphore.acquire()
        try:
            for attempt in range(3):
                try:
                    with db_lock:
                        result = supabase.table('r_product_data').insert(db_record).execute()
                    if result.data:
                        saved_count += 1
                        break
                except Exception as e:
                    if attempt < 2:
                        time.sleep(0.5 * (2 ** attempt))
                    else:
                        logger.debug(f"Failed to save individual product after retries: {type(e).__name__}")
        finally:
            db_semaphore.release()
    
    return saved_count


def update_url_status(
    url_id: int,
    success: bool,
    products_found: int = 0,
    products_saved: int = 0,
    error_message: Optional[str] = None
):
    """
    Update the processing status of a URL in product_page_urls table.
    Uses connection throttling to prevent pool exhaustion.
    
    Args:
        url_id: ID of the URL record
        success: Whether processing was successful
        products_found: Number of products found
        products_saved: Number of products saved to database
        error_message: Error message if processing failed
    """
    if not supabase:
        logger.warning("Supabase client not initialized. Skipping status update.")
        return
    
    # Acquire semaphore to limit concurrent database operations
    db_semaphore.acquire()
    try:
        update_data = {
            'processing_status': 'completed' if success else 'failed',
            'processed_at': datetime.utcnow().isoformat(),
            'success': success,
            'products_found': products_found,
            'products_saved': products_saved,
        }
        
        if error_message:
            # Truncate error message if too long (database constraint)
            if len(error_message) > 1000:
                error_message = error_message[:997] + "..."
            update_data['error_message'] = error_message
            # Increment retry count on failure
            try:
                with db_lock:
                    current_record = supabase.table('product_page_urls').select('retry_count').eq('id', url_id).execute()
                if current_record.data:
                    current_retry_count = current_record.data[0].get('retry_count', 0) or 0
                    update_data['retry_count'] = current_retry_count + 1
            except Exception as e:
                logger.warning(f"Could not fetch current retry count: {e}")
                update_data['retry_count'] = 1
        
        with db_lock:
            supabase.table('product_page_urls').update(update_data).eq('id', url_id).execute()
        logger.debug(f"Updated status for URL ID {url_id}: success={success}, products={products_found}")
        
    except Exception as e:
        logger.error(f"Error updating URL status for ID {url_id}: {e}", exc_info=True)
    finally:
        # Always release semaphore
        db_semaphore.release()


def process_batch(url_records: List[Dict[str, Any]]):
    """
    Process a batch of URLs: fetch HTML, extract products, save to database.
    
    Args:
        url_records: List of URL records with id, product_type_id, product_page_url
    """
    if not url_records:
        return
    
    # Extract ID range for logging
    url_ids = [record['id'] for record in url_records]
    min_id = min(url_ids)
    max_id = max(url_ids)
    
    logger.info("=" * 60)
    logger.info(f"PROCESSING BATCH")
    logger.info(f"  Batch size: {len(url_records)} URLs")
    logger.info(f"  ID Range: {min_id} to {max_id}")
    logger.info("=" * 60)
    
    # Filter out Meesho URLs (temporary condition - to be removed later)
    meesho_urls = []
    non_meesho_records = []
    
    for record in url_records:
        url = record.get('product_page_url', '').lower()
        if 'meesho' in url:
            meesho_urls.append(record)
        else:
            non_meesho_records.append(record)
    
    if meesho_urls:
        logger.info(f"⚠️  Skipping {len(meesho_urls)} Meesho URLs (temporary filter)")
        for meesho_record in meesho_urls:
            url_id = meesho_record['id']
            url = meesho_record['product_page_url']
            logger.info(f"  [ID {url_id}] Skipped Meesho URL: {url}")
            # Mark as failed with skip reason
            update_url_status(
                url_id=url_id,
                success=False,
                error_message="Skipped: Meesho links temporarily excluded (filter will be removed later)"
            )
    
    # Extract URLs for HTML fetching (only non-Meesho URLs)
    urls = [record['product_page_url'] for record in non_meesho_records]
    
    # Create mapping from URL to record
    url_to_record = {record['product_page_url']: record for record in non_meesho_records}
    
    if not urls:
        logger.info("No URLs to process after filtering (all were Meesho URLs)")
        return
    
    logger.info(f"Processing {len(urls)} non-Meesho URLs")
    logger.info(f"📤 Sending all {len(urls)} URLs in a SINGLE batch request to URL-to-HTML API...")
    
    # Fetch HTML from Railway service - ALL URLs in ONE request
    html_results = fetch_html_from_railway(urls)
    
    logger.info(f"📥 Received HTML responses for {len(html_results)} URLs")
    logger.info(f"⚡ Now processing HTML contents in parallel using {EXTRACTION_WORKERS} workers...")
    
    # Filter successful results for parallel processing
    successful_results = []
    failed_results = []
    
    for html_result in html_results:
        url = html_result.get('url', '')
        status = html_result.get('status', '')
        html = html_result.get('html', '')
        
        # Find corresponding record
        record = url_to_record.get(url)
        if not record:
            logger.warning(f"No record found for URL: {url}")
            continue
        
        # Check if HTML fetch was successful
        is_success = status == 'success' and html and (isinstance(html, str) and len(html.strip()) > 0)
        
        if is_success:
            successful_results.append({
                'html_result': html_result,
                'record': record
            })
        else:
            failed_results.append({
                'html_result': html_result,
                'record': record
            })
    
    # Process failed results immediately (no HTML to extract)
    for item in failed_results:
        html_result = item['html_result']
        record = item['record']
        url = html_result.get('url', '')
        url_id = record['id']
        error_msg = html_result.get('error', 'No HTML content received')
        method = html_result.get('method', 'unknown')
        
        logger.warning(f"[ID {url_id}] Failed to fetch HTML for {url}: {error_msg} (Method: {method})")
        update_url_status(
            url_id=url_id,
            success=False,
            error_message=error_msg
        )
    
    # Process successful results in parallel
    if successful_results:
        logger.info(f"Processing {len(successful_results)} URLs with HTML content using {EXTRACTION_WORKERS} parallel workers...")
        start_time = time.time()
        
        def process_single_url(item):
            """Process a single URL: extract products and save to database."""
            html_result = item['html_result']
            record = item['record']
            url = html_result.get('url', '')
            html = html_result.get('html', '')
            method = html_result.get('method', 'unknown')
            url_id = record['id']
            product_type_id = record['product_type_id']
            
            try:
                # Extract products from HTML
                extraction_result = extract_products_from_html(html, url, product_type_id)
                
                products = extraction_result.get('products', [])
                products_found = len(products)
                
                # Save products to database
                products_saved = 0
                if products:
                    products_saved = save_products_to_supabase(products, url, product_type_id)
                
                # Update URL status
                success = extraction_result.get('success', False) and products_found > 0
                error_message = extraction_result.get('error')
                
                update_url_status(
                    url_id=url_id,
                    success=success,
                    products_found=products_found,
                    products_saved=products_saved,
                    error_message=error_message
                )
                
                return {
                    'url_id': url_id,
                    'url': url,
                    'success': True,
                    'products_found': products_found,
                    'products_saved': products_saved,
                    'error': None
                }
                
            except Exception as e:
                logger.error(f"[ID {url_id}] Error processing {url}: {e}", exc_info=True)
                update_url_status(
                    url_id=url_id,
                    success=False,
                    error_message=f"{type(e).__name__}: {str(e)}"
                )
                return {
                    'url_id': url_id,
                    'url': url,
                    'success': False,
                    'products_found': 0,
                    'products_saved': 0,
                    'error': str(e)
                }
        
        # Process in parallel using ThreadPoolExecutor
        processed_count = 0
        with ThreadPoolExecutor(max_workers=EXTRACTION_WORKERS) as executor:
            # Submit all tasks
            future_to_item = {
                executor.submit(process_single_url, item): item
                for item in successful_results
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_item):
                item = future_to_item[future]
                try:
                    result = future.result()
                    processed_count += 1
                    
                    if result['success']:
                        logger.info(f"[ID {result['url_id']}] Processed {result['url']}: "
                                  f"{result['products_found']} products found, {result['products_saved']} saved")
                    else:
                        logger.warning(f"[ID {result['url_id']}] Failed: {result['error']}")
                        
                except Exception as e:
                    logger.error(f"Error in parallel processing: {e}", exc_info=True)
        
        elapsed_time = time.time() - start_time
        logger.info(f"✓ Parallel extraction complete: {processed_count}/{len(successful_results)} URLs processed in {elapsed_time:.2f}s "
                  f"(avg: {elapsed_time/len(successful_results):.2f}s per URL)")
    
    # Log batch completion summary
    logger.info("=" * 60)
    logger.info(f"BATCH PROCESSING COMPLETE")
    logger.info(f"  Total URLs in batch: {len(url_records)}")
    logger.info(f"  HTML results received: {len(html_results)}")
    logger.info(f"  ID Range processed: {min_id} to {max_id}")
    logger.info("=" * 60)


def run_worker():
    """
    Main worker loop: continuously fetch and process URLs.
    """
    logger.info("=" * 60)
    logger.info(f"Product Extraction Worker Running")
    logger.info(f"Worker ID: {WORKER_ID}")
    logger.info(f"Batch size: {BATCH_SIZE}")
    logger.info(f"Poll interval: {POLL_INTERVAL}s")
    logger.info(f"Parallel extraction workers: {EXTRACTION_WORKERS} (from MAX_WORKERS or EXTRACTION_WORKERS env var)")
    logger.info(f"Max concurrent DB operations: {MAX_CONCURRENT_DB_OPS} (throttled to prevent connection errors)")
    if EXTRACTION_WORKERS >= 30:
        logger.info(f"⚡ High-performance mode: {EXTRACTION_WORKERS} workers (Railway Pro recommended)")
    logger.info(f"URL-to-HTML service: {URLTOHTML_URL}")
    logger.info(f"Using public HTTPS API endpoint")
    logger.info(f"Supabase URL: {SUPABASE_URL[:50]}..." if SUPABASE_URL else "Not set")
    logger.info("=" * 60)
    
    consecutive_empty_batches = 0
    max_empty_batches = 10  # Log warning after 10 empty batches
    
    while True:
        try:
            # Fetch pending URLs
            url_records = fetch_pending_urls(BATCH_SIZE)
            
            if not url_records:
                consecutive_empty_batches += 1
                if consecutive_empty_batches >= max_empty_batches:
                    logger.info(f"No pending URLs found (checked {consecutive_empty_batches} times). Waiting {POLL_INTERVAL}s...")
                    consecutive_empty_batches = 0  # Reset counter
                time.sleep(POLL_INTERVAL)
                continue
            
            # Reset empty batch counter
            consecutive_empty_batches = 0
            
            # Process the batch
            process_batch(url_records)
            
            # Small delay between batches
            time.sleep(1)
            
        except KeyboardInterrupt:
            logger.info("Worker stopped by user")
            break
        except Exception as e:
            logger.error(f"Error in worker loop: {e}", exc_info=True)
            logger.info(f"Waiting {POLL_INTERVAL}s before retrying...")
            time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    try:
        run_worker()
    except KeyboardInterrupt:
        logger.info("Worker stopped by user")
    except Exception as e:
        logger.critical(f"Fatal error in worker: {e}", exc_info=True)
        raise

