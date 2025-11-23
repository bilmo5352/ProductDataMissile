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

# Railway URL-to-HTML service (private networking)
URLTOHTML_URL = os.getenv(
    "URLTOHTML_PRIVATE_URL",
    "http://urltohtml.railway.internal:8000/api/v1/fetch-batch"
)

# Worker configuration
BATCH_SIZE = int(os.getenv('WORKER_BATCH_SIZE', '100'))
WORKER_ID = os.getenv('WORKER_ID', socket.gethostname() or str(uuid.uuid4())[:8])
POLL_INTERVAL = int(os.getenv('POLL_INTERVAL', '5'))  # seconds between batches
MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))
RETRY_DELAY = int(os.getenv('RETRY_DELAY', '10'))  # seconds

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
        # Fetch pending URLs and claim them atomically
        # Use a transaction-like approach: select and update in one query where possible
        # For Supabase, we'll fetch and then update claimed status
        
        # First, fetch pending URLs
        response = supabase.table('product_page_urls').select(
            'id, product_type_id, product_page_url, retry_count'
        ).eq('processing_status', 'pending').limit(batch_size).execute()
        
        if not response.data:
            return []
        
        urls = response.data
        logger.info(f"Fetched {len(urls)} pending URLs")
        
        # Claim the URLs by updating their status
        url_ids = [url['id'] for url in urls]
        claim_timestamp = datetime.utcnow().isoformat()
        
        # Update all URLs to 'processing' status and set claim info
        update_response = supabase.table('product_page_urls').update({
            'processing_status': 'processing',
            'claimed_by': WORKER_ID,
            'claimed_at': claim_timestamp
        }).in_('id', url_ids).execute()
        
        logger.info(f"Claimed {len(url_ids)} URLs for processing")
        
        return urls
        
    except Exception as e:
        logger.error(f"Error fetching pending URLs: {e}", exc_info=True)
        return []


def fetch_html_from_railway(urls: List[str]) -> List[Dict[str, Any]]:
    """
    Fetch HTML content from Railway URL-to-HTML service via private networking.
    
    Args:
        urls: List of URLs to fetch HTML for
        
    Returns:
        List of dicts with 'url' and 'html' keys
    """
    if not urls:
        return []
    
    logger.info(f"Fetching HTML for {len(urls)} URLs from Railway service")
    
    payload = {"urls": urls}
    
    for attempt in range(MAX_RETRIES):
        try:
            response = session.post(
                URLTOHTML_URL,
                json=payload,
                timeout=300  # 5 minute timeout for batch
            )
            response.raise_for_status()
            
            data = response.json()
            
            # Parse API response
            if isinstance(data, dict) and 'results' in data:
                results = data['results']
            elif isinstance(data, list):
                results = data
            else:
                logger.error(f"Unexpected API response format: {type(data)}")
                return []
            
            logger.info(f"Successfully fetched HTML for {len(results)} URLs")
            return results
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Attempt {attempt + 1}/{MAX_RETRIES} failed: {e}")
            
            if attempt < MAX_RETRIES - 1:
                delay = RETRY_DELAY * (2 ** attempt)  # Exponential backoff
                logger.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                logger.error("All retry attempts exhausted for HTML fetching")
                # Return empty results with error info
                return [{'url': url, 'html': '', 'status': 'error', 'error': str(e)} for url in urls]
    
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
    
    saved_count = 0
    errors = []
    
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
            if not db_record['product_name'] or not db_record['product_url']:
                logger.debug(f"Skipping product with missing required fields")
                continue
            
            # Insert into Supabase
            result = supabase.table('r_product_data').insert(db_record).execute()
            
            if result.data:
                saved_count += 1
            else:
                errors.append(f"Failed to save product: {db_record.get('product_name', 'Unknown')}")
                
        except Exception as e:
            error_msg = f"Error saving product to Supabase: {type(e).__name__}: {str(e)}"
            logger.error(error_msg)
            errors.append(error_msg)
            continue
    
    if saved_count > 0:
        logger.info(f"Successfully saved {saved_count}/{len(products)} products to Supabase")
    if errors:
        logger.warning(f"Encountered {len(errors)} errors while saving to Supabase")
    
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
                current_record = supabase.table('product_page_urls').select('retry_count').eq('id', url_id).execute()
                if current_record.data:
                    current_retry_count = current_record.data[0].get('retry_count', 0) or 0
                    update_data['retry_count'] = current_retry_count + 1
            except Exception as e:
                logger.warning(f"Could not fetch current retry count: {e}")
                update_data['retry_count'] = 1
        
        supabase.table('product_page_urls').update(update_data).eq('id', url_id).execute()
        logger.debug(f"Updated status for URL ID {url_id}: success={success}, products={products_found}")
        
    except Exception as e:
        logger.error(f"Error updating URL status for ID {url_id}: {e}", exc_info=True)


def process_batch(url_records: List[Dict[str, Any]]):
    """
    Process a batch of URLs: fetch HTML, extract products, save to database.
    
    Args:
        url_records: List of URL records with id, product_type_id, product_page_url
    """
    if not url_records:
        return
    
    logger.info(f"Processing batch of {len(url_records)} URLs")
    
    # Extract URLs for HTML fetching
    urls = [record['product_page_url'] for record in url_records]
    
    # Create mapping from URL to record
    url_to_record = {record['product_page_url']: record for record in url_records}
    
    # Fetch HTML from Railway service
    html_results = fetch_html_from_railway(urls)
    
    # Process each result
    for html_result in html_results:
        url = html_result.get('url', '')
        html = html_result.get('html', '')
        status = html_result.get('status', '')
        success_flag = html_result.get('success', None)
        
        # Find corresponding record
        record = url_to_record.get(url)
        if not record:
            logger.warning(f"No record found for URL: {url}")
            continue
        
        url_id = record['id']
        product_type_id = record['product_type_id']
        
        # Check if HTML fetch was successful
        # Handle both string status ('success') and boolean success flag
        is_success = (
            status == 'success' or 
            (success_flag is True) or
            (status == '' and success_flag is None and html)  # Assume success if no status but HTML present
        )
        
        if not is_success or not html or (isinstance(html, str) and len(html.strip()) == 0):
            error_msg = html_result.get('error', 'No HTML content received')
            logger.warning(f"Failed to fetch HTML for {url}: {error_msg}")
            update_url_status(
                url_id=url_id,
                success=False,
                error_message=error_msg
            )
            continue
        
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
            
            logger.info(f"Processed {url}: {products_found} products found, {products_saved} saved")
            
        except Exception as e:
            logger.error(f"Error processing {url}: {e}", exc_info=True)
            update_url_status(
                url_id=url_id,
                success=False,
                error_message=f"{type(e).__name__}: {str(e)}"
            )


def run_worker():
    """
    Main worker loop: continuously fetch and process URLs.
    """
    logger.info("=" * 60)
    logger.info(f"Product Extraction Worker Running")
    logger.info(f"Worker ID: {WORKER_ID}")
    logger.info(f"Batch size: {BATCH_SIZE}")
    logger.info(f"Poll interval: {POLL_INTERVAL}s")
    logger.info(f"URL-to-HTML service: {URLTOHTML_URL}")
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

