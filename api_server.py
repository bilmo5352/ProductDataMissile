"""
Flask API Server - Product Extraction API
==========================================
API endpoint for extracting product data from HTML content.

Features:
- Single or batch HTML processing
- Parallel processing with configurable workers
- Returns structured product data
"""

from flask import Flask, request, jsonify
from flask_cors import CORS
from html_parser import HTMLProductParser
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Optional
import logging
from datetime import datetime
import traceback
import os
from dotenv import load_dotenv
from supabase import create_client, Client

# Load environment variables
load_dotenv()

# Initialize Flask app
app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize parser
parser = HTMLProductParser()

# Initialize Supabase client
SUPABASE_URL = os.getenv('SUPABASE_URL', '')
SUPABASE_KEY = os.getenv('SUPABASE_KEY', '')
supabase: Optional[Client] = None

if SUPABASE_URL and SUPABASE_KEY:
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        logger.info("Supabase client initialized successfully")
    except Exception as e:
        logger.warning(f"Failed to initialize Supabase client: {e}")
        supabase = None
else:
    logger.warning("Supabase credentials not provided. Database storage will be disabled.")

# Import configuration
try:
    from api_config import (
        MAX_WORKERS as DEFAULT_MAX_WORKERS,
        MAX_PRODUCTS_PER_PAGE as DEFAULT_MAX_PRODUCTS,
        FLASK_HOST,
        FLASK_PORT,
        FLASK_DEBUG,
        MAX_BATCH_SIZE
    )
except ImportError:
    # Fallback defaults if config file doesn't exist
    DEFAULT_MAX_WORKERS = int(os.getenv('MAX_WORKERS', '4'))
    DEFAULT_MAX_PRODUCTS = int(os.getenv('MAX_PRODUCTS_PER_PAGE', '100'))
    FLASK_HOST = os.getenv('FLASK_HOST', '0.0.0.0')
    FLASK_PORT = int(os.getenv('FLASK_PORT', '5000'))
    FLASK_DEBUG = os.getenv('FLASK_DEBUG', 'False').lower() == 'true'
    MAX_BATCH_SIZE = int(os.getenv('MAX_BATCH_SIZE', '50'))

# Configuration (can be updated via API)
MAX_WORKERS = DEFAULT_MAX_WORKERS
MAX_PRODUCTS_PER_PAGE = DEFAULT_MAX_PRODUCTS


def extract_products_from_html(html_content: str, source_url: str, product_type_id: Optional[int] = None) -> Dict[str, Any]:
    """
    Extract products from a single HTML content.
    
    Args:
        html_content: Raw HTML string
        source_url: Source URL of the HTML
        product_type_id: Product type ID for database storage
        
    Returns:
        Dict with platform_url, products list, and metadata
    """
    try:
        result = parser.parse_html(html_content, source_url, max_items=MAX_PRODUCTS_PER_PAGE)
        
        # Format products to match required output format
        formatted_products = []
        for product in result.get('products', []):
            formatted_product = {
                'product_name': product.get('title', ''),
                'product_url': product.get('product_url', ''),
                'cost': product.get('price'),
                'currency': product.get('currency', 'USD'),
                'image_url': product.get('image_url', ''),
                # Additional fields for database
                'rating': product.get('rating'),
                'review_count': product.get('review_count'),
                'brand': product.get('brand'),
                'in_stock': product.get('in_stock', True),
                'description': product.get('description', ''),
                'original_price': product.get('original_price') or product.get('price'),  # Use price as original if no original_price
            }
            # Only include products with at least a name or URL
            if formatted_product['product_name'] or formatted_product['product_url']:
                formatted_products.append(formatted_product)
        
        # Save to Supabase if configured
        saved_count = 0
        if supabase and product_type_id and formatted_products:
            saved_count = save_products_to_supabase(formatted_products, source_url, product_type_id)
        
        return {
            'platform_url': source_url,
            'success': result.get('success', False),
            'num_products': len(formatted_products),
            'products': formatted_products,
            'extraction_strategy': result.get('extraction_strategy', 'none'),
            'error': result.get('error'),
            'saved_to_db': saved_count
        }
    except Exception as e:
        logger.error(f"Error extracting products from {source_url}: {e}", exc_info=True)
        return {
            'platform_url': source_url,
            'success': False,
            'num_products': 0,
            'products': [],
            'error': f"{type(e).__name__}: {str(e)}",
            'saved_to_db': 0
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
                logger.debug(f"Skipping product with missing required fields: {db_record}")
                continue
            
            # Insert into Supabase
            result = supabase.table('r_product_data').insert(db_record).execute()
            
            if result.data:
                saved_count += 1
                logger.debug(f"Saved product: {db_record['product_name'][:50]}")
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


@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    return jsonify({
        'status': 'healthy',
        'service': 'Product Extraction API',
        'timestamp': datetime.now().isoformat()
    })


@app.route('/extract', methods=['POST'])
def extract_products():
    """
    Extract products from HTML content.
    
    Request body (single HTML):
    {
        "html": "<html>...</html>",
        "url": "https://example.com/products"
    }
    
    Request body (batch):
    {
        "html_contents": [
            {
                "html": "<html>...</html>",
                "url": "https://example.com/products"
            },
            ...
        ],
        "max_workers": 4  # Optional, defaults to config value
    }
    
    Response:
    {
        "success": true,
        "results": [
            {
                "platform_url": "https://example.com/products",
                "success": true,
                "num_products": 10,
                "products": [
                    {
                        "product_name": "Product Name",
                        "product_url": "https://...",
                        "cost": 99.99,
                        "currency": "USD",
                        "image_url": "https://..."
                    },
                    ...
                ]
            },
            ...
        ],
        "total_processed": 2,
        "total_products": 20,
        "processing_time_seconds": 1.23
    }
    """
    start_time = datetime.now()
    
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({
                'success': False,
                'error': 'No JSON data provided'
            }), 400
        
        # Check if it's a single HTML or batch
        if 'html' in data and 'url' in data:
            # Single HTML processing
            html_content = data.get('html', '')
            source_url = data.get('url', '')
            product_type_id = data.get('product_type_id')
            
            if not html_content:
                return jsonify({
                    'success': False,
                    'error': 'HTML content is required'
                }), 400
            
            if not source_url:
                return jsonify({
                    'success': False,
                    'error': 'URL is required'
                }), 400
            
            # Convert product_type_id to int if provided
            if product_type_id is not None:
                try:
                    product_type_id = int(product_type_id)
                except (ValueError, TypeError):
                    return jsonify({
                        'success': False,
                        'error': 'product_type_id must be an integer'
                    }), 400
            
            result = extract_products_from_html(html_content, source_url, product_type_id)
            
            processing_time = (datetime.now() - start_time).total_seconds()
            
            return jsonify({
                'success': True,
                'results': [result],
                'total_processed': 1,
                'total_products': result['num_products'],
                'total_saved_to_db': result.get('saved_to_db', 0),
                'processing_time_seconds': round(processing_time, 2)
            })
        
        elif 'html_contents' in data:
            # Batch processing
            html_contents = data.get('html_contents', [])
            max_workers = data.get('max_workers', MAX_WORKERS)
            
            if not html_contents:
                return jsonify({
                    'success': False,
                    'error': 'html_contents array is required'
                }), 400
            
            if not isinstance(html_contents, list):
                return jsonify({
                    'success': False,
                    'error': 'html_contents must be an array'
                }), 400
            
            # Check batch size limit
            if len(html_contents) > MAX_BATCH_SIZE:
                return jsonify({
                    'success': False,
                    'error': f'Batch size exceeds maximum of {MAX_BATCH_SIZE}. Received {len(html_contents)} items.'
                }), 400
            
            # Validate max_workers
            try:
                max_workers = int(max_workers)
                if max_workers < 1:
                    max_workers = 1
                elif max_workers > 20:  # Cap at 20 for safety
                    max_workers = 20
            except (ValueError, TypeError):
                max_workers = MAX_WORKERS
            
            logger.info(f"Processing {len(html_contents)} HTML contents with {max_workers} workers")
            
            # Process in parallel
            results = []
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all tasks
                future_to_url = {
                    executor.submit(
                        extract_products_from_html,
                        item.get('html', ''),
                        item.get('url', ''),
                        item.get('product_type_id')  # Pass product_type_id from each item
                    ): item.get('url', 'unknown')
                    for item in html_contents
                }
                
                # Collect results as they complete
                for future in as_completed(future_to_url):
                    url = future_to_url[future]
                    try:
                        result = future.result()
                        results.append(result)
                    except Exception as e:
                        logger.error(f"Error processing {url}: {e}", exc_info=True)
                        results.append({
                            'platform_url': url,
                            'success': False,
                            'num_products': 0,
                            'products': [],
                            'error': f"{type(e).__name__}: {str(e)}"
                        })
            
            # Calculate totals
            total_products = sum(r.get('num_products', 0) for r in results)
            total_saved = sum(r.get('saved_to_db', 0) for r in results)
            processing_time = (datetime.now() - start_time).total_seconds()
            
            return jsonify({
                'success': True,
                'results': results,
                'total_processed': len(results),
                'total_products': total_products,
                'total_saved_to_db': total_saved,
                'processing_time_seconds': round(processing_time, 2),
                'max_workers_used': max_workers
            })
        
        else:
            return jsonify({
                'success': False,
                'error': 'Invalid request format. Provide either {"html": "...", "url": "..."} or {"html_contents": [...]}'
            }), 400
    
    except Exception as e:
        logger.error(f"Error in extract_products endpoint: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'error': f"{type(e).__name__}: {str(e)}",
            'traceback': traceback.format_exc() if app.debug else None
        }), 500


@app.route('/config', methods=['GET', 'POST'])
def config_endpoint():
    """
    Get or update configuration.
    
    GET: Returns current configuration
    POST: Updates configuration
    {
        "max_workers": 4,
        "max_products_per_page": 100
    }
    """
    global MAX_WORKERS, MAX_PRODUCTS_PER_PAGE
    
    if request.method == 'GET':
        return jsonify({
            'max_workers': MAX_WORKERS,
            'max_products_per_page': MAX_PRODUCTS_PER_PAGE
        })
    
    elif request.method == 'POST':
        data = request.get_json()
        
        if 'max_workers' in data:
            try:
                max_workers = int(data['max_workers'])
                if 1 <= max_workers <= 20:
                    MAX_WORKERS = max_workers
                else:
                    return jsonify({
                        'success': False,
                        'error': 'max_workers must be between 1 and 20'
                    }), 400
            except (ValueError, TypeError):
                return jsonify({
                    'success': False,
                    'error': 'max_workers must be an integer'
                }), 400
        
        if 'max_products_per_page' in data:
            try:
                max_products = int(data['max_products_per_page'])
                if 1 <= max_products <= 1000:
                    MAX_PRODUCTS_PER_PAGE = max_products
                else:
                    return jsonify({
                        'success': False,
                        'error': 'max_products_per_page must be between 1 and 1000'
                    }), 400
            except (ValueError, TypeError):
                return jsonify({
                    'success': False,
                    'error': 'max_products_per_page must be an integer'
                }), 400
        
        return jsonify({
            'success': True,
            'message': 'Configuration updated',
            'config': {
                'max_workers': MAX_WORKERS,
                'max_products_per_page': MAX_PRODUCTS_PER_PAGE
            }
        })


if __name__ == '__main__':
    # Run the Flask app
    # In production, Railway will use gunicorn, so this is mainly for local development
    port = int(os.getenv('PORT', FLASK_PORT))
    logger.info(f"Starting Product Extraction API on {FLASK_HOST}:{port}")
    logger.info(f"Default max_workers: {MAX_WORKERS}, max_products_per_page: {MAX_PRODUCTS_PER_PAGE}")
    logger.info(f"Environment: {os.getenv('ENVIRONMENT', 'development')}")
    app.run(host=FLASK_HOST, port=port, debug=FLASK_DEBUG)

