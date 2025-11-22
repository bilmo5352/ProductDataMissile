"""
Configuration - Product Extraction System
==========================================
Centralized configuration for HTML fetcher and parser.
"""

from pathlib import Path
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# ============ Environment Detection ============
ENVIRONMENT = os.getenv('ENVIRONMENT', 'development').lower()
IS_PRODUCTION = ENVIRONMENT == 'production'

# ============ API Configuration ============

# Chrome Worker API endpoint (from environment variable)
CHROME_WORKER_API_URL = os.getenv(
    'CHROME_WORKER_API_URL',
    'https://chromeworkers-production.up.railway.app/render'
)

# ============ Processing Configuration ============

# Number of URLs to process in a single batch
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '10'))

# Maximum retry attempts for failed requests
MAX_RETRIES = int(os.getenv('MAX_RETRIES', '2'))

# Delay between retries (seconds)
RETRY_DELAY = int(os.getenv('RETRY_DELAY', '2'))

# Maximum products to extract per URL
MAX_PRODUCTS_PER_PAGE = int(os.getenv('MAX_PRODUCTS_PER_PAGE', '100'))

# Timeout for API requests (seconds)
API_TIMEOUT = int(os.getenv('API_TIMEOUT', '120'))

# ============ Storage Configuration ============

# Whether to save HTML cache (disabled in production by default)
SAVE_HTML_CACHE = os.getenv('SAVE_HTML_CACHE', 'False' if IS_PRODUCTION else 'True').lower() == 'true'

# Whether to save results to files (disabled in production by default)
SAVE_RESULTS = os.getenv('SAVE_RESULTS', 'False' if IS_PRODUCTION else 'True').lower() == 'true'

# Whether to save logs to files
SAVE_LOGS = os.getenv('SAVE_LOGS', 'True').lower() == 'true'

# ============ Directory Configuration ============

# Base directory (current working directory)
BASE_DIR = Path.cwd()

# HTML cache directory (only used if SAVE_HTML_CACHE is True)
HTML_CACHE_DIR = BASE_DIR / "html_cache" if SAVE_HTML_CACHE else None

# Results directory (only used if SAVE_RESULTS is True)
RESULTS_DIR = BASE_DIR / "results" if SAVE_RESULTS else None

# Logs directory (only used if SAVE_LOGS is True)
LOGS_DIR = BASE_DIR / "logs" if SAVE_LOGS else None


# ============ Parser Configuration ============

# Minimum products required for strategy to be considered successful
MIN_PRODUCTS_THRESHOLD = 3

# Wait time for page load (milliseconds)
PAGE_LOAD_WAIT = 5000


# ============ Logging Configuration ============

# Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

# Whether to log to console
LOG_TO_CONSOLE = True

# Whether to log to file (uses SAVE_LOGS setting)
LOG_TO_FILE = SAVE_LOGS


# ============ Optional: Supabase Configuration ============
# Uncomment and configure if you want to enable database storage later

# SUPABASE_URL = "https://your-project.supabase.co"
# SUPABASE_KEY = "your-anon-key"
# SUPABASE_TABLE = "r_product_data"


# ============ Chrome Worker API Request Format ============
# Example payload structure for reference:
"""
{
  "urls": ["https://example.com/products", "https://..."],
}

Expected response format:
{
  "results": [
    {
      "url": "https://example.com/products",
      "html": "<html>...</html>",
      "success": true
    },
    {
      "url": "https://example2.com/products",
      "html": "<html>...</html>",
      "success": true,
      "error": null
    }
  ]
}

OR simple array format:
[
  {
    "url": "https://example.com/products",
    "html": "<html>...</html>",
    "success": true
  }
]
"""