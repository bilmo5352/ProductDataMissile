"""
API Configuration
================
Configuration settings for the Flask API server.
Loads from environment variables with fallback defaults.
"""

import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Maximum number of parallel workers for batch processing
# This controls how many HTML contents are processed simultaneously
# Example: If you send 20 HTML contents with max_workers=4, 
#          4 will be processed at a time, the rest wait in queue
MAX_WORKERS = int(os.getenv('MAX_WORKERS', '20'))

# Maximum products to extract per HTML page
MAX_PRODUCTS_PER_PAGE = int(os.getenv('MAX_PRODUCTS_PER_PAGE', '100'))

# Flask server configuration
FLASK_HOST = os.getenv('FLASK_HOST', '0.0.0.0')
FLASK_PORT = int(os.getenv('FLASK_PORT', '5000'))
FLASK_DEBUG = os.getenv('FLASK_DEBUG', 'False').lower() == 'true'

# API settings
API_TIMEOUT = int(os.getenv('API_TIMEOUT', '300'))  # 5 minutes timeout for batch processing

# Maximum number of HTML contents allowed in a single batch request
# This is a LIMIT on input size - how many HTML contents you can send at once
# Different from MAX_WORKERS: 
#   - MAX_BATCH_SIZE = how many HTML contents you can send in one request (input limit)
#   - MAX_WORKERS = how many HTML contents are processed in parallel (concurrency)
# Example: MAX_BATCH_SIZE=50 means you can send up to 50 HTML contents in one request.
#          If MAX_WORKERS=4, those 50 will be processed 4 at a time.
MAX_BATCH_SIZE = int(os.getenv('MAX_BATCH_SIZE', '100'))

