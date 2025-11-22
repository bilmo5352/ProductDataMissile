# Product Extraction API

Flask API server for extracting product data from HTML content.

## Installation

```bash
pip install -r requirements.txt
```

## Running the Server

```bash
python api_server.py
```

The server will start on `http://0.0.0.0:5000` by default.

## API Endpoints

### 1. Health Check

**GET** `/health`

Returns server health status.

**Response:**
```json
{
  "status": "healthy",
  "service": "Product Extraction API",
  "timestamp": "2025-11-23T02:10:00.000000"
}
```

### 2. Extract Products (Single HTML)

**POST** `/extract`

Extract products from a single HTML content.

**Request Body:**
```json
{
  "html": "<html>...</html>",
  "url": "https://example.com/products",
  "product_type_id": 123
}
```

**Parameters:**
- `html` (required): The HTML content of the page
- `url` (required): The source URL of the HTML
- `product_type_id` (optional): Product type ID for database storage. If provided and Supabase is configured, products will be saved to the database.

**Response:**
```json
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
          "product_url": "https://example.com/product/123",
          "cost": 99.99,
          "currency": "USD",
          "image_url": "https://example.com/image.jpg"
        }
      ],
      "extraction_strategy": "dom_css"
    }
  ],
  "total_processed": 1,
  "total_products": 10,
  "total_saved_to_db": 10,
  "processing_time_seconds": 0.5
}
```

### 3. Extract Products (Batch)

**POST** `/extract`

Extract products from multiple HTML contents in parallel.

**Request Body:**
```json
{
  "html_contents": [
    {
      "html": "<html>...</html>",
      "url": "https://example.com/products",
      "product_type_id": 123
    },
    {
      "html": "<html>...</html>",
      "url": "https://another-site.com/items",
      "product_type_id": 124
    }
  ],
  "max_workers": 4
}
```

**Parameters:**
- `html_contents` (required): Array of objects with `html` and `url` fields
- `product_type_id` (optional): Product type ID for each HTML content. If provided and Supabase is configured, products will be saved to the database.
- `max_workers` (optional): Number of parallel workers (default: 4, max: 20)

**Response:**
```json
{
  "success": true,
  "results": [
    {
      "platform_url": "https://example.com/products",
      "success": true,
      "num_products": 10,
      "products": [...]
    },
    {
      "platform_url": "https://another-site.com/items",
      "success": true,
      "num_products": 5,
      "products": [...]
    }
  ],
  "total_processed": 2,
  "total_products": 15,
  "total_saved_to_db": 15,
  "processing_time_seconds": 1.2,
  "max_workers_used": 4
}
```

### 4. Configuration

**GET** `/config`

Get current configuration.

**Response:**
```json
{
  "max_workers": 4,
  "max_products_per_page": 100
}
```

**POST** `/config`

Update configuration.

**Request Body:**
```json
{
  "max_workers": 8,
  "max_products_per_page": 200
}
```

**Response:**
```json
{
  "success": true,
  "message": "Configuration updated",
  "config": {
    "max_workers": 8,
    "max_products_per_page": 200
  }
}
```

## Configuration File

Edit `api_config.py` to change default settings:

```python
MAX_WORKERS = 4  # Default parallel workers
MAX_PRODUCTS_PER_PAGE = 100  # Max products per HTML
FLASK_HOST = '0.0.0.0'
FLASK_PORT = 5000
FLASK_DEBUG = False
MAX_BATCH_SIZE = 50  # Max HTML contents per batch
```

## Example Usage

### Python

```python
import requests

# Single HTML
response = requests.post('http://localhost:5000/extract', json={
    'html': '<html>...</html>',
    'url': 'https://example.com/products',
    'product_type_id': 123  # Optional: for database storage
})
print(response.json())

# Batch processing
response = requests.post('http://localhost:5000/extract', json={
    'html_contents': [
        {'html': '<html>...</html>', 'url': 'https://example.com/products', 'product_type_id': 123},
        {'html': '<html>...</html>', 'url': 'https://another.com/items', 'product_type_id': 124}
    ],
    'max_workers': 4
})
print(response.json())
```

### cURL

```bash
# Single HTML
curl -X POST http://localhost:5000/extract \
  -H "Content-Type: application/json" \
  -d '{
    "html": "<html>...</html>",
    "url": "https://example.com/products",
    "product_type_id": 123
  }'

# Batch
curl -X POST http://localhost:5000/extract \
  -H "Content-Type: application/json" \
  -d '{
    "html_contents": [
      {"html": "<html>...</html>", "url": "https://example.com/products", "product_type_id": 123}
    ],
    "max_workers": 4
  }'
```

## Error Handling

All endpoints return appropriate HTTP status codes:
- `200`: Success
- `400`: Bad request (missing/invalid parameters)
- `500`: Server error

Error responses include an `error` field with details.

## Database Storage (Supabase)

The API can automatically save extracted products to Supabase `r_product_data` table.

### Configuration

Set these environment variables:

```env
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your-supabase-anon-key
```

### Database Schema Mapping

The API maps extracted product data to the `r_product_data` table:

| API Field | Database Column | Notes |
|-----------|----------------|-------|
| `url` (input) | `platform_url` | Source URL |
| `product_name` | `product_name` | Extracted product name |
| `product_url` | `product_url` | Product page URL |
| `image_url` | `product_image_url` | Product image URL |
| `original_price` | `original_price` | Original price (text) |
| `cost` | `current_price` | Current price (numeric) |
| `product_type_id` (input) | `product_type_id` | From request |
| `rating` | `rating` | Product rating |
| `review_count` | `reviews` | Number of reviews |
| `brand` | `brand` | Product brand |
| `in_stock` | `in_stock` | "Yes" or "No" |
| - | `category_id` | Set to NULL |
| - | `searched_product_id` | Set to NULL |

### Usage

To save products to the database, include `product_type_id` in your request:

```json
{
  "html": "<html>...</html>",
  "url": "https://example.com/products",
  "product_type_id": 123
}
```

The response will include `total_saved_to_db` indicating how many products were saved.

