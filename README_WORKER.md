# Product Extraction Worker

Continuous processing system that fetches URLs from Supabase, extracts product data, and saves results.

## Overview

The worker runs continuously in an infinite loop, processing URLs from the `product_page_urls` table:

1. Fetches batches of pending URLs from Supabase
2. Claims URLs to prevent duplicate processing
3. Fetches HTML content via Railway private networking
4. Extracts product data from HTML
5. Saves products to `r_product_data` table
6. Updates processing status in `product_page_urls` table

## Installation

```bash
pip install -r requirements.txt
```

## Configuration

Set the following environment variables:

### Required

```env
# Supabase Configuration
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your-supabase-anon-key

# Railway URL-to-HTML Service (Public HTTPS API)
URLTOHTML_URL=https://urltohtml-production.up.railway.app/api/v1/fetch-batch
```

### Optional

```env
# Worker Configuration
WORKER_BATCH_SIZE=100          # Number of URLs to process per batch (default: 100)
WORKER_ID=worker-1             # Unique worker identifier (default: hostname)
POLL_INTERVAL=5                # Seconds to wait between batches when no URLs found (default: 5)
MAX_RETRIES=3                  # Max retries for HTML fetching (default: 3)
RETRY_DELAY=10                 # Base delay in seconds for retries (default: 10)
```

## Running the Worker

```bash
python product_worker.py
```

The worker will:
- Start processing immediately
- Fetch batches of URLs continuously
- Log all operations to console
- Handle errors gracefully and continue processing

## Database Schema

### Input Table: `product_page_urls`

The worker reads from this table:

```sql
CREATE TABLE public.product_page_urls (
  id bigserial NOT NULL,
  product_type_id bigint NOT NULL,
  product_page_url text NOT NULL,
  created_at timestamp with time zone DEFAULT now(),
  updated_at timestamp with time zone DEFAULT now(),
  processing_status text DEFAULT 'pending',
  processed_at timestamp with time zone,
  success boolean,
  products_found integer,
  products_saved integer,
  error_message text,
  retry_count integer DEFAULT 0,
  claimed_by text,
  claimed_at timestamp with time zone,
  PRIMARY KEY (id),
  UNIQUE (product_type_id, product_page_url)
);
```

**Processing Status Values:**
- `pending`: URL is ready to be processed
- `processing`: URL is currently being processed
- `completed`: URL was successfully processed
- `failed`: URL processing failed

### Output Table: `r_product_data`

The worker saves extracted products to this table:

```sql
CREATE TABLE public.r_product_data (
  id bigserial NOT NULL,
  platform_url text NOT NULL,
  product_name text NOT NULL,
  original_price text,
  current_price numeric,
  product_url text NOT NULL,
  product_image_url text,
  description text,
  rating numeric,
  reviews integer,
  created_at timestamp with time zone DEFAULT now(),
  updated_at timestamp with time zone DEFAULT now(),
  in_stock text,
  brand text,
  category_id integer,
  searched_product_id integer,
  product_type_id bigint,
  PRIMARY KEY (id)
);
```

## How It Works

### 1. URL Fetching and Claiming

The worker fetches URLs with `processing_status = 'pending'` and immediately updates them to `processing` status with:
- `claimed_by`: Worker ID
- `claimed_at`: Current timestamp

This prevents multiple workers from processing the same URL.

### 2. HTML Fetching

URLs are sent to the Railway URL-to-HTML service via public HTTPS API:
- Endpoint: `https://urltohtml-production.up.railway.app/api/v1/fetch-batch`
- Batch size: Up to 100 URLs per request
- Retry logic: Exponential backoff on failures
- Timeout: 1 hour for large batches

### 3. Product Extraction

HTML content is parsed using the existing `HTMLProductParser` which:
- Uses multiple extraction strategies (DOM, JSON-LD, Microdata, etc.)
- Extracts: product name, URL, price, image, rating, reviews, brand, etc.
- Returns structured product data

### 4. Database Storage

Extracted products are saved to `r_product_data` with:
- `platform_url`: Original page URL
- `product_type_id`: From the input record
- All extracted product fields

### 5. Status Update

After processing, the `product_page_urls` record is updated with:
- `processing_status`: `completed` or `failed`
- `processed_at`: Processing timestamp
- `success`: Boolean success flag
- `products_found`: Number of products extracted
- `products_saved`: Number of products saved to database
- `error_message`: Error details if failed
- `retry_count`: Incremented on failure

## Error Handling

- **HTML Fetch Failures**: Retries with exponential backoff
- **Parsing Errors**: Logged and URL marked as failed
- **Database Errors**: Logged and processing continues
- **Network Issues**: Worker continues to next batch

## Monitoring

The worker logs:
- Batch processing start/completion
- Number of URLs processed
- Products found and saved
- Errors and warnings
- Empty batch notifications

## Running Multiple Workers

You can run multiple worker instances by:
1. Setting different `WORKER_ID` values
2. Each worker will claim different URLs
3. No coordination needed - Supabase handles concurrency

## Stopping the Worker

Press `Ctrl+C` to gracefully stop the worker. It will finish processing the current batch before stopping.

## Troubleshooting

### No URLs Being Processed

- Check that URLs exist with `processing_status = 'pending'`
- Verify Supabase connection and credentials
- Check worker logs for errors

### HTML Fetch Failures

- Verify the public HTTPS API endpoint is accessible
- Check `URLTOHTML_URL` is correct
- Verify the URL-to-HTML service is running and publicly accessible

### Database Errors

- Verify Supabase credentials
- Check table permissions
- Verify schema matches expected structure

## Example Log Output

```
2025-11-23 10:00:00 - INFO - Starting product extraction worker (ID: worker-1)
2025-11-23 10:00:00 - INFO - Batch size: 100, Poll interval: 5s
2025-11-23 10:00:00 - INFO - URL-to-HTML service: http://urltohtml.railway.internal:8000/api/v1/fetch-batch
2025-11-23 10:00:01 - INFO - Fetched 100 pending URLs
2025-11-23 10:00:01 - INFO - Claimed 100 URLs for processing
2025-11-23 10:00:01 - INFO - Processing batch of 100 URLs
2025-11-23 10:00:01 - INFO - Fetching HTML for 100 URLs from Railway service
2025-11-23 10:00:15 - INFO - Successfully fetched HTML for 100 URLs
2025-11-23 10:00:20 - INFO - Processed https://example.com/products: 25 products found, 25 saved
2025-11-23 10:00:25 - INFO - Successfully saved 25/25 products to Supabase
...
```

