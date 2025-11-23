# Product Extraction Worker

Continuous processing system that extracts product data from URLs and saves to Supabase.

## Quick Start

### Local Development

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Set environment variables:
```env
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your-supabase-anon-key
URLTOHTML_PRIVATE_URL=http://urltohtml.railway.internal:8000/api/v1/fetch-batch
```

3. Run the worker:
```bash
python product_worker.py
```

### Railway Deployment

See [DEPLOYMENT.md](DEPLOYMENT.md) for detailed deployment instructions.

**Quick Steps:**
1. Push code to GitHub
2. Create new Railway project
3. Connect GitHub repository
4. Set environment variables in Railway dashboard
5. Deploy!

## Documentation

- [README_WORKER.md](README_WORKER.md) - Detailed worker documentation
- [DEPLOYMENT.md](DEPLOYMENT.md) - Railway deployment guide

## How It Works

1. Fetches batches of URLs from Supabase `product_page_urls` table
2. Claims URLs to prevent duplicate processing
3. Fetches HTML via Railway private networking
4. Extracts product data using advanced parsing strategies
5. Saves products to `r_product_data` table
6. Updates processing status in `product_page_urls` table
7. Runs continuously in an infinite loop

## Required Files

- `product_worker.py` - Main worker script
- `html_parser.py` - HTML parser module
- `requirements.txt` - Python dependencies
- `Dockerfile` - Docker configuration for Railway

## Environment Variables

### Required
- `SUPABASE_URL` - Your Supabase project URL
- `SUPABASE_KEY` - Your Supabase anon key
- `URLTOHTML_PRIVATE_URL` - Railway private networking URL for HTML fetching

### Optional
- `WORKER_BATCH_SIZE` - URLs per batch (default: 100)
- `WORKER_ID` - Unique worker identifier (default: hostname)
- `POLL_INTERVAL` - Seconds between batches when no URLs (default: 5)
- `MAX_RETRIES` - Max retries for HTML fetching (default: 3)
- `RETRY_DELAY` - Base retry delay in seconds (default: 10)

## License

MIT

