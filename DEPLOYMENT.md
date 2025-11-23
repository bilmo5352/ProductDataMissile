# Railway Deployment Guide

This guide explains how to deploy the Product Extraction Worker to Railway.

## Prerequisites

1. Railway account (sign up at https://railway.app)
2. Railway CLI installed (optional, for local testing)
3. Git repository with your code

## Step 1: Prepare Your Repository

Make sure your repository contains:
- `product_worker.py` - Main worker script
- `html_parser.py` - HTML parser module
- `requirements.txt` - Python dependencies
- `Dockerfile` - Docker configuration
- `.gitignore` - Git ignore file

## Step 2: Create a New Railway Project

### Option A: Using Railway Dashboard

1. Go to https://railway.app/dashboard
2. Click **"New Project"**
3. Select **"Deploy from GitHub repo"** (if your code is on GitHub)
   - OR select **"Empty Project"** and connect later

### Option B: Using Railway CLI

```bash
# Install Railway CLI (if not installed)
npm i -g @railway/cli

# Login to Railway
railway login

# Initialize project
railway init

# Link to existing project (if you have one)
railway link
```

## Step 3: Configure Environment Variables

In Railway dashboard, go to your service → **Variables** tab and add:

### Required Variables

```
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your-supabase-anon-key
URLTOHTML_PRIVATE_URL=http://urltohtml.railway.internal:8000/api/v1/fetch-batch
```

### Optional Variables

```
WORKER_BATCH_SIZE=100
WORKER_ID=worker-1
POLL_INTERVAL=5
MAX_RETRIES=3
RETRY_DELAY=10
```

**Important Notes:**
- `URLTOHTML_PRIVATE_URL` should use Railway's private networking format: `http://service-name.railway.internal:port/path`
- Replace `urltohtml` with your actual Railway service name for the URL-to-HTML service
- Make sure both services are in the same Railway project for private networking to work

## Step 4: Deploy

### Option A: Deploy from GitHub (Recommended)

1. In Railway dashboard, click **"New"** → **"GitHub Repo"**
2. Select your repository
3. Railway will automatically detect the `Dockerfile` and start building
4. The service will deploy automatically on every push to your main branch

### Option B: Deploy via Railway CLI

```bash
# Deploy current directory
railway up

# Or deploy specific service
railway up --service worker
```

### Option C: Deploy via Git Push

```bash
# Add Railway as remote (Railway will provide this URL)
git remote add railway <railway-git-url>

# Push to deploy
git push railway main
```

## Step 5: Verify Deployment

1. Go to your service in Railway dashboard
2. Click on **"Deployments"** tab to see build logs
3. Click on **"Logs"** tab to see worker output
4. You should see logs like:
   ```
   Starting product extraction worker (ID: worker-1)
   Batch size: 100, Poll interval: 5s
   Fetched 100 pending URLs
   ```

## Step 6: Configure Private Networking

For the worker to communicate with the URL-to-HTML service via private networking:

1. **Both services must be in the same Railway project**
2. In Railway dashboard, go to your URL-to-HTML service
3. Note the service name (e.g., `urltohtml`)
4. Use the private networking URL format:
   ```
   http://urltohtml.railway.internal:8000/api/v1/fetch-batch
   ```
5. Replace `urltohtml` with your actual service name
6. Replace `8000` with your actual port if different

## Step 7: Monitor the Worker

### View Logs

1. In Railway dashboard, go to your service
2. Click **"Logs"** tab
3. You'll see real-time logs of the worker processing URLs

### Check Metrics

1. Go to **"Metrics"** tab
2. Monitor CPU, Memory, and Network usage
3. Set up alerts if needed

## Troubleshooting

### Worker Not Starting

- Check environment variables are set correctly
- Verify `SUPABASE_URL` and `SUPABASE_KEY` are valid
- Check logs for error messages

### Cannot Connect to URL-to-HTML Service

- Verify both services are in the same Railway project
- Check service name in `URLTOHTML_PRIVATE_URL` matches actual service name
- Verify port number is correct
- Check URL-to-HTML service is running and healthy

### No URLs Being Processed

- Verify URLs exist in Supabase with `processing_status = 'pending'`
- Check Supabase connection credentials
- Review worker logs for database errors

### High Memory/CPU Usage

- Reduce `WORKER_BATCH_SIZE` (e.g., from 100 to 50)
- Increase `POLL_INTERVAL` to process less frequently
- Consider upgrading Railway plan if needed

## Scaling

To run multiple workers:

1. In Railway dashboard, go to your service
2. Click **"Settings"**
3. Increase **"Instances"** count
4. Each instance will have a unique `WORKER_ID` (or set it manually)

**Note:** Multiple workers will automatically coordinate via Supabase URL claiming mechanism.

## Updating the Worker

### Automatic Updates (GitHub Integration)

If you've connected GitHub:
1. Push changes to your main branch
2. Railway will automatically rebuild and redeploy

### Manual Updates

```bash
# Via Railway CLI
railway up

# Or via Git
git push railway main
```

## Cost Optimization

- Use Railway's free tier for testing
- Monitor resource usage in Metrics tab
- Adjust `WORKER_BATCH_SIZE` and `POLL_INTERVAL` based on your needs
- Consider using Railway's sleep mode for development

## Support

For Railway-specific issues:
- Railway Docs: https://docs.railway.app
- Railway Discord: https://discord.gg/railway

For worker-specific issues:
- Check `README_WORKER.md` for detailed documentation
- Review worker logs in Railway dashboard

