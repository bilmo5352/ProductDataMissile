"""
Test Product Extractor API using Existing HTML Cache
====================================================
Reads HTML files from html_cache folder and tests the Flask API
with a batch request containing all HTML contents.
"""

import requests
import json
import time
from pathlib import Path
from typing import List, Dict, Any, Optional
from urllib.parse import urlparse

# Flask API URL
API_BASE_URL = "http://localhost:5000"

# Map of domain names to common URL patterns (for reconstructing URLs)
DOMAIN_URL_MAP = {
    'flipkart': 'https://www.flipkart.com/search?q=products',
    'myntra': 'https://www.myntra.com/products',
    'meesho': 'https://www.meesho.com/search?q=products',
    'nykaa': 'https://www.nykaa.com/search/result/?q=products',
    'jiomart': 'https://www.jiomart.com/search?q=products',
    'dotandkey': 'https://www.dotandkey.com/pages/searchtap-search?q=products',
    'levi': 'https://levi.in/pages/searchtap-search?q=products',
    'aqualogica': 'https://aqualogica.in/pages/searchtap-search?q=products',
    'darlingretail': 'https://darlingretail.com/search?q=products',
}


def extract_url_from_filename(filename: str) -> str:
    """
    Extract or reconstruct URL from HTML cache filename.
    
    Filename format: domain_hash_timestamp.html
    Example: flipkart_86ba66b4_20251123_020353.html
    
    Args:
        filename: HTML cache filename
        
    Returns:
        Reconstructed URL based on domain
    """
    # Remove extension
    name_without_ext = filename.replace('.html', '')
    
    # Split by underscore
    parts = name_without_ext.split('_')
    
    if len(parts) >= 2:
        domain = parts[0]
        # Use domain to get URL pattern
        if domain in DOMAIN_URL_MAP:
            return DOMAIN_URL_MAP[domain]
        else:
            # Construct generic URL from domain
            return f"https://{domain}.com/products"
    
    # Fallback
    return "https://example.com/products"


def load_html_from_cache(cache_dir: Path, max_files: Optional[int] = None) -> List[Dict[str, str]]:
    """
    Load HTML contents from cache directory.
    
    Args:
        cache_dir: Path to html_cache directory
        max_files: Maximum number of files to load (None = all)
        
    Returns:
        List of dicts with 'html' and 'url' keys
    """
    if not cache_dir.exists():
        print(f"❌ Cache directory not found: {cache_dir}")
        return []
    
    html_files = sorted(list(cache_dir.glob("*.html")))
    
    if not html_files:
        print(f"⚠️  No HTML files found in {cache_dir}")
        return []
    
    if max_files:
        html_files = html_files[:max_files]
    
    print(f"\nFound {len(html_files)} HTML file(s) in cache")
    
    html_contents = []
    
    for i, html_file in enumerate(html_files, 1):
        try:
            with open(html_file, 'r', encoding='utf-8') as f:
                html_content = f.read()
            
            # Extract URL from filename
            url = extract_url_from_filename(html_file.name)
            
            html_len = len(html_content)
            html_contents.append({
                'html': html_content,
                'url': url
            })
            
            print(f"  [{i:2d}] ✓ {html_file.name[:50]:<50} ({html_len:,} chars) -> {url[:50]}")
            
        except Exception as e:
            print(f"  [{i:2d}] ✗ Error reading {html_file.name}: {e}")
            continue
    
    return html_contents


def get_url_from_results(html_filename: str, results_dir: Path) -> Optional[str]:
    """
    Try to find the original URL from results JSON file.
    
    Args:
        html_filename: HTML cache filename
        results_dir: Path to results directory
        
    Returns:
        URL if found, None otherwise
    """
    if not results_dir.exists():
        return None
    
    # Convert HTML filename to JSON filename
    json_filename = html_filename.replace('.html', '.json')
    json_file = results_dir / json_filename
    
    if json_file.exists():
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                result_data = json.load(f)
            
            # Try to get URL from result
            url = result_data.get('url') or result_data.get('platform_url')
            if url:
                return url
        except Exception:
            pass
    
    return None


def load_html_with_urls_from_results(cache_dir: Path, results_dir: Path, max_files: Optional[int] = None) -> List[Dict[str, str]]:
    """
    Load HTML contents and try to get original URLs from results folder.
    
    Args:
        cache_dir: Path to html_cache directory
        results_dir: Path to results directory
        max_files: Maximum number of files to load (None = all)
        
    Returns:
        List of dicts with 'html' and 'url' keys
    """
    if not cache_dir.exists():
        print(f"❌ Cache directory not found: {cache_dir}")
        return []
    
    html_files = sorted(list(cache_dir.glob("*.html")))
    
    if not html_files:
        print(f"⚠️  No HTML files found in {cache_dir}")
        return []
    
    if max_files:
        html_files = html_files[:max_files]
    
    print(f"\nFound {len(html_files)} HTML file(s) in cache")
    print("Loading HTML and extracting URLs...")
    
    html_contents = []
    urls_from_results = 0
    
    for i, html_file in enumerate(html_files, 1):
        try:
            with open(html_file, 'r', encoding='utf-8') as f:
                html_content = f.read()
            
            # Try to get URL from results first
            url = get_url_from_results(html_file.name, results_dir)
            
            if url:
                urls_from_results += 1
            else:
                # Fallback to extracting from filename
                url = extract_url_from_filename(html_file.name)
            
            html_len = len(html_content)
            html_contents.append({
                'html': html_content,
                'url': url
            })
            
            print(f"  [{i:2d}] ✓ {html_file.name[:40]:<40} ({html_len:,} chars)")
            print(f"       URL: {url[:70]}")
            
        except Exception as e:
            print(f"  [{i:2d}] ✗ Error reading {html_file.name}: {e}")
            continue
    
    if urls_from_results > 0:
        print(f"\n✓ Found original URLs for {urls_from_results}/{len(html_files)} files from results")
    else:
        print(f"\n⚠️  Using reconstructed URLs from filenames")
    
    return html_contents


def test_batch_api(html_contents: List[Dict[str, str]]) -> Dict[str, Any]:
    """
    Test Flask API with batch HTML contents.
    
    Args:
        html_contents: List of dicts with 'html' and 'url' keys
        
    Returns:
        API response
    """
    print(f"\n{'='*60}")
    print(f"Testing Flask API with {len(html_contents)} HTML Contents")
    print(f"{'='*60}")
    
    # Prepare payload
    payload = {
        "html_contents": html_contents,
        "max_workers": min(len(html_contents), 20)  # Use up to 20 workers
    }
    
    print(f"\nPayload:")
    print(f"  - HTML Contents: {len(html_contents)}")
    print(f"  - Max Workers: {payload['max_workers']}")
    print(f"  - Total HTML Size: {sum(len(h['html']) for h in html_contents):,} characters")
    
    try:
        print(f"\nSending POST request to {API_BASE_URL}/extract...")
        start_time = time.time()
        
        response = requests.post(
            f"{API_BASE_URL}/extract",
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=300  # 5 minutes timeout
        )
        
        elapsed = time.time() - start_time
        print(f"✓ Request completed in {elapsed:.2f} seconds")
        
        print(f"\nResponse Status: {response.status_code}")
        
        if response.status_code != 200:
            print(f"❌ Error Response: {response.text}")
            return None
        
        result = response.json()
        
        # Display summary
        print(f"\n{'='*60}")
        print("API Response Summary")
        print(f"{'='*60}")
        
        if result.get('success'):
            total_processed = result.get('total_processed', 0)
            total_products = result.get('total_products', 0)
            processing_time = result.get('processing_time_seconds', 0)
            max_workers_used = result.get('max_workers_used', 0)
            
            print(f"✓ Success: True")
            print(f"  Total Processed: {total_processed}")
            print(f"  Total Products Extracted: {total_products}")
            print(f"  Processing Time: {processing_time:.2f} seconds")
            print(f"  Max Workers Used: {max_workers_used}")
            
            # Show per-URL results
            results = result.get('results', [])
            print(f"\nPer-URL Results:")
            print(f"{'-'*60}")
            
            success_count = 0
            failed_count = 0
            
            for i, res in enumerate(results, 1):
                url = res.get('platform_url', res.get('url', f'URL_{i}'))
                success = res.get('success', False)
                num_products = res.get('num_products', 0)
                strategy = res.get('extraction_strategy', 'N/A')
                
                if success:
                    success_count += 1
                    status = "✓"
                else:
                    failed_count += 1
                    status = "✗"
                    error = res.get('error', 'Unknown error')
                
                print(f"  [{i:2d}] {status} {url[:45]:<45} Products: {num_products:3d} | Strategy: {strategy}")
                if not success:
                    print(f"       Error: {error}")
            
            print(f"\n✓ Successful: {success_count}/{len(results)}")
            print(f"✗ Failed: {failed_count}/{len(results)}")
            
            # Show sample products
            if total_products > 0:
                print(f"\n{'='*60}")
                print("Sample Products (first 5):")
                print(f"{'='*60}")
                
                product_count = 0
                for res in results:
                    if res.get('success') and res.get('products'):
                        for product in res['products'][:5]:
                            product_count += 1
                            if product_count > 5:
                                break
                            name = product.get('product_name', 'N/A')[:40]
                            cost = product.get('cost', 'N/A')
                            url = product.get('product_url', 'N/A')[:50]
                            print(f"  {product_count}. {name}")
                            print(f"     Cost: ₹{cost} | URL: {url}")
                        if product_count > 5:
                            break
        else:
            error = result.get('error', 'Unknown error')
            print(f"❌ API Error: {error}")
        
        return result
        
    except requests.exceptions.ConnectionError:
        print(f"❌ Error: Could not connect to Flask API at {API_BASE_URL}")
        print("   Make sure the server is running: python api_server.py")
        return None
    except requests.exceptions.Timeout:
        print(f"❌ Error: Request timed out after 5 minutes")
        return None
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return None


def main():
    """Main test function."""
    print("\n" + "="*60)
    print("Test Product Extractor API using HTML Cache")
    print("="*60)
    
    # Paths
    cache_dir = Path("html_cache")
    results_dir = Path("results")
    
    # Step 1: Load HTML from cache
    print("\n[Step 1/2] Loading HTML from cache...")
    html_contents = load_html_with_urls_from_results(cache_dir, results_dir)
    
    if not html_contents:
        print("\n❌ No HTML contents loaded. Cannot proceed with API test.")
        return
    
    print(f"\n✓ Loaded {len(html_contents)} HTML file(s)")
    
    # Step 2: Test Flask API with batch request
    print("\n[Step 2/2] Testing Flask API with batch request...")
    result = test_batch_api(html_contents)
    
    # Save results to file
    if result:
        output_file = "test_cache_results.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        print(f"\n✓ Results saved to: {output_file}")
    
    print("\n" + "="*60)
    print("Test Completed!")
    print("="*60)


if __name__ == "__main__":
    main()

