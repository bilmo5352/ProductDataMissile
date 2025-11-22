"""
HTML Parser - Product Extraction System
========================================
Parses HTML content to extract structured product data using multiple strategies.

Extraction Strategies (in order):
1. DOM CSS Selectors - Comprehensive selector library
2. JSON-LD Schema.org - Structured data extraction
3. Microdata (itemscope/itemprop) - Schema.org attributes
4. Inline Data Scripts - JSON data in script tags
5. Global Heuristics - Pattern matching
6. Link + Image Fallback - Last resort extraction

Based on the original productExtraction.py but optimized for pre-rendered HTML.
"""

from bs4 import BeautifulSoup, Tag
from typing import List, Dict, Any, Optional, Tuple
import re
import json
from urllib.parse import urljoin, urlparse
from datetime import datetime
import logging


class HTMLProductParser:
    """Parses HTML content to extract product information using multiple strategies."""
    
    def __init__(self):
        """Initialize parser with selector sets and blacklist patterns."""
        self.logger = logging.getLogger(__name__)
        self._build_selector_sets()
        self._build_blacklist()
        
    def _build_selector_sets(self):
        """Build comprehensive CSS selector library for product extraction."""
        self.selector_sets = {
            # Result containers (where product lists live)
            'result_containers': [
                '[data-component-type="s-search-result"]',
                '.s-result-list',
                '.search-result-gridview-items',
                '#products-grid',
                '.products',
                '.product-list',
                '.results',
                '[class*="product-grid"]',
                '[class*="search-results"]',
                '[data-test*="product"]',
                '.catalog-grid',
                '[role="list"]',
                '.product-base',  # Myntra
                '.product-tuple',  # Snapdeal
                '.productCard',  # Generic
                '[class*="productBase"]',  # Myntra variant
                '[class*="product-tuple"]',  # Snapdeal variant
                '[class*="jm-product"]',  # JioMart
                '[class*="product-item"]',  # Generic
                '[id*="product"]',  # Generic
            ],
            
            # Product cards/items
            'product_cards': [
                '[data-component-type="s-search-result"]',
                '.product-card',
                '.product-item',
                '.product-base',  # Myntra
                '.product-tuple',  # Snapdeal
                '[class*="productBase"]',  # Myntra variant
                '[class*="product-tuple"]',  # Snapdeal variant
                '[class*="product"]',
                '[data-product-id]',
                '[data-sku]',
                'article',
                '[itemtype*="Product"]',
                '[class*="item"]',
                '[class*="listing"]',
                'li[class*="product"]',
                'div[class*="product"]',
            ],
            
            # Titles
            'titles': [
                'h2 a',
                'h3 a',
                '[class*="title"] a',
                '[class*="name"] a',
                'a[title]',
                '[itemprop="name"]',
                '[data-title]',
                '.product-title',
                '.item-title',
            ],
            
            # Prices
            'prices': [
                '[class*="price"]',
                '[itemprop="price"]',
                '[data-price]',
                'span[class*="cost"]',
                '.price-box',
                '[class*="amount"]',
            ],
            
            # Images
            'images': [
                'img[src]',
                'img[data-src]',
                'img[data-lazy-src]',
                'img[data-original]',
                'img[data-image]',
                'img[data-lazy]',
                '[class*="image"] img',
                '[class*="product-image"] img',
                '[class*="thumbnail"] img',
                'picture img',
                'picture source',
                '[class*="img"]',
            ],
            
            # Links
            'links': [
                'a[href*="/product"]',
                'a[href*="/item"]',
                'a[href*="/p/"]',
                'a[href*="/dp/"]',
                'a[href*="/c/"]',  # JioMart category/product pattern
                'a[href*="/men-"]',  # Myntra pattern
                'a[href*="/women-"]',  # Myntra pattern
                'a[href*="/kids-"]',  # Myntra pattern
                'a.product-link',
                '[itemprop="url"]',
                'a[href*="productId="]',  # Snapdeal pattern
                'a[href*="pid="]',  # Generic product ID pattern
            ],
            
            # Ratings
            'ratings': [
                '[class*="rating"]',
                '[itemprop="ratingValue"]',
                '[data-rating]',
                '.stars',
                '[aria-label*="star"]',
            ],
            
            # Review counts
            'reviews': [
                '[class*="review"]',
                '[itemprop="reviewCount"]',
                '[data-review-count]',
                '.rating-count',
            ],
        }
        
    def _build_blacklist(self):
        """Build URL blacklist patterns to avoid non-product pages."""
        self.blacklist_keywords = [
            'login', 'signin', 'register', 'cart', 'checkout',
            'account', 'help', 'support', 'contact', 'about',
            'terms', 'privacy', 'policy', 'blog', 'news',
            'category', 'brand', 'deals', 'offer', 'sale',
        ]
        
        self.product_url_patterns = [
            r'/product[/-]',
            r'/item[/-]',
            r'/p/',
            r'/dp/',
            r'/products/',
            r'[?&]pid=',
            r'[?&]id=',
            r'/men-',  # Myntra pattern (e.g., /men-tshirts)
            r'/women-',  # Myntra pattern
            r'/kids-',  # Myntra pattern
            r'productId=',  # Snapdeal pattern
            r'/product/',  # Generic product path
        ]
    
    def parse_html(self, html_content: str, source_url: str, max_items: int = 100) -> Dict[str, Any]:
        """
        Parse HTML and extract product data.
        
        Args:
            html_content: Raw HTML string
            source_url: Original URL (for link resolution)
            max_items: Maximum products to extract
            
        Returns:
            Dict with success status, products list, and metadata
        """
        start_time = datetime.now()
        
        try:
            # Check for error pages (403, 404, etc.)
            html_lower = html_content.lower()
            error_indicators = [
                '403 error', '404 error', 'access denied', 'request blocked',
                'error: the request could not be satisfied', 'cloudfront',
                'page not found', 'not found', 'forbidden'
            ]
            if any(indicator in html_lower for indicator in error_indicators) and len(html_content) < 5000:
                # Likely an error page
                return {
                    'success': False,
                    'url': source_url,
                    'platform': self._extract_platform(source_url),
                    'error': 'Error page detected (likely blocked or not found)',
                    'num_products': 0
                }
            
            soup = BeautifulSoup(html_content, 'html.parser')
            platform = self._extract_platform(source_url)
            
            # Try extraction strategies in order
            products = []
            strategy_used = None
            
            strategies = [
                ('dom_css', self._extract_from_dom),
                ('jsonld', self._extract_from_jsonld),
                ('microdata', self._extract_from_microdata),
                ('inline_scripts', self._extract_from_inline_scripts),
                ('heuristics', self._extract_by_heuristics),
                ('fallback', self._extract_from_links_with_images),
            ]
            
            best_products = []
            best_strategy = None
            
            for strategy_name, strategy_func in strategies:
                self.logger.info(f"Trying strategy: {strategy_name}")
                try:
                    found_products = strategy_func(soup, source_url, max_items)
                
                    if found_products:
                        # If this strategy finds 3+ products, use it immediately
                        if len(found_products) >= 3:
                            strategy_used = strategy_name
                            products = found_products
                            self.logger.info(f"[OK] Strategy '{strategy_name}' succeeded with {len(found_products)} products")
                            break
                        # Otherwise, keep track of the best result so far (accept any products found)
                        elif len(found_products) > len(best_products):
                            best_products = found_products
                            best_strategy = strategy_name
                            self.logger.info(f"Strategy '{strategy_name}' found {len(found_products)} products (keeping as candidate)")
                    else:
                        self.logger.info(f"Strategy '{strategy_name}' found no products")
                except (AttributeError, TypeError, ValueError) as e:
                    # Catch errors like 'int' object has no attribute 'get'
                    self.logger.warning(f"Strategy '{strategy_name}' failed with error: {type(e).__name__}: {e}")
                    continue
                except Exception as e:
                    # Catch any other unexpected errors
                    self.logger.warning(f"Strategy '{strategy_name}' failed with unexpected error: {type(e).__name__}: {e}")
                    continue
            
            # Use the best strategy found (even if < 3 products)
            if best_products and not strategy_used:
                strategy_used = best_strategy
                products = best_products
                self.logger.info(f"Using strategy '{strategy_used}' with {len(products)} products")
            elif not products:
                products = []
            
            # Deduplicate
            products = self._dedupe_by_url(products)
            
            duration = (datetime.now() - start_time).total_seconds()
            
            result = {
                'success': len(products) > 0,
                'url': source_url,
                'platform': platform,
                'timestamp': datetime.now().isoformat(),
                'num_products': len(products),
                'extraction_strategy': strategy_used or 'none',
                'products': products[:max_items],
                'duration_seconds': round(duration, 2)
            }
            
            if not products:
                result['error'] = 'No products found by any strategy'
            
            return result
            
        except (AttributeError, TypeError, ValueError) as e:
            # Catch specific errors like 'int' object has no attribute 'get'
            error_msg = f"{type(e).__name__}: {str(e)}"
            self.logger.error(f"Error parsing HTML: {error_msg}", exc_info=True)
            return {
                'success': False,
                'url': source_url,
                'platform': self._extract_platform(source_url) if source_url else 'unknown',
                'error': error_msg,
                'num_products': 0
            }
        except Exception as e:
            # Catch any other unexpected errors
            error_msg = f"{type(e).__name__}: {str(e)}"
            self.logger.error(f"Unexpected error parsing HTML: {error_msg}", exc_info=True)
            return {
                'success': False,
                'url': source_url,
                'platform': self._extract_platform(source_url) if source_url else 'unknown',
                'error': error_msg,
                'num_products': 0
            }
    
    def _extract_from_dom(self, soup: BeautifulSoup, base_url: str, max_items: int) -> List[Dict]:
        """Extract products using CSS selectors (Strategy 1)."""
        products = []
        
        # Find result containers
        containers = []
        for selector in self.selector_sets['result_containers']:
            found = soup.select(selector)
            if found:
                containers.extend(found)
                break
        
        # If no containers found, use body
        if not containers:
            containers = [soup.body] if soup.body else [soup]
        
        # Extract products from containers
        for container in containers:
            # Find product cards
            cards = []
            for selector in self.selector_sets['product_cards']:
                found = container.select(selector)
                if found and len(found) >= 1:  # Accept even single products
                    cards = found
                    break
            
            # If still no cards, try more aggressive search
            if not cards:
                # Look for any div/li with an image and a link
                potential_cards = container.find_all(['div', 'li', 'article'])
                for card in potential_cards:
                    has_img = card.find('img')
                    has_link = card.find('a', href=True)
                    # Must have both image and link, and link should not be javascript:
                    if has_img and has_link:
                        href = has_link.get('href', '')
                        if href and not href.startswith('javascript:') and href != '#':
                            cards.append(card)
                            if len(cards) >= max_items * 2:  # Get more candidates
                                break
            
            # Extract fields from each card
            for card in cards[:max_items]:
                product = self._extract_fields_from_card(card, base_url)
                if product and self._validate_product(product):
                    products.append(product)
                    if len(products) >= max_items:
                        break
            
            if products:
                break
        
        return products
    
    def _extract_fields_from_card(self, card: Tag, base_url: str) -> Optional[Dict]:
        """Extract all fields from a product card."""
        product = {}
        
        # Title
        for selector in self.selector_sets['titles']:
            elem = card.select_one(selector)
            if elem:
                product['title'] = self._clean_text(elem.get('title') or elem.get_text())
                break
        
        # Product URL
        for selector in self.selector_sets['links']:
            elem = card.select_one(selector)
            if elem and elem.get('href'):
                url = elem.get('href')
                product['product_url'] = urljoin(base_url, url)
                break
        
        # If no product URL found, try any link
        if 'product_url' not in product:
            link = card.find('a', href=True)
            if link:
                product['product_url'] = urljoin(base_url, link['href'])
        
        # Image - comprehensive extraction
        product['image_url'] = self._extract_image_from_element(card, base_url)
        
        # Price
        for selector in self.selector_sets['prices']:
            elem = card.select_one(selector)
            if elem:
                price_text = elem.get('content') or elem.get_text()
                price, currency = self._parse_price(price_text)
                if price:
                    product['price'] = price
                    product['currency'] = currency
                    product['price_raw'] = price_text.strip()
                    break
        
        # Rating
        for selector in self.selector_sets['ratings']:
            elem = card.select_one(selector)
            if elem:
                rating_text = elem.get('content') or elem.get('aria-label') or elem.get_text()
                rating = self._parse_rating(rating_text)
                if rating:
                    product['rating'] = rating
                    break
        
        # Review count
        for selector in self.selector_sets['reviews']:
            elem = card.select_one(selector)
            if elem:
                review_text = elem.get('content') or elem.get_text()
                count = self._parse_review_count(review_text)
                if count:
                    product['review_count'] = count
                    break
        
        # Brand (if available)
        brand_elem = card.select_one('[itemprop="brand"], [class*="brand"], [data-brand]')
        if brand_elem:
            product['brand'] = self._clean_text(brand_elem.get('content') or brand_elem.get_text())
        
        # SKU (if available)
        sku_elem = card.select_one('[itemprop="sku"], [data-sku], [data-product-id]')
        if sku_elem:
            product['sku'] = self._clean_text(sku_elem.get('content') or sku_elem.get_text())
        
        # In stock
        stock_elem = card.select_one('[itemprop="availability"], [class*="stock"], [data-stock]')
        if stock_elem:
            stock_text = (stock_elem.get('content') or stock_elem.get_text()).lower()
            product['in_stock'] = 'instock' in stock_text or 'available' in stock_text
        else:
            product['in_stock'] = True  # Assume in stock if not specified
        
        return product if product else None
    
    def _extract_from_jsonld(self, soup: BeautifulSoup, base_url: str, max_items: int) -> List[Dict]:
        """Extract products from JSON-LD structured data (Strategy 2)."""
        products = []
        
        scripts = soup.find_all('script', type='application/ld+json')
        
        for script in scripts:
            try:
                if not script.string:
                    continue
                data = json.loads(script.string)
                found_products = self._find_products_in_jsonld(data, base_url)
                products.extend(found_products)
                
                if len(products) >= max_items:
                    break
            except (json.JSONDecodeError, AttributeError, TypeError, ValueError, KeyError) as e:
                # Log but continue - some scripts might not be valid JSON-LD or have unexpected structure
                self.logger.debug(f"Error parsing JSON-LD: {type(e).__name__}: {e}")
                continue
            except Exception as e:
                # Catch any other unexpected errors (like 'int' object has no attribute 'get')
                self.logger.warning(f"Unexpected error in JSON-LD parsing: {type(e).__name__}: {e}")
                continue
        
        return products[:max_items]
    
    def _find_products_in_jsonld(self, data: Any, base_url: str) -> List[Dict]:
        """Recursively find Product objects in JSON-LD data."""
        products = []
        
        try:
            # Skip non-dict, non-list types (strings, ints, etc.)
            if not isinstance(data, (dict, list)):
                return products
        except Exception:
            # If isinstance fails for some reason, return empty
            return products
        
        try:
            if isinstance(data, dict):
                # Handle ItemList (e.g., Myntra uses this)
                if data.get('@type') == 'ItemList' and 'itemListElement' in data:
                    items = data.get('itemListElement', [])
                    if isinstance(items, list):
                        for item in items:
                            # Skip non-dict items (position numbers, etc.)
                            if not isinstance(item, dict):
                                continue
                            
                            # ItemList items can have 'item' property containing the product
                            item_value = item.get('item')
                            # Use item_value if it's a dict, otherwise use item itself
                            product_data = item_value if isinstance(item_value, dict) else (item if isinstance(item, dict) else None)
                            
                            if isinstance(product_data, dict):
                                # Check if it's a Product or has product-like data
                                if product_data.get('@type') == 'Product' or 'name' in product_data or 'url' in product_data:
                                    product = self._parse_jsonld_product(product_data, base_url)
                                    if product:
                                        products.append(product)
                            
                            # Also check if item itself has product data (only if we haven't already processed it)
                            if isinstance(item, dict) and product_data != item:
                                if item.get('@type') == 'Product' or ('name' in item and 'url' in item):
                                    product = self._parse_jsonld_product(item, base_url)
                                    if product:
                                        products.append(product)
                            # Recurse into the item to find nested products
                            products.extend(self._find_products_in_jsonld(item, base_url))
                
                # Handle direct Product objects
                if data.get('@type') == 'Product' or 'Product' in str(data.get('@type', '')):
                    product = self._parse_jsonld_product(data, base_url)
                    if product:
                        products.append(product)
                
                # Recurse into nested objects (skip non-dict, non-list values)
                try:
                    for value in data.values():
                        if isinstance(value, (dict, list)):
                            products.extend(self._find_products_in_jsonld(value, base_url))
                except (AttributeError, TypeError):
                    # data.values() might fail if data is not actually a dict
                    pass
            
            elif isinstance(data, list):
                try:
                    for item in data:
                        if isinstance(item, (dict, list)):
                            products.extend(self._find_products_in_jsonld(item, base_url))
                except (TypeError, AttributeError):
                    # Iteration might fail
                    pass
        except (AttributeError, TypeError, ValueError) as e:
            # Catch any unexpected errors like 'int' object has no attribute 'get'
            self.logger.debug(f"Error in _find_products_in_jsonld: {type(e).__name__}: {e}")
            return products
        
        return products
    
    def _parse_jsonld_product(self, data: Dict, base_url: str) -> Optional[Dict]:
        """Parse a single Product object from JSON-LD."""
        # Ensure data is a dict
        if not isinstance(data, dict):
            return None
        
        product = {}
        
        # Title
        product['title'] = data.get('name', '')
        
        # URL
        url = data.get('url', '')
        if url:
            product['product_url'] = urljoin(base_url, url)
        
        # Image - comprehensive extraction
        image = data.get('image', '')
        if not image:
            # Try alternative image field names
            image = data.get('imageUrl') or data.get('imageURL') or data.get('thumbnail') or data.get('thumbnailUrl')
        
        if isinstance(image, list):
            # Get first valid image from list
            for img in image:
                if isinstance(img, str) and img:
                    image = img
                    break
                elif isinstance(img, dict):
                    img_url = img.get('url') or img.get('src') or img.get('@id')
                    if img_url:
                        image = img_url
                        break
            else:
                image = image[0] if image else ''
        
        if isinstance(image, dict):
            image = image.get('url') or image.get('src') or image.get('@id') or image.get('contentUrl') or ''
        
        if not isinstance(image, str):
            image = str(image) if image else ''
        
        if image and isinstance(image, str) and self._is_valid_product_image(image):
            product['image_url'] = urljoin(base_url, image)
        
        # Price
        offers = data.get('offers', {})
        if not isinstance(offers, dict):
            if isinstance(offers, list):
                offers = offers[0] if offers and isinstance(offers[0], dict) else {}
            else:
                offers = {}
        
        if isinstance(offers, dict):
            price_value = offers.get('price') or offers.get('lowPrice')
            if price_value:
                try:
                    product['price'] = float(price_value)
                    product['currency'] = offers.get('priceCurrency', 'USD')
                except (ValueError, TypeError):
                    pass
        
        # Rating
        rating_data = data.get('aggregateRating', {})
        if isinstance(rating_data, dict):
            rating = rating_data.get('ratingValue')
            if rating:
                try:
                    product['rating'] = float(rating)
                except (ValueError, TypeError):
                    pass
            review_count = rating_data.get('reviewCount') or rating_data.get('ratingCount')
            if review_count:
                try:
                    product['review_count'] = int(review_count)
                except (ValueError, TypeError):
                    pass
        
        # Brand
        brand = data.get('brand')
        if isinstance(brand, dict):
            product['brand'] = brand.get('name', '')
        elif isinstance(brand, str):
            product['brand'] = brand
        
        # SKU
        sku = data.get('sku')
        if sku:
            product['sku'] = str(sku)
        
        # Availability
        if isinstance(offers, dict):
            availability = offers.get('availability', '')
            if isinstance(availability, str):
                product['in_stock'] = 'InStock' in availability
            else:
                product['in_stock'] = True  # Default to in stock if not specified
        else:
            product['in_stock'] = True
        
        return product if product.get('title') else None
    
    def _extract_image_from_element(self, element: Tag, base_url: str) -> str:
        """
        Comprehensive image extraction from an element.
        Checks multiple attributes, parent/sibling elements, and background images.
        """
        if not element:
            return ''
        
        # First, try direct image selectors
        for selector in self.selector_sets['images']:
            img_elem = element.select_one(selector)
            if img_elem:
                img_url = self._get_image_url_from_element(img_elem)
                if img_url:
                    return urljoin(base_url, img_url)
        
        # Try to find any img tag in the element
        img_tags = element.find_all('img', limit=5)
        for img in img_tags:
            img_url = self._get_image_url_from_element(img)
            if img_url and self._is_valid_product_image(img_url):
                return urljoin(base_url, img_url)
        
        # Check parent elements for images
        parent = element.parent
        for _ in range(3):  # Check up to 3 levels up
            if not parent:
                break
            img_tags = parent.find_all('img', limit=3)
            for img in img_tags:
                img_url = self._get_image_url_from_element(img)
                if img_url and self._is_valid_product_image(img_url):
                    return urljoin(base_url, img_url)
            parent = parent.parent
        
        # Check siblings for images
        if element.parent:
            for sibling in element.parent.find_all(['div', 'li', 'article'], limit=5):
                if sibling == element:
                    continue
                img_tags = sibling.find_all('img', limit=2)
                for img in img_tags:
                    img_url = self._get_image_url_from_element(img)
                    if img_url and self._is_valid_product_image(img_url):
                        return urljoin(base_url, img_url)
        
        # Try background image from style attribute
        style = element.get('style', '')
        if style:
            bg_match = re.search(r'background-image:\s*url\(["\']?([^"\']+)["\']?\)', style)
            if bg_match:
                img_url = bg_match.group(1)
                if self._is_valid_product_image(img_url):
                    return urljoin(base_url, img_url)
        
        return ''
    
    def _get_image_url_from_element(self, img_elem: Tag) -> str:
        """Extract image URL from an img element, checking multiple attributes."""
        if not img_elem:
            return ''
        
        # Check various image attributes in order of preference
        attrs_to_check = [
            'src',
            'data-src',
            'data-lazy-src',
            'data-original',
            'data-image',
            'data-lazy',
            'data-srcset',
            'srcset',
        ]
        
        for attr in attrs_to_check:
            value = img_elem.get(attr, '')
            if value:
                # Handle srcset (format: "url1 1x, url2 2x" or "url1 100w, url2 200w")
                if attr in ('srcset', 'data-srcset'):
                    # Extract first URL from srcset
                    urls = re.findall(r'([^\s,]+)(?:\s+\d+[wx])?', value)
                    if urls:
                        return urls[0].strip()
                else:
                    return value.strip()
        
        return ''
    
    def _is_valid_product_image(self, img_url: str) -> bool:
        """Check if an image URL looks like a valid product image (not a logo/icon)."""
        if not img_url:
            return False
        
        img_url_lower = img_url.lower()
        
        # Skip common non-product images
        skip_patterns = [
            'logo', 'icon', 'favicon', 'sprite', 'placeholder',
            'banner', 'header', 'footer', 'nav', 'menu',
            '.svg', '.ico', 'data:image', 'base64',
            'chevron', 'arrow', 'close', 'search', 'cart',
        ]
        
        for pattern in skip_patterns:
            if pattern in img_url_lower:
                return False
        
        # Prefer common product image formats
        valid_extensions = ['.jpg', '.jpeg', '.png', '.webp', '.gif']
        has_valid_extension = any(img_url_lower.endswith(ext) for ext in valid_extensions)
        
        # If it has a valid extension or contains product-related keywords, it's likely valid
        if has_valid_extension:
            return True
        
        # Check for product-related keywords
        product_keywords = ['product', 'item', 'image', 'photo', 'picture', 'thumb']
        if any(keyword in img_url_lower for keyword in product_keywords):
            return True
        
        # If URL is very short or looks like an icon path, skip it
        if len(img_url) < 20 or '/icons/' in img_url_lower or '/assets/icons' in img_url_lower:
            return False
        
        # Default: accept if it's a reasonable length URL
        return len(img_url) > 10
    
    def _extract_from_microdata(self, soup: BeautifulSoup, base_url: str, max_items: int) -> List[Dict]:
        """Extract products from microdata attributes (Strategy 3)."""
        products = []
        
        # Find elements with itemtype containing "Product"
        product_elems = soup.find_all(attrs={'itemtype': re.compile(r'Product', re.I)})
        
        for elem in product_elems[:max_items]:
            product = {}
            
            # Title
            name_elem = elem.find(attrs={'itemprop': 'name'})
            if name_elem:
                product['title'] = self._clean_text(name_elem.get('content') or name_elem.get_text())
            
            # URL
            url_elem = elem.find(attrs={'itemprop': 'url'})
            if url_elem:
                product['product_url'] = urljoin(base_url, url_elem.get('href') or url_elem.get('content'))
            
            # Image
            img_elem = elem.find(attrs={'itemprop': 'image'})
            if img_elem:
                img_url = self._get_image_url_from_element(img_elem) or img_elem.get('content') or img_elem.get('href')
                if img_url and self._is_valid_product_image(img_url):
                    product['image_url'] = urljoin(base_url, img_url)
            
            # If no image found via itemprop, try comprehensive search
            if 'image_url' not in product or not product['image_url']:
                product['image_url'] = self._extract_image_from_element(elem, base_url)
            
            # Price
            price_elem = elem.find(attrs={'itemprop': 'price'})
            if price_elem:
                price_text = price_elem.get('content') or price_elem.get_text()
                price, currency = self._parse_price(price_text)
                if price:
                    product['price'] = price
                    product['currency'] = currency
            
            if self._validate_product(product):
                products.append(product)
        
        return products
    
    def _extract_from_inline_scripts(self, soup: BeautifulSoup, base_url: str, max_items: int) -> List[Dict]:
        """Extract products from inline JSON scripts (Strategy 4)."""
        products = []
        
        # Find script tags with JSON data (but not ld+json)
        scripts = soup.find_all('script', type=['application/json', 'text/javascript'])
        # Also check scripts without type (might contain JSON)
        scripts.extend([s for s in soup.find_all('script') if s not in scripts and s.string])
        
        for script in scripts:
            if not script.string:
                continue
            
            try:
                # Try to parse entire script as JSON (for window.__INITIAL_STATE__ etc.)
                try:
                    data = json.loads(script.string)
                    # Only process if data is a dict or list
                    if isinstance(data, (dict, list)):
                        # Look for product arrays or objects
                        found = self._find_products_in_jsonld(data, base_url)
                        products.extend(found)
                        if len(products) >= max_items:
                            return products[:max_items]
                except json.JSONDecodeError:
                    pass
                except (AttributeError, TypeError, ValueError) as e:
                    # Skip if data structure is unexpected
                    self.logger.debug(f"Error processing inline script JSON: {e}")
                    pass
                
                # Try to find JSON objects in script content with various patterns
                script_content = script.string
                
                # Look for common JavaScript variable patterns - use more aggressive matching
                js_patterns = [
                    # React/Next.js state patterns (non-greedy to avoid matching too much)
                    (r'window\.__INITIAL_STATE__\s*=\s*(\{.*?"products".*?\})', re.DOTALL),
                    (r'window\.__PRELOADED_STATE__\s*=\s*(\{.*?"products".*?\})', re.DOTALL),
                    (r'__NEXT_DATA__\s*=\s*(\{.*?"products".*?\})', re.DOTALL),
                    # Product arrays - be more aggressive
                    (r'"products"\s*:\s*(\[[^\]]+\])', re.DOTALL),
                    (r'"items"\s*:\s*(\[[^\]]+\])', re.DOTALL),
                    (r'"data"\s*:\s*(\[[^\]]+\])', re.DOTALL),
                    (r'"results"\s*:\s*(\[[^\]]+\])', re.DOTALL),
                    # Meesho and other platforms might use different keys
                    (r'"catalog"\s*:\s*(\[[^\]]+\])', re.DOTALL),
                    (r'"list"\s*:\s*(\[[^\]]+\])', re.DOTALL),
                ]
                
                for pattern_info in js_patterns:
                    if isinstance(pattern_info, tuple):
                        pattern, flags = pattern_info
                    else:
                        pattern = pattern_info
                        flags = re.I | re.DOTALL
                    
                    matches = re.findall(pattern, script_content, flags)
                    for match in matches:
                        try:
                            data = json.loads(match)
                            if isinstance(data, (dict, list)):
                                found = self._find_products_in_jsonld(data, base_url)
                                products.extend(found)
                                if len(products) >= max_items:
                                    return products[:max_items]
                        except (json.JSONDecodeError, TypeError, ValueError):
                            continue
                    
                    # Also try to find the pattern and extract larger context
                    if '"products"' in pattern or '"items"' in pattern or '"data"' in pattern:
                        # Try to extract larger JSON structures
                        try:
                            # Find the key and extract the array
                            key_match = re.search(r'("products"|"items"|"data"|"results"|"catalog"|"list")\s*:\s*(\[)', script_content, re.I)
                            if key_match:
                                # Try to extract balanced brackets
                                start_pos = key_match.end()
                                bracket_count = 1
                                end_pos = start_pos
                                for i, char in enumerate(script_content[start_pos:], start_pos):
                                    if char == '[':
                                        bracket_count += 1
                                    elif char == ']':
                                        bracket_count -= 1
                                        if bracket_count == 0:
                                            end_pos = i + 1
                                            break
                                
                                if end_pos > start_pos:
                                    array_str = script_content[start_pos:end_pos]
                                    try:
                                        data = json.loads(array_str)
                                        if isinstance(data, list) and len(data) > 0:
                                            # Check if items look like products
                                            for item in data[:max_items]:
                                                if isinstance(item, dict):
                                                    product = self._extract_product_from_dict(item, base_url)
                                                    if product and self._validate_product(product):
                                                        products.append(product)
                                                        if len(products) >= max_items:
                                                            return products[:max_items]
                                    except (json.JSONDecodeError, TypeError, ValueError):
                                        pass
                        except Exception:
                            pass
                
                # Also try simple JSON object patterns
                simple_patterns = [
                    r'\{[^{}]*"product"[^{}]*\}',
                    r'\{[^{}]*"name"[^{}]*"url"[^{}]*\}',
                    r'\{[^{}]*"title"[^{}]*"link"[^{}]*\}',
                ]
                
                for pattern in simple_patterns:
                    json_matches = re.findall(pattern, script_content, re.I | re.DOTALL)
                    for match in json_matches:
                        try:
                            data = json.loads(match)
                            if isinstance(data, dict):
                                product = self._extract_product_from_dict(data, base_url)
                                if product and self._validate_product(product):
                                    products.append(product)
                                    if len(products) >= max_items:
                                        return products[:max_items]
                        except (json.JSONDecodeError, TypeError):
                            continue
            except Exception:
                continue
        
        return products[:max_items]
    
    def _extract_product_from_dict(self, data: Dict, base_url: str) -> Optional[Dict]:
        """Extract product info from a generic dictionary."""
        # Ensure data is a dict
        if not isinstance(data, dict):
            return None
        
        product = {}
        
        # Common field mappings
        title_keys = ['name', 'title', 'productName', 'product_name']
        url_keys = ['url', 'link', 'productUrl', 'product_url']
        image_keys = ['image', 'imageUrl', 'img', 'thumbnail']
        price_keys = ['price', 'cost', 'amount']
        
        try:
            for key in title_keys:
                if key in data:
                    product['title'] = str(data[key])
                    break
            
            for key in url_keys:
                if key in data:
                    product['product_url'] = urljoin(base_url, str(data[key]))
                    break
            
            for key in image_keys:
                if key in data:
                    img_value = data[key]
                    if isinstance(img_value, list) and img_value:
                        img_value = img_value[0]
                    if isinstance(img_value, dict):
                        img_value = img_value.get('url') or img_value.get('src') or ''
                    img_url = str(img_value) if img_value else ''
                    if img_url and self._is_valid_product_image(img_url):
                        product['image_url'] = urljoin(base_url, img_url)
                    break
            
            for key in price_keys:
                if key in data:
                    price, currency = self._parse_price(str(data[key]))
                    if price:
                        product['price'] = price
                        product['currency'] = currency
                    break
        except (TypeError, AttributeError, KeyError) as e:
            # If any error occurs, return what we have so far
            self.logger.debug(f"Error extracting from dict: {e}")
            pass
        
        return product if product else None
    
    def _extract_by_heuristics(self, soup: BeautifulSoup, base_url: str, max_items: int) -> List[Dict]:
        """Extract using heuristic pattern matching (Strategy 5)."""
        products = []
        
        # Find all links with images
        links = soup.find_all('a', href=True)
        
        for link in links[:max_items * 3]:  # Check more than needed
            # Must have an image
            img = link.find('img', src=True)
            if not img:
                continue
            
            # URL must look like a product
            url = urljoin(base_url, link['href'])
            if not self._is_product_url(url):
                continue
            
            # Find price nearby (in same parent or ancestor)
            price_elem = None
            current = link
            for _ in range(3):  # Check up to 3 levels up
                if current:
                    price_elem = current.find(class_=re.compile(r'price', re.I))
                    if price_elem:
                        break
                    current = current.parent
            
            if not price_elem:
                continue
            
            # Build product
            product = {
                'product_url': url,
                'image_url': urljoin(base_url, self._get_image_url_from_element(img) or ''),
                'title': self._clean_text(link.get('title') or img.get('alt') or link.get_text())
            }
            
            price_text = price_elem.get_text()
            price, currency = self._parse_price(price_text)
            if price:
                product['price'] = price
                product['currency'] = currency
            
            if self._validate_product(product):
                products.append(product)
                if len(products) >= max_items:
                    break
        
        return products
    
    def _extract_from_links_with_images(self, soup: BeautifulSoup, base_url: str, max_items: int) -> List[Dict]:
        """Last resort: extract any link with image (Strategy 6)."""
        products = []
        
        # First, try to find all links
        links = soup.find_all('a', href=True)
        
        # Also try to find divs/li that might contain products (even without direct links)
        # This helps with JavaScript-loaded content
        potential_products = soup.find_all(['div', 'li', 'article'], limit=max_items * 10)
        
        for elem in potential_products:
            # Look for image and some text content
            img = elem.find('img')
            if not img:
                continue
            
            # Try to find a link in this element or parent
            link = elem.find('a', href=True)
            if not link:
                # Check parent
                parent = elem.parent
                if parent:
                    link = parent.find('a', href=True)
            
            # If no link found, create a product from the element itself if it has enough info
            if not link:
                # Check if element has data attributes that might be a product ID
                product_id = (elem.get('data-product-id') or 
                             elem.get('data-id') or 
                             elem.get('id') or 
                             '')
                
                # Only create product if we have image, some text, and it looks like a product
                text = elem.get_text()
                if img and text and len(text.strip()) > 10 and len(text.strip()) < 500:
                    # Check if text doesn't look like navigation
                    text_lower = text.lower()
                    if not any(nav in text_lower for nav in ['home', 'menu', 'login', 'cart', 'search', 'account']):
                        product = {
                            'product_url': base_url,  # Use base URL as fallback
                            'image_url': urljoin(base_url, self._get_image_url_from_element(img) or ''),
                            'title': self._clean_text(text)[:200]  # Limit title length
                        }
                        
                        # Try to find price
                        price_elem = elem.find(class_=re.compile(r'price|cost|amount|rs\.?|₹', re.I))
                        if price_elem:
                            price_text = price_elem.get_text()
                            price, currency = self._parse_price(price_text)
                            if price:
                                product['price'] = price
                                product['currency'] = currency
                        
                        if self._validate_product(product):
                            products.append(product)
                            if len(products) >= max_items:
                                return products
        
        # Continue with original link-based extraction
        
        for link in links:
            # Find image in link or nearby (parent, sibling, ancestor)
            img = link.find('img')
            if not img:
                # Check parent for image
                if link.parent:
                    img = link.parent.find('img')
                # Check next sibling
                if not img and link.next_sibling:
                    if hasattr(link.next_sibling, 'find'):
                        img = link.next_sibling.find('img')
                # Check ancestor (up to 5 levels)
                if not img:
                    current = link.parent
                    for _ in range(5):
                        if current and hasattr(current, 'find'):
                            img = current.find('img')
                            if img:
                                break
                            current = current.parent if hasattr(current, 'parent') else None
                        else:
                            break
            
            if not img:
                continue
            
            url = urljoin(base_url, link['href'])
            
            # Skip invalid URLs
            if not url or url.startswith('javascript:') or url == '#' or url == 'javascript:void(0)':
                continue
            
            # Skip if blacklisted
            if self._is_blacklisted(url):
                continue
            
            # Be very lenient - accept if URL contains domain and looks reasonable
            parsed_base = urlparse(base_url)
            parsed_link = urlparse(url)
            
            # Accept if same domain and not obviously a non-product page
            is_same_domain = parsed_base.netloc == parsed_link.netloc
            skip_paths = ['/login', '/cart', '/checkout', '/account', '/help', '/contact', '/about', '/terms', '/privacy']
            
            is_product_like = self._is_product_url(url) or (
                is_same_domain and 
                parsed_link.path and 
                len(parsed_link.path) > 3 and  # Not just "/"
                not any(skip in parsed_link.path.lower() for skip in skip_paths) and
                not parsed_link.path.lower().endswith(('.jpg', '.png', '.gif', '.css', '.js', '.svg', '.ico'))  # Not media files
            )
            
            # Also accept relative URLs that look like product pages
            if not is_product_like and not url.startswith('http'):
                is_product_like = (
                    len(parsed_link.path) > 3 and
                    not any(skip in parsed_link.path.lower() for skip in skip_paths) and
                    not parsed_link.path.lower().endswith(('.jpg', '.png', '.gif', '.css', '.js', '.svg', '.ico'))
                )
            
            if not is_product_like:
                continue
            
            # Get title from various sources
            title = (link.get('title') or 
                    img.get('alt') or 
                    link.get_text() or
                    (link.parent and link.parent.get_text()) or
                    '')
            title = self._clean_text(title)
            
            # Skip if title is too generic or empty
            generic_titles = ['click here', 'more', 'view', 'link', 'image', 'logo', 'home', 'menu', 'search', 
                            'cart', 'account', 'login', 'sign in', 'sign up', 'jiomart.com', 'jiomart']
            if not title or len(title) < 3 or title.lower() in generic_titles or any(gt in title.lower() for gt in generic_titles):
                continue
            
            # Skip if image URL looks like a logo
            img_src = img.get('src') or img.get('data-src') or img.get('data-lazy-src') or ''
            if 'logo' in img_src.lower() or 'brand' in img_src.lower() or 'icon' in img_src.lower():
                continue
            
            product = {
                'product_url': url,
                'image_url': urljoin(base_url, self._get_image_url_from_element(img) or ''),
                'title': title
            }
            
            # Try to find price nearby
            price_elem = None
            current = link
            for _ in range(5):  # Check up to 5 levels up
                if current and hasattr(current, 'find'):
                    price_elem = current.find(class_=re.compile(r'price|cost|amount|rs\.?|₹', re.I))
                    if price_elem:
                        break
                    current = current.parent if hasattr(current, 'parent') else None
                else:
                    break
            
            if price_elem:
                price_text = price_elem.get_text()
                price, currency = self._parse_price(price_text)
                if price:
                    product['price'] = price
                    product['currency'] = currency
            
            if self._validate_product(product):
                products.append(product)
                if len(products) >= max_items:
                    break
        
        return products
    
    # ============ Utility Methods ============
    
    def _validate_product(self, product: Dict) -> bool:
        """Check if product has minimum required fields."""
        # Must have either (title + URL) or (price + title)
        has_title = bool(product.get('title', '').strip())
        has_url = bool(product.get('product_url', '').strip())
        has_price = product.get('price') is not None
        
        if not has_title:
            return False
        
        if not (has_url or has_price):
            return False
        
        # URL must not be blacklisted
        if has_url and self._is_blacklisted(product['product_url']):
            return False
        
        # Title must not be generic navigation text
        title_lower = product['title'].lower()
        if title_lower in ['home', 'about', 'contact', 'cart', 'login', 'search']:
            return False
        
        return True
    
    def _is_product_url(self, url: str) -> bool:
        """Check if URL looks like a product page."""
        for pattern in self.product_url_patterns:
            if re.search(pattern, url, re.I):
                return True
        return False
    
    def _is_blacklisted(self, url: str) -> bool:
        """Check if URL is blacklisted."""
        url_lower = url.lower()
        return any(keyword in url_lower for keyword in self.blacklist_keywords)
    
    def _dedupe_by_url(self, products: List[Dict]) -> List[Dict]:
        """Remove duplicate products by URL."""
        seen = {}
        for product in products:
            url = product.get('product_url', '')
            if not url:
                continue
            
            if url not in seen:
                seen[url] = product
            else:
                # Merge fields (keep first, fill missing)
                for key, value in product.items():
                    if key not in seen[url] and value:
                        seen[url][key] = value
        
        return list(seen.values())
    
    def _parse_price(self, text: str) -> Tuple[Optional[float], str]:
        """Extract price and currency from text."""
        if not text:
            return None, 'USD'
        
        # Detect currency
        currency = 'USD'
        if '₹' in text or 'INR' in text or 'Rs' in text:
            currency = 'INR'
        elif '€' in text or 'EUR' in text:
            currency = 'EUR'
        elif '£' in text or 'GBP' in text:
            currency = 'GBP'
        elif '$' in text:
            currency = 'USD'
        
        # Extract numeric value
        numbers = re.findall(r'[\d,]+\.?\d*', text)
        if numbers:
            price_str = numbers[0].replace(',', '')
            try:
                return float(price_str), currency
            except ValueError:
                pass
        
        return None, currency
    
    def _parse_rating(self, text: str) -> Optional[float]:
        """Extract rating value from text."""
        if not text:
            return None
        
        # Find first decimal number
        match = re.search(r'(\d+\.?\d*)\s*(?:out of|\/|\|)?', text)
        if match:
            try:
                return float(match.group(1))
            except ValueError:
                pass
        
        return None
    
    def _parse_review_count(self, text: str) -> Optional[int]:
        """Extract review count from text."""
        if not text:
            return None
        
        # Remove commas and find numbers
        text = text.replace(',', '')
        numbers = re.findall(r'\d+', text)
        
        if numbers:
            try:
                return int(numbers[0])
            except ValueError:
                pass
        
        return None
    
    def _clean_text(self, text: str) -> str:
        """Normalize whitespace and trim text."""
        if not text:
            return ''
        return ' '.join(text.split()).strip()
    
    def _extract_platform(self, url: str) -> str:
        """Extract platform name from URL."""
        parsed = urlparse(url)
        domain = parsed.netloc.replace('www.', '')
        return domain.split('.')[0] if domain else 'unknown'