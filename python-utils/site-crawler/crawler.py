import argparse
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import nltk
from collections import Counter
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import pandas as pd
import re
import numpy as np
from urllib.parse import urljoin, urlparse, urlunparse


# Download NLTK assets
nltk.download('punkt')
nltk.download('stopwords')

parser = argparse.ArgumentParser(description="Crawl a website and extract links, images, and DOM metadata.")
parser.add_argument('--url', required=True, help='The starting URL to crawl')
args = parser.parse_args()

start_url = args.url
visited = set()
queue = [start_url]
output_data = []

def should_crawl(url):
    url = url.lower()
    path = urlparse(url).path

    # 1. Block media/video and static assets
    if url.endswith(('.png', '.jpg', '.jpeg', '.svg', '.gif', '.webp', '.pdf', '.mp4', '.mov', '.avi', '.wmv', '.flv')):
        return False

    # 2. Language translation paths
    if re.search(r"/(en|es|fr|de|pt|it|zh|ja|ko|ru|pl)(/|$)", url):
        return False

    # 3. Auth, login, checkout, etc.
    if any(kw in url for kw in ['login', 'auth', 'checkout', 'cart', 'track', 'register', 'signup']):
        return False

    # 4. Legal/boilerplate pages
    if any(kw in url for kw in ['privacy', 'terms', 'cookies', 'gdpr']):
        return False

    # 5. Depth > 3
    path = urlparse(url).path
    segments = [seg for seg in path.split('/') if seg]
    if len(segments) > 3:
        return False

    # 6. Blog-style date pattern
    if re.search(r"/\d{4}/\d{2}/\d{2}/", url):
        return False

    # 7. Anchors or mail links
    if any(p in url for p in ['#', 'mailto:', 'tel:']):
        return False

    # 8. Known city/store patterns
    if re.match(r"^/(city|location|store|office)/[a-z\-]+", path):
        return False

    # 9. Tag/category/author paths
    if re.search(r'/category/|/tag/|/author/', url, re.IGNORECASE):
        return False

    return True

def get_position_hint(element):
    try:
        parent = element
        for _ in range(5):
            parent = parent.evaluate_handle("el => el.parentElement")
            # Check if parent exists
            is_null = parent.evaluate("el => el === null")
            if is_null:
                break

            tag_name = parent.evaluate("el => el.tagName?.toLowerCase()")
            if tag_name in ["header", "footer", "nav", "main", "aside"]:
                return tag_name
    except Exception as e:
        print(f"Position hint error: {e}")
    return "unknown"

def extract_all_links(page, base_url):
    links_info = []
    base_domain = urlparse(base_url).netloc

    a_elements = page.query_selector_all('a[href]')
    for a in a_elements:
        href = a.get_attribute('href')
        if not href:
            continue

        full_url = urljoin(base_url, href)
        parsed_url = urlparse(full_url)

        # Normalize both domains by stripping "www."
        parsed_base = base_domain.replace("www.", "").lower()
        parsed_target = parsed_url.netloc.replace("www.", "").lower()

        # Determine link type
        if parsed_target == parsed_base:
            link_type = 'Internal Link'
        else:
            link_type = 'External Link'

        text = a.inner_text() or ''
        text = text.strip()

        links_info.append({
            'link_type': link_type,
            'text': text,
            'url': full_url,
            'visible': a.is_visible(),
            'position_hint': get_position_hint(a),
            'element_id': a.get_attribute('id') or '',
            'class_attr': a.get_attribute('class') or '',
            'extra': ''
        })

    return links_info

def extract_buttons(page):
    buttons_info = []

    for button in page.query_selector_all('button'):
        try:
            buttons_info.append({
                'link_type': 'CTA Button',
                'text': button.inner_text().strip(),
                'url': '',
                'visible': button.is_visible(),
                'position_hint': get_position_hint(button),
                'element_id': button.get_attribute('id') or '',
                'class_attr': button.get_attribute('class') or '',
                'extra': f"onclick={button.get_attribute('onclick') or ''}"
            })
        except Exception:
            continue

    for a in page.query_selector_all('a'):
        try:
            class_attr = a.get_attribute('class') or ''
            role_attr = a.get_attribute('role') or ''
            if 'button' in class_attr.lower() or 'btn' in class_attr.lower() or 'button' in role_attr.lower():
                buttons_info.append({
                    'link_type': 'CTA Button',
                    'text': a.inner_text().strip(),
                    'url': a.get_attribute('href') or '',
                    'visible': a.is_visible(),
                    'position_hint': get_position_hint(a),
                    'element_id': a.get_attribute('id') or '',
                    'class_attr': class_attr,
                    'extra': f"role={role_attr}"
                })
        except Exception:
            continue

    return buttons_info

    
def fetch_rendered_html(url):
    """Fetch fully rendered HTML, extract clickable elements, and capture visible text."""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        try:
            page.goto(url, wait_until="networkidle", timeout=30000)
            page.wait_for_timeout(3000)  # Give time for JS-rendered content
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")  # Scroll to trigger lazy load
            page.wait_for_timeout(1000)
        except Exception as e:
            print(f"[Timeout/Error] Could not load {url}: {e}")
            browser.close()
            return "", []

        # Try clicking visible swiper bullets
        try:
            bullets = page.query_selector_all('span.swiper-pagination-bullet')
            visible_bullets = [b for b in bullets if b.is_visible()]
            for bullet in visible_bullets:
                try:
                    bullet.click(timeout=1000)
                    page.wait_for_timeout(300)
                except Exception as ce:
                    print(f"[Click Error] Bullet click failed: {ce}")
        except Exception as e:
            print(f"[Slide Error] Skipping swiper logic on {url}: {e}")

        try:
            # Fallback to using innerText if content() is not reliable
            html = page.content()
            fallback_text = page.evaluate("document.body.innerText")
            internal_external_links = extract_all_links(page, url)
            button_links = extract_buttons(page)
            all_links_info = internal_external_links + button_links
        except Exception as e:
            print(f"[HTML Fetch Error] {url}: {e}")
            html = ""
            fallback_text = ""
            all_links_info = []

        browser.close()

        # Return fallback_text as final HTML body for parsing
        return html, fallback_text.strip(), all_links_info


def extract_visible_text(html):
    """Extract visible and meaningful text from the HTML."""
    soup = BeautifulSoup(html, 'html.parser')

    # Remove unwanted structural tags
    for tag in soup(['script', 'style', 'meta', 'noscript', 'header', 'footer', 'nav', 'aside', 'form']):
        tag.decompose()

    # Additional: Remove common cookie banner and GDPR overlays
    junk_classes = re.compile(r'cookie|consent|cmplz|preferences|gdpr|tcf', re.I)
    for junk in soup.find_all(attrs={"class": junk_classes}):
        junk.decompose()
    for junk in soup.find_all(id=junk_classes):
        junk.decompose()

    # Optional: Keep only content inside <main>, if present
    main = soup.find('main')
    if main:
        text = main.get_text(separator=' ', strip=True)
    else:
        text = soup.get_text(separator=' ', strip=True)

    # Normalize whitespace
    text = re.sub(r'\s+', ' ', text)

    return text

def extract_internal_links(page_html, base_url):
    soup = BeautifulSoup(page_html, 'html.parser')
    internal_links = set()

    base_domain = urlparse(base_url).netloc

    for link in soup.find_all('a', href=True):
        href = link['href']
        full_url = urljoin(base_url, href)
        parsed_url = urlparse(full_url)

        # Keep only links within the same domain
        if parsed_url.netloc == base_domain:
            internal_links.add(full_url)

    return list(internal_links)

def extract_page_metadata(soup):
    """Extract SEO, UX and form structure tags from BeautifulSoup soup."""
    metadata = {}

    # Meta Title
    title_tag = soup.find('title')
    metadata['meta_title'] = title_tag.get_text(strip=True) if title_tag else ''

    # Meta Description
    meta_tag = soup.find('meta', attrs={'name': 'description'})
    metadata['meta_description'] = meta_tag['content'].strip() if meta_tag and meta_tag.has_attr('content') else ''

    # Canonical URL
    canonical_link = soup.find('link', rel='canonical')
    metadata['canonical_url'] = canonical_link['href'].strip() if canonical_link and canonical_link.has_attr('href') else ''

    # Open Graph Title
    og_title = soup.find('meta', property='og:title')
    metadata['og_title'] = og_title['content'].strip() if og_title and og_title.has_attr('content') else ''

    # First H1
    h1_tag = soup.find('h1')
    metadata['h1_tag'] = h1_tag.get_text(strip=True) if h1_tag else ''

    # All headings
    headings = {}
    for tag in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
        headings[tag] = [h.get_text(strip=True) for h in soup.find_all(tag)]
    metadata['all_headings'] = headings

    # Forms
    forms_info = []
    forms = soup.find_all('form')
    for form in forms:
        form_details = {
            'form_action': form.get('action', ''),
            'form_method': form.get('method', '').lower(),
            'input_fields': []
        }
        for input_tag in form.find_all('input'):
            form_details['input_fields'].append({
                'type': input_tag.get('type', 'text'),
                'name': input_tag.get('name', '')
            })
        forms_info.append(form_details)

    metadata['forms_info'] = forms_info

    #images
    # All images with metadata
    images_info = []
    for img in soup.find_all('img'):
        img_details = {
            'src': img.get('src'),
            'alt': img.get('alt', ''),
            'title': img.get('title', ''),
            'class': img.get('class', []),
            'loading': img.get('loading', '')
        }
        images_info.append(img_details)

    metadata['images'] = images_info

    return metadata

def detect_tracking_tags(html):
    tags_found = []

    # Google Tag Manager
    gtm_matches = re.findall(r'GTM-[A-Z0-9]+', html)
    tags_found.extend(gtm_matches)

    # Google Analytics 4 (GA4)
    ga4_matches = re.findall(r'\bG-[A-Z0-9]{6,}\b', html)
    tags_found.extend(ga4_matches)

    # Universal Analytics (UA)
    ua_matches = re.findall(r'UA-\d{4,10}-\d{1,4}', html)
    tags_found.extend(ua_matches)

    # Meta Pixel
    fb_matches = re.findall(r'tr\?id=(\d+)', html)
    tags_found.extend([f'FB-{match}' for match in fb_matches])

    # LinkedIn Insight
    linkedin_matches = re.findall(r'linkedin\.com\/(insight|px)', html)
    if linkedin_matches:
        tags_found.append("LinkedIn-Insight")

    # TikTok Pixel
    tiktok_matches = re.findall(r't.tiktok.com\/t\.gif\?pid=(\d+)', html)
    tags_found.extend([f'TT-{match}' for match in tiktok_matches])

    # Hotjar
    if 'static.hotjar.com' in html:
        tags_found.append("Hotjar")

    # HubSpot
    if 'js.hs-scripts.com' in html:
        tags_found.append("HubSpot")

    return ', '.join(sorted(set(tags_found)))  # Remove duplicates and return as string

def scrape_landing_page(url):
    """Fetch the page, extract meta description, title, H1, links, and text."""
    html, fallback_text, all_links_info = fetch_rendered_html(url)
    soup = BeautifulSoup(html, 'html.parser')

    # Extract structured metadata
    metadata = extract_page_metadata(soup)

    # Detect tracking scripts from raw HTML
    tracking_summary = detect_tracking_tags(html)
    metadata['tracking_tags'] = tracking_summary

    # Extract visible text
    page_text = extract_visible_text(html)

    # Fallback: if text extraction fails or yields too little
    if not page_text or len(page_text) < 200:
        page_text = fallback_text 

    return page_text, metadata, all_links_info


def normalize_url(url):
    parsed = urlparse(url)

    # Remove www, lowercase domain
    netloc = parsed.netloc.lower().replace("www.", "")

    # Strip query, fragment, and trailing slash
    path = parsed.path.rstrip('/')
    
    # Rebuild normalized URL (https only, no params/fragments)
    return urlunparse((
        'https',  # force https
        netloc,
        path,
        '', '', ''  # clear params, query, fragment
    ))

pages_crawled = 0
while queue:
    raw_url = queue.pop(0)
    current_url = normalize_url(raw_url)

    if current_url in visited or not should_crawl(current_url):
        continue

    print(f"Crawling: {current_url}")
    page_text, meta_data, all_links_info = scrape_landing_page(current_url)

    output_data.append({
        'page': current_url,
        'meta_data': meta_data,
        'link_type': 'Main Page',
        'text': '',
        'url': current_url,
        'extra': '',
        'page_text': page_text,
        'visible': '',
        'position_hint': '',
        'element_id': '',
        'class_attr': ''
    })

    pages_crawled += 1

    for link_info in all_links_info:
        output_data.append({
            'page': current_url,
            'meta_data': '',
            'link_type': link_info.get('link_type', ''),
            'text': link_info.get('text', ''),
            'url': link_info.get('url', ''),
            'extra': link_info.get('extra', ''),
            'page_text': '',
            'visible': link_info.get('visible', ''),
            'position_hint': link_info.get('position_hint', ''),
            'element_id': link_info.get('element_id', ''),
            'class_attr': link_info.get('class_attr', '')
        })

        if link_info['link_type'] == 'Internal Link':
            normalized_link = link_info['url'].rstrip('/')
            if should_crawl(normalized_link) and normalized_link not in visited:
                queue.append(normalized_link)

    visited.add(current_url)  # Already normalized above
# Separate entries for full metadata (i.e., 'Main Page') and others
visible_rows = []
dom_rows = []

for row in output_data:
    base_row = {
        'page': row['page'],
        'link_type': row['link_type'],
        'text': row.get('text', ''),
        'url': row.get('url', ''),
        'extra': row.get('extra', ''),
        'page_text': row.get('page_text', ''),
        'visible': row.get('visible', ''),
        'position_hint': row.get('position_hint', ''),
        'element_id': row.get('element_id', ''),
        'class_attr': row.get('class_attr', '')
    }

    # For full DOM metadata (e.g., SEO tags, images, forms)
    if row.get('link_type') == 'Main Page':
        meta = row.get('meta_data', {})
        if isinstance(meta, dict):
            dom_row = {**base_row, **meta}
            dom_row['page_text'] = row.get('page_text', '')
            dom_rows.append(dom_row)

    # For visible content only
    visible_row = base_row.copy()
    visible_row['page_text'] = row.get('page_text', '')
    visible_rows.append(visible_row)

# Convert and export
df_dom = pd.DataFrame(dom_rows)
df_visible = pd.DataFrame(visible_rows)

# Save to CSVs
df_dom.to_csv("output/full_dom_h.csv", index=False)
df_visible.to_csv("output/visible_content_h.csv", index=False)

image_rows = []

for row in output_data:
    if row['link_type'] == 'Main Page' and isinstance(row['meta_data'], dict):
        page = row['page']
        images = row['meta_data'].get('images', [])
        for img in images:
            image_rows.append({
                'page': page,
                'src': img.get('src', ''),
                'alt': img.get('alt', ''),
                'title': img.get('title', ''),
                'class': ' '.join(img.get('class', [])),
                'loading': img.get('loading', '')
            })

# Convert to DataFrame and export to CSV
df_images = pd.DataFrame(image_rows)
df_images.to_csv("output/images.csv", index=False)

form_rows = []

for row in output_data:
    if row['link_type'] == 'Main Page' and isinstance(row['meta_data'], dict):
        forms = row['meta_data'].get('forms_info', [])
        for form in forms:
            form_rows.append({
                'page': row['page'],
                'form_index': form.get('form_index'),
                'form_action': form.get('form_action'),
                'form_method': form.get('form_method'),
                'input_type': form.get('input_type'),
                'input_name': form.get('input_name'),
                'crawl_date': pd.Timestamp.now()
            })

df_forms = pd.DataFrame(form_rows)
df_forms.to_csv("output/forms_flat.csv", index=False)
