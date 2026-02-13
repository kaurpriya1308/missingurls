import streamlit as st
import json
import re
from datetime import datetime
from urllib.parse import urljoin, urlparse, urlunparse
import requests
from bs4 import BeautifulSoup
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import pandas as pd

st.set_page_config(
    page_title="Missing URL Identifier",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="collapsed"
)

st.markdown("""
    <style>
    .main { padding: 2rem; }
    .stTextArea textarea {
        font-family: 'Courier New', monospace;
        font-size: 12px;
    }
    .success-box {
        border-left: 4px solid #00c853;
        padding: 15px;
        margin: 10px 0;
        background-color: #f1f8f4;
        border-radius: 4px;
    }
    .warning-box {
        border-left: 4px solid #ff9800;
        padding: 15px;
        margin: 10px 0;
        background-color: #fff8e1;
        border-radius: 4px;
    }
    .info-box {
        border-left: 4px solid #2196f3;
        padding: 15px;
        margin: 10px 0;
        background-color: #e3f2fd;
        border-radius: 4px;
    }
    .dataframe a {
        color: #1a73e8;
        text-decoration: none;
    }
    .dataframe a:hover {
        text-decoration: underline;
    }
    table {
        font-size: 13px;
    }
    </style>
""", unsafe_allow_html=True)


# =============================================================================
# URL EXTRACTOR - Extracts real HTTP URLs from the mixed-format input
# =============================================================================
class URLExtractor:
    """
    Extracts all real HTTP/HTTPS URLs from the added URL list.
    Handles formats like:
      - Plain URLs: https://example.com/page
      - text: prefixed: text:https://example.com/page
      - Template URLs with embedded HTTP: ${...}json:xhr:https://...
      - URLs with ${...} prefixes followed by http
      - Regex patterns: ev:(...), cp:(...), df:(...), if:(...)
    """

    @staticmethod
    def extract_all_http_urls(raw_url):
        """Extract all http/https URLs from a single raw entry."""
        if not isinstance(raw_url, str):
            return []

        urls = []
        # Find all http/https URLs in the string
        # This regex captures URLs starting with http:// or https://
        http_pattern = r'(https?://[^\s\'"<>\}\)]+)'
        matches = re.findall(http_pattern, raw_url)

        for match in matches:
            # Clean trailing characters that might be JSON/template artifacts
            cleaned = match.rstrip(',;|')
            # Remove trailing curly braces that are part of templates
            cleaned = re.sub(r'\}+$', '', cleaned)
            if cleaned and len(cleaned) > 10:
                urls.append(cleaned)

        return urls

    @staticmethod
    def extract_regex_patterns(urls):
        """Extract regex patterns (ev:, cp:, df:, if:) from URL list."""
        patterns = []
        for u in urls:
            if not isinstance(u, str):
                continue
            stripped = u.strip()
            if re.match(r'^(ev|cp|df|if):', stripped, re.IGNORECASE):
                patterns.append(stripped)
        return patterns

    @staticmethod
    def get_all_plain_http_urls(urls):
        """Get all unique HTTP URLs extracted from the entire URL list."""
        all_http = []
        for u in urls:
            if not isinstance(u, str):
                continue
            extracted = URLExtractor.extract_all_http_urls(u)
            all_http.extend(extracted)
        return list(set(all_http))


# =============================================================================
# DOMAIN UTILITY
# =============================================================================
class DomainUtil:

    @staticmethod
    def get_domain_root(url):
        """Get the root (scheme + netloc) of a URL."""
        try:
            parsed = urlparse(url.strip())
            if not parsed.scheme or not parsed.netloc:
                return None
            return urlunparse((parsed.scheme, parsed.netloc, '', '', '', ''))
        except Exception:
            return None

    @staticmethod
    def get_normalized_domain(url):
        """Get normalized domain (lowercase, no www)."""
        try:
            parsed = urlparse(url.strip())
            return parsed.netloc.lower().replace('www.', '')
        except Exception:
            return None

    @staticmethod
    def extract_unique_domain_roots(urls):
        """
        From a list of URLs (raw format), extract all unique domain roots.
        Returns dict: {normalized_domain: root_url}
        """
        domain_map = {}
        # First extract all HTTP URLs from the raw entries
        all_http_urls = URLExtractor.get_all_plain_http_urls(urls)

        for url in all_http_urls:
            url = url.strip()
            if not url.startswith('http'):
                continue
            root = DomainUtil.get_domain_root(url)
            norm = DomainUtil.get_normalized_domain(url)
            if root and norm and norm not in domain_map:
                domain_map[norm] = root
        return domain_map


# =============================================================================
# CONCURRENT DOMAIN CRAWLER
# =============================================================================
class ConcurrentDomainCrawler:

    EXCLUDED_EXTENSIONS = {
        '.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx',
        '.zip', '.rar', '.tar', '.gz', '.7z',
        '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.svg', '.ico', '.webp',
        '.mp3', '.mp4', '.avi', '.mov', '.wmv', '.flv', '.wav',
        '.css', '.js', '.woff', '.woff2', '.ttf', '.eot', '.otf',
        '.exe', '.dmg', '.msi', '.apk',
    }

    EXCLUDED_PATH_PATTERNS = [
        r'/wp-content/', r'/wp-includes/', r'/wp-admin/',
        r'/assets/', r'/static/', r'/images/', r'/img/',
        r'/fonts/', r'/css/', r'/js/',
        r'javascript:', r'mailto:', r'tel:',
        r'/cdn-cgi/', r'/feed/', r'/rss/',
        r'/login', r'/logout', r'/signup', r'/register',
        r'/cart', r'/checkout', r'/account',
        r'/page/\d+', r'\?replytocom=',
        r'/xmlrpc\.php', r'/wp-json/',
    ]

    def __init__(self, max_depth=10, max_pages=1000, max_workers=50,
                 timeout=10, delay=0.1):
        self.max_depth = max_depth
        self.max_pages = max_pages
        self.max_workers = max_workers
        self.timeout = timeout
        self.delay = delay
        self._lock = threading.Lock()
        self._pages_crawled = 0

    def _make_session(self):
        s = requests.Session()
        s.headers.update({
            'User-Agent': (
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                'AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36'
            ),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        })
        s.max_redirects = 5
        return s

    def _is_valid_url(self, url, allowed_domains):
        try:
            parsed = urlparse(url)
            if not parsed.scheme or parsed.scheme not in ('http', 'https'):
                return False
            if not parsed.netloc:
                return False
            domain = parsed.netloc.lower().replace('www.', '')
            if domain not in allowed_domains:
                return False
            path_lower = parsed.path.lower()
            for ext in self.EXCLUDED_EXTENSIONS:
                if path_lower.endswith(ext):
                    return False
            url_lower = url.lower()
            for pat in self.EXCLUDED_PATH_PATTERNS:
                if re.search(pat, url_lower):
                    return False
            return True
        except Exception:
            return False

    def _normalize_url(self, url):
        try:
            parsed = urlparse(url)
            normalized = parsed._replace(fragment='')
            path = normalized.path.rstrip('/') or '/'
            normalized = normalized._replace(path=path)
            if normalized.query:
                clean = [
                    p for p in normalized.query.split('&')
                    if p.split('=')[0].lower() not in
                    ('utm_source', 'utm_medium', 'utm_campaign',
                     'utm_term', 'utm_content', 'fbclid', 'gclid')
                ]
                normalized = normalized._replace(query='&'.join(clean))
            return normalized.geturl()
        except Exception:
            return url

    def _fetch_links(self, url, session):
        links = []
        try:
            time.sleep(self.delay)
            r = session.get(url, timeout=self.timeout, allow_redirects=True)
            if r.status_code != 200:
                return links
            if 'text/html' not in r.headers.get('Content-Type', ''):
                return links
            soup = BeautifulSoup(r.text, 'html.parser')
            for a in soup.find_all('a', href=True):
                href = a['href'].strip()
                if href and not href.startswith('#') and not href.startswith('javascript:'):
                    links.append(urljoin(url, href))
        except Exception:
            pass
        return links

    def _crawl_batch(self, urls_with_depth, allowed_domains, visited, session):
        new_urls = []
        for url, depth in urls_with_depth:
            if depth > self.max_depth:
                continue
            with self._lock:
                if url in visited:
                    continue
                visited.add(url)
                self._pages_crawled += 1
                if self._pages_crawled > self.max_pages:
                    return new_urls
            for link in self._fetch_links(url, session):
                norm = self._normalize_url(link)
                if self._is_valid_url(norm, allowed_domains):
                    with self._lock:
                        if norm not in visited:
                            new_urls.append((norm, depth + 1))
        return new_urls

    def crawl(self, domain_roots, progress_callback=None):
        allowed_domains = set(domain_roots.keys())
        seed_urls = list(domain_roots.values())
        if not seed_urls:
            return {}

        self._pages_crawled = 0
        visited = set()
        all_discovered = {}
        current_level = []

        for norm_domain, root_url in domain_roots.items():
            normalized = self._normalize_url(root_url)
            all_discovered[normalized] = {
                "seed": root_url, "depth": 0, "domain": norm_domain
            }
            current_level.append((normalized, 0))

        if progress_callback:
            progress_callback(0, len(seed_urls), len(all_discovered), 0,
                              f"Starting: {len(seed_urls)} domain root(s)")

        for depth_level in range(self.max_depth + 1):
            if not current_level or self._pages_crawled >= self.max_pages:
                break

            batch_size = max(1, len(current_level) // self.max_workers)
            batches = [current_level[i:i + batch_size]
                       for i in range(0, len(current_level), batch_size)]
            next_level = []

            with ThreadPoolExecutor(
                max_workers=min(self.max_workers, max(len(batches), 1))
            ) as ex:
                futures = [
                    ex.submit(self._crawl_batch, batch, allowed_domains,
                              visited, self._make_session())
                    for batch in batches
                ]
                for f in as_completed(futures):
                    try:
                        for new_url, new_depth in f.result(timeout=120):
                            if new_url not in all_discovered:
                                parsed = urlparse(new_url)
                                domain = parsed.netloc.lower().replace('www.', '')
                                root = domain_roots.get(domain, seed_urls[0])
                                all_discovered[new_url] = {
                                    "seed": root, "depth": new_depth, "domain": domain
                                }
                                next_level.append((new_url, new_depth))
                    except Exception:
                        continue

            if progress_callback:
                progress_callback(
                    self._pages_crawled, len(next_level), len(all_discovered),
                    depth_level,
                    f"Depth {depth_level} done | Crawled: {self._pages_crawled} | "
                    f"Next: {len(next_level)}"
                )
            current_level = next_level

        return all_discovered


# =============================================================================
# URL MATCHING - Checks if discovered URL is covered by the added URLs
# =============================================================================
class URLMatcher:

    @staticmethod
    def normalize_for_comparison(url):
        """Normalize URL for comparison: lowercase, strip trailing slash, remove www."""
        if not url:
            return ""
        return url.strip().rstrip('/').lower().replace('://www.', '://')

    @staticmethod
    def is_url_covered(discovered_url, all_http_urls, regex_patterns):
        """
        Check if a discovered URL is covered by:
        1. Exact match against any HTTP URL extracted from the added URLs
        2. Path match against regex patterns (ev:, cp:, df:, if:)
        """
        norm_discovered = URLMatcher.normalize_for_comparison(discovered_url)

        # --- Check 1: Exact match against all extracted HTTP URLs ---
        for http_url in all_http_urls:
            norm_added = URLMatcher.normalize_for_comparison(http_url)
            if norm_discovered == norm_added:
                return True, "Exact match"

        # --- Check 2: Path-prefix / contains check ---
        # If the discovered URL's path starts with any added URL's path on same domain
        parsed_disc = urlparse(discovered_url)
        disc_domain = parsed_disc.netloc.lower().replace('www.', '')

        for http_url in all_http_urls:
            parsed_added = urlparse(http_url)
            added_domain = parsed_added.netloc.lower().replace('www.', '')
            if disc_domain == added_domain:
                # Check if discovered path starts with the added URL's path
                disc_path = parsed_disc.path.rstrip('/')
                added_path = parsed_added.path.rstrip('/')
                if added_path and disc_path == added_path:
                    return True, f"Path match: {added_path}"

        # --- Check 3: Regex patterns (ev:, cp:, df:, if:) ---
        for pat_str in regex_patterns:
            m = re.match(r'^(ev|cp|df|if):\s*\(?(.*?)\)?\s*$', pat_str, re.IGNORECASE)
            if not m:
                # Try with just colon
                m = re.match(r'^(ev|cp|df|if):(.*)', pat_str, re.IGNORECASE)
            if not m:
                continue
            regex_part = m.group(2).strip()
            if not regex_part:
                continue
            # Remove wrapping parentheses if present
            if regex_part.startswith('(') and regex_part.endswith(')'):
                regex_part_inner = regex_part[1:-1]
            else:
                regex_part_inner = regex_part
            try:
                # Test against path
                if re.search(regex_part, parsed_disc.path):
                    return True, f"Regex match: {pat_str[:60]}"
                if regex_part_inner != regex_part:
                    if re.search(regex_part_inner, parsed_disc.path):
                        return True, f"Regex match: {pat_str[:60]}"
                # Test against full URL
                if re.search(regex_part, discovered_url):
                    return True, f"Regex match: {pat_str[:60]}"
            except re.error:
                continue

        return False, ""


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================
def make_clickable(url):
    """Make a URL into a clickable HTML link."""
    short = url if len(url) <= 80 else url[:77] + "..."
    return f'<a href="{url}" target="_blank" title="{url}">{short}</a>'


def build_missing_df(missing_rows):
    """Build DataFrame from missing rows list."""
    if not missing_rows:
        return pd.DataFrame(columns=["domain", "seed_url", "missing_url", "depth"])
    return pd.DataFrame(missing_rows)


def parse_url_list(text):
    """Parse the URL list input. Accepts JSON array or one-per-line."""
    text = text.strip()
    if not text:
        return None, "Input is empty"

    # Try JSON array parse
    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            return parsed, None
    except json.JSONDecodeError:
        pass

    # Try with relaxed JSON (trailing commas, single quotes)
    try:
        cleaned = text
        cleaned = re.sub(r',\s*\]', ']', cleaned)  # trailing comma before ]
        cleaned = cleaned.replace("'", '"')
        parsed = json.loads(cleaned)
        if isinstance(parsed, list):
            return parsed, None
    except json.JSONDecodeError:
        pass

    # Fallback: treat as one URL per line
    lines = [line.strip().strip(',').strip('"').strip("'") for line in text.split('\n')]
    lines = [l for l in lines if l and l not in ('[', ']')]
    if lines:
        return lines, None

    return None, "Could not parse input. Please provide a JSON array or one URL per line."


# =============================================================================
# MAIN APP
# =============================================================================
def main():
    st.title("🔍 Missing URL Identifier")
    st.markdown("Paste your added URLs → Crawl domains → Find what's missing from your list")
    st.markdown("---")

    with st.expander("ℹ️ How It Works", expanded=False):
        st.markdown("""
        **Input:** Paste your URLs in JSON array format (or one per line).

        **Process:**
        1. All HTTP/HTTPS URLs are extracted from your entries (even from template strings)
        2. Unique domain roots are identified
        3. Each domain root is crawled (configurable depth/threads/pages)
        4. Every discovered URL is checked against your list:
           - **Exact match** against any HTTP URL found in your entries
           - **Regex match** against `ev:`, `cp:`, `df:`, `if:` patterns
        5. URLs on the domain but **NOT** in your list are flagged as **Missing**

        **Supported URL formats in input:**
        - Plain: `https://example.com/page`
        - text: prefixed: `text:https://example.com/page`
        - Template: `${...}json:xhr:https://example.com/api/...`
        - Regex: `ev:(/event-details/|/news-details/)`
        """)

    # --- Session State ---
    if 'crawl_summary' not in st.session_state:
        st.session_state.crawl_summary = None
    if 'missing_df' not in st.session_state:
        st.session_state.missing_df = None
    if 'parsed_urls' not in st.session_state:
        st.session_state.parsed_urls = None
    if 'domain_map' not in st.session_state:
        st.session_state.domain_map = None

    # --- Input ---
    st.subheader("📝 Paste Your URLs")
    url_input = st.text_area(
        "URL List (JSON array format or one per line):",
        height=300,
        placeholder='[\n  "https://example.com/page1",\n  "text:https://example.com/page2",\n  "${miny=:2019}json:xhr:https://example.com/api/...",\n  "ev:(/event-details/|/news-details/)"\n]',
        key="url_input_area"
    )

    col_parse, col_clear = st.columns([2, 2])
    with col_parse:
        parse_btn = st.button("📋 Parse & Analyze URLs", type="primary", use_container_width=True)
    with col_clear:
        clear_btn = st.button("🗑️ Clear All", use_container_width=True)

    if clear_btn:
        st.session_state.crawl_summary = None
        st.session_state.missing_df = None
        st.session_state.parsed_urls = None
        st.session_state.domain_map = None
        st.rerun()

    # --- Parse ---
    if parse_btn:
        if not url_input.strip():
            st.warning("⚠️ Please paste your URLs first!")
        else:
            urls, err = parse_url_list(url_input)
            if err:
                st.error(f"❌ {err}")
            else:
                st.session_state.parsed_urls = urls
                st.session_state.domain_map = DomainUtil.extract_unique_domain_roots(urls)
                st.session_state.crawl_summary = None
                st.session_state.missing_df = None
                st.success(f"✅ Parsed **{len(urls)}** entries successfully!")

    # --- Show parsed info ---
    if st.session_state.parsed_urls is not None:
        urls = st.session_state.parsed_urls
        domain_map = st.session_state.domain_map

        st.markdown("---")
        st.subheader("📊 Parsed URL Analysis")

        # Breakdown
        all_http = URLExtractor.get_all_plain_http_urls(urls)
        regex_pats = URLExtractor.extract_regex_patterns(urls)

        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.metric("Total Entries", len(urls))
        with c2:
            st.metric("HTTP URLs Found", len(all_http))
        with c3:
            st.metric("Regex Patterns", len(regex_pats))
        with c4:
            st.metric("Unique Domains", len(domain_map))

        # Show domains
        if domain_map:
            st.markdown("**🌐 Domains to Crawl:**")
            domain_display = []
            for norm_domain, root_url in sorted(domain_map.items()):
                # Count how many URLs belong to this domain
                count = sum(1 for u in all_http
                            if DomainUtil.get_normalized_domain(u) == norm_domain)
                domain_display.append({
                    "Domain": norm_domain,
                    "Seed URL (root)": root_url,
                    "URLs in List": count,
                })
            st.table(domain_display)
        else:
            st.warning("⚠️ No HTTP URLs found in input. Cannot crawl.")

        # Show regex patterns if any
        if regex_pats:
            with st.expander(f"🔤 Regex Patterns ({len(regex_pats)})", expanded=False):
                for rp in regex_pats:
                    st.code(rp, language=None)

        # Show all extracted HTTP URLs
        with st.expander(f"🔗 All Extracted HTTP URLs ({len(all_http)})", expanded=False):
            for i, u in enumerate(sorted(all_http), 1):
                st.text(f"{i:3d}. {u}")

        # =============================================================
        # CRAWL SECTION
        # =============================================================
        if domain_map:
            st.markdown("---")
            st.header("🕷️ Crawl & Find Missing URLs")

            with st.expander("⚙️ Crawl Settings", expanded=False):
                s1, s2, s3, s4 = st.columns(4)
                with s1:
                    depth = st.slider("Max Depth", 1, 15, 10, key="cd")
                with s2:
                    pages = st.slider("Max Pages", 100, 3000, 1000, 100, key="cp")
                with s3:
                    workers = st.slider("Threads", 5, 100, 50, 5, key="cw")
                with s4:
                    delay = st.slider("Delay (sec)", 0.0, 1.0, 0.1, 0.05, key="cdl")

            crawl_btn = st.button(
                "🔎 Start Crawl & Find Missing URLs",
                type="secondary",
                use_container_width=True,
                key="crawl_btn"
            )

            if crawl_btn:
                crawler = ConcurrentDomainCrawler(
                    max_depth=depth, max_pages=pages,
                    max_workers=workers, delay=delay
                )
                prog = st.progress(0)
                stat = st.empty()

                def cb(crawled, queued, discovered, d, msg):
                    pct = min(crawled / pages, 1.0) if pages > 0 else 0
                    prog.progress(pct)
                    stat.markdown(
                        f"**Crawled:** {crawled} | **Queued:** {queued} | "
                        f"**Discovered:** {discovered} | **Depth:** {d}"
                    )

                with st.spinner(
                    f"🕷️ Crawling {len(domain_map)} domain(s) — "
                    f"{workers} threads, depth {depth}, max {pages} pages..."
                ):
                    discovered = crawler.crawl(domain_map, progress_callback=cb)

                prog.progress(1.0)
                stat.markdown(
                    f"✅ **Crawl complete!** Discovered **{len(discovered)}** URLs "
                    f"across **{len(domain_map)}** domain(s)"
                )

                # --- Match ---
                all_http_urls = URLExtractor.get_all_plain_http_urls(urls)
                regex_patterns = URLExtractor.extract_regex_patterns(urls)

                missing_rows = []
                covered_count = 0

                for url, info in sorted(discovered.items()):
                    covered, reason = URLMatcher.is_url_covered(
                        url, all_http_urls, regex_patterns
                    )
                    if covered:
                        covered_count += 1
                    else:
                        missing_rows.append({
                            "domain": info["domain"],
                            "seed_url": info["seed"],
                            "missing_url": url,
                            "depth": info["depth"],
                        })

                st.session_state.crawl_summary = {
                    "total_discovered": len(discovered),
                    "covered_count": covered_count,
                    "missing_count": len(missing_rows),
                    "domains_crawled": len(domain_map),
                }
                st.session_state.missing_df = build_missing_df(missing_rows)

        # =============================================================
        # DISPLAY RESULTS
        # =============================================================
        if st.session_state.crawl_summary is not None:
            cs = st.session_state.crawl_summary

            if not isinstance(cs, dict) or "total_discovered" not in cs:
                st.warning("⚠️ Crawl data corrupted. Please re-run.")
                st.session_state.crawl_summary = None
                st.session_state.missing_df = None
            else:
                st.markdown("---")
                st.subheader("📊 Results")

                x1, x2, x3, x4 = st.columns(4)
                with x1:
                    st.metric("Domains Crawled", cs.get("domains_crawled", "?"))
                with x2:
                    st.metric("URLs Discovered", cs["total_discovered"])
                with x3:
                    st.metric("Already Covered", cs["covered_count"])
                with x4:
                    st.metric("🔴 Missing", cs["missing_count"])

                df = st.session_state.missing_df

                if df is not None and not df.empty and len(df) > 0:

                    st.markdown(
                        f'<div class="warning-box">'
                        f'<h3>⚠️ {len(df)} URLs Found on Domain but Missing from Your List</h3>'
                        f'</div>',
                        unsafe_allow_html=True
                    )

                    # --- Filters ---
                    st.markdown("**🔧 Filter Results:**")
                    fc1, fc2, fc3 = st.columns(3)
                    with fc1:
                        domain_filter = st.multiselect(
                            "Filter by Domain:",
                            options=sorted(df["domain"].unique()),
                            default=sorted(df["domain"].unique()),
                            key="domain_filter"
                        )
                    with fc2:
                        depth_options = sorted(df["depth"].unique())
                        depth_filter = st.multiselect(
                            "Filter by Depth:",
                            options=depth_options,
                            default=depth_options,
                            key="depth_filter"
                        )
                    with fc3:
                        search_text = st.text_input(
                            "Search in URL:",
                            value="",
                            key="search_filter",
                            placeholder="e.g. /news/ or /investor"
                        )

                    # Apply filters
                    filtered = df[
                        (df["domain"].isin(domain_filter)) &
                        (df["depth"].isin(depth_filter))
                    ]
                    if search_text.strip():
                        filtered = filtered[
                            filtered["missing_url"].str.contains(
                                search_text.strip(), case=False, na=False
                            )
                        ]

                    st.markdown(f"**Showing {len(filtered)} of {len(df)} missing URLs**")

                    if not filtered.empty:
                        # Build display version with clickable links
                        display_df = filtered.copy()
                        display_df["missing_url"] = display_df["missing_url"].apply(make_clickable)
                        display_df["seed_url"] = display_df["seed_url"].apply(make_clickable)

                        # Rename for display
                        rename_map = {
                            "domain": "Domain",
                            "seed_url": "Seed URL",
                            "missing_url": "Missing URL",
                            "depth": "Depth",
                        }
                        display_df = display_df.rename(columns=rename_map)

                        # Sort
                        sort_col = st.selectbox(
                            "Sort by:",
                            list(rename_map.values()),
                            index=0,
                            key="sort_col"
                        )
                        filtered_renamed = filtered.rename(columns=rename_map)
                        sorted_idx = filtered_renamed.sort_values(sort_col).index
                        display_df = display_df.loc[sorted_idx].reset_index(drop=True)
                        display_df.index = display_df.index + 1
                        display_df.index.name = "#"

                        st.markdown(
                            display_df.to_html(escape=False, index=True),
                            unsafe_allow_html=True
                        )

                    # --- Breakdowns ---
                    b1, b2 = st.columns(2)

                    with b1:
                        st.markdown("### 📊 Missing by Domain")
                        dc = df["domain"].value_counts().reset_index()
                        dc.columns = ["Domain", "Missing URLs"]
                        st.table(dc)

                    with b2:
                        st.markdown("### 📊 Missing by Depth")
                        dpc = df["depth"].value_counts().sort_index().reset_index()
                        dpc.columns = ["Depth", "Count"]
                        st.bar_chart(dpc.set_index("Depth"))

                    # --- Downloads ---
                    st.markdown("### 📥 Download Missing URLs")
                    download_df = df.rename(columns={
                        "domain": "Domain",
                        "seed_url": "Seed URL",
                        "missing_url": "Missing URL",
                        "depth": "Depth",
                    })

                    d1, d2, d3 = st.columns(3)
                    with d1:
                        st.download_button(
                            "📥 Download CSV",
                            data=download_df.to_csv(index=False),
                            file_name=f"missing_urls_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            mime="text/csv",
                            use_container_width=True
                        )
                    with d2:
                        st.download_button(
                            "📥 Download JSON",
                            data=download_df.to_json(orient="records", indent=2),
                            file_name=f"missing_urls_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                            mime="application/json",
                            use_container_width=True
                        )
                    with d3:
                        # Plain text: just the missing URLs, one per line
                        plain_text = "\n".join(df["missing_url"].tolist())
                        st.download_button(
                            "📥 Download TXT (URLs only)",
                            data=plain_text,
                            file_name=f"missing_urls_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt",
                            mime="text/plain",
                            use_container_width=True
                        )

                elif cs["missing_count"] == 0:
                    st.markdown("""
                        <div class="success-box">
                            <h3>✅ No Missing URLs!</h3>
                            <p>All discovered URLs on the domain are covered by
                            your URL list (exact match or regex pattern match).</p>
                        </div>
                    """, unsafe_allow_html=True)

    # --- Footer ---
    st.markdown("---")
    st.markdown(
        '<div style="text-align:center;color:#888;padding:20px;font-size:12px;">'
        'Missing URL Identifier v1.0 — Crawl domains & find gaps in your URL list'
        '</div>',
        unsafe_allow_html=True
    )


if __name__ == "__main__":
    main()
