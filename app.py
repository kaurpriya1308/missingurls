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
    .main { padding: 1rem; }
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
    .pdf-box {
        border-left: 4px solid #e53935;
        padding: 15px;
        margin: 10px 0;
        background-color: #ffebee;
        border-radius: 4px;
    }
    .html-box {
        border-left: 4px solid #1565c0;
        padding: 15px;
        margin: 10px 0;
        background-color: #e3f2fd;
        border-radius: 4px;
    }
    .both-box {
        border-left: 4px solid #6a1b9a;
        padding: 15px;
        margin: 10px 0;
        background-color: #f3e5f5;
        border-radius: 4px;
    }
    .oos-box {
        border-left: 4px solid #757575;
        padding: 15px;
        margin: 10px 0;
        background-color: #f5f5f5;
        border-radius: 4px;
    }
    .exclude-box {
        border-left: 4px solid #f57c00;
        padding: 15px;
        margin: 10px 0;
        background-color: #fff3e0;
        border-radius: 4px;
    }
    .dataframe a {
        color: #1a73e8;
        text-decoration: none;
    }
    .dataframe a:hover {
        text-decoration: underline;
    }
    table { font-size: 13px; }
    .module-header {
        padding: 10px 15px;
        border-radius: 6px;
        margin-bottom: 10px;
        font-weight: bold;
    }
    .pdf-header {
        background: linear-gradient(90deg, #ffebee, #ffcdd2);
        color: #b71c1c;
    }
    .html-header {
        background: linear-gradient(90deg, #e3f2fd, #bbdefb);
        color: #0d47a1;
    }
    .keyword-tag {
        display: inline-block;
        background: #fff3e0;
        color: #e65100;
        border: 1px solid #ffb74d;
        border-radius: 12px;
        padding: 2px 10px;
        margin: 2px;
        font-size: 12px;
    }
    </style>
""", unsafe_allow_html=True)


# =============================================================================
# BLOCKED DOMAINS — never crawled
# =============================================================================
ALWAYS_BLOCKED_DOMAINS = {
    "s3.amazonaws.com",
    "amazonaws.com",
}


def is_blocked_domain(url: str) -> bool:
    """Return True if the URL belongs to a domain that must never be crawled."""
    try:
        netloc = urlparse(url).netloc.lower()
        for blocked in ALWAYS_BLOCKED_DOMAINS:
            if netloc == blocked or netloc.endswith("." + blocked):
                return True
    except Exception:
        pass
    return False


# =============================================================================
# ANALYST KEYWORD EXCLUSION HELPER
# =============================================================================
def build_exclusion_regex(keyword_input: str):
    """
    Parse the analyst keyword exclusion box.
    Keywords separated by | are treated as alternatives.
    Each keyword is wrapped so that hyphens / spaces / underscores are optional.
    Returns a compiled regex or None.
    """
    if not keyword_input or not keyword_input.strip():
        return None

    raw_keywords = [k.strip() for k in keyword_input.split("|") if k.strip()]
    if not raw_keywords:
        return None

    patterns = []
    for kw in raw_keywords:
        # escape special regex chars first
        escaped = re.escape(kw)
        # Replace escaped spaces / hyphens / underscores with a flexible separator
        # re.escape turns ' ' -> '\ ', '-' -> '\-', '_' -> '_'
        flexible = re.sub(r'(\\ |\\\-|_)+', r'[\\s\\-_]*', escaped)
        patterns.append(flexible)

    combined = "|".join(f"(?:{p})" for p in patterns)
    try:
        return re.compile(combined, re.IGNORECASE)
    except re.error:
        return None


def url_matches_exclusion(url: str, exclusion_regex) -> bool:
    """Return True if the URL should be excluded by analyst keywords."""
    if exclusion_regex is None:
        return False
    return bool(exclusion_regex.search(url))


# =============================================================================
# DOCUMENT TYPE CLASSIFICATION ENGINE
# =============================================================================
class DocTypeClassifier:
    """
    Classifies discovered URLs into PDF-scope, HTML-scope, Both, or Out-of-Scope.
    """

    # --- PDF ONLY document types ---
    PDF_ONLY_KEYWORDS = [
        r'investor[\s\-_]*day[\s\-_]*presentation',
        r'earnings[\s\-_]*presentation',
        r'supplementary[\s\-_]*information',
        r'non[\s\-_]*gaap[\s\-_]*reconciliation',
        r'non[\s\-_]*ifrs[\s\-_]*measures',
        r'esg[\s\-_]*presentation',
        r'sasb[\s\-_]*presentation',
        r'letter[\s\-_]*to[\s\-_]*shareholders',
        r'roadshow[\s\-_]*presentation',
        r'agm[\s\-_]*presentation',
        r'annual[\s\-_]*general[\s\-_]*meeting[\s\-_]*presentation',
        r'annual[\s\-_]*report',
        r'integrated[\s\-_]*report',
        r'interim[\s\-_]*report',
        r'quarterly[\s\-_]*report',
        r'semi[\s\-_]*annual[\s\-_]*report',
        r'management[\s\-_]*report',
        r'management[\s\-_]*commentary',
        r'md[\s\-_]*&?[\s\-_]*a\b',
        r'prox(?:y|ies)',
        r'proxy[\s\-_]*statement',
        r'contractual[\s\-_]*agreement',
        r'cancellation',
        r'agm[\s\-_]*notice',
        r'egm[\s\-_]*notice',
        r'reorgani[sz]ation',
        r'restructur',
        r'board[\s\-_]*change',
        r'appointment',
        r'resignation',
        r'exemption',
        r'delisting',
        r'suspension',
        r'bankruptcy',
        r'acquisition',
        r'disposal',
        r'legal[\s\-_]*action',
        r'material[\s\-_]*change',
        r'late[\s\-_]*filing',
        r'regulatory[\s\-_]*correspondence',
        r'bond[\s\-_]*prospectus',
        r'fixed[\s\-_]*income[\s\-_]*prospectus',
        r'debt[\s\-_]*prospectus',
        r'prospectus[\s\-_]*general',
        r'equity[\s\-_]*prospectus',
        r'ipo[\s\-_]*prospectus',
        r'securities[\s\-_]*registration',
        r'withdrawal[\s\-_]*termination',
        r'listing[\s\-_]*application',
        r'debt[\s\-_]*indenture',
        r'credit[\s\-_]*agreement',
        r'notice[\s\-_]*of[\s\-_]*offering',
        r'pre[\s\-_]*ipo',
        r'institutional[\s\-_]*ownership',
        r'japan[\s\-_]*5',
        r'director[\s\-_]*officer[\s\-_]*ownership',
        r'beneficial[\s\-_]*ownership',
        r'capital[\s\-_]*change',
        r'stock[\s\-_]*option',
        r'tender[\s\-_]*offer',
        r'exchange[\s\-_]*offer',
        r'stock[\s\-_]*split',
        r'securities[\s\-_]*purchase',
        r'securities[\s\-_]*repurchase',
        r'securities[\s\-_]*sale',
        r'merger',
        r'takeover',
        r'm[\s\-_]*&?[\s\-_]*a\b',
        r'dividend(?![\s\-_]*reinvestment[\s\-_]*stock)',
        r'auditor[\s\-_]*report',
        r'change[\s\-_]*in[\s\-_]*auditor',
        r'change[\s\-_]*in[\s\-_]*year[\s\-_]*end',
        r'fund[\s\-_]*sheet',
        r'estma[\s\-_]*report',
        r'prepared[\s\-_]*remark',
        r'follow[\s\-_]*up[\s\-_]*transcript',
        r'integrated[\s\-_]*resource[\s\-_]*plan',
        r'scientific[\s\-_]*poster',
        r'scientific[\s\-_]*presentation',
        r'research[\s\-_]*publication',
    ]

    # --- HTML ONLY document types ---
    HTML_ONLY_KEYWORDS = [
        r'blog(?:s)?[\s\-_]*(?:&|and)?[\s\-_]*insight',
        r'/blog/',
        r'/blogs/',
        r'/insight/',
        r'/insights/',
        r'about[\s\-_]*us',
        r'/about/',
        r'company[\s\-_]*history',
        r'/history/',
        r'mission[\s\-_]*(?:&|and)?[\s\-_]*vision',
        r'corporate[\s\-_]*information',
        r'management[\s\-_]*profile',
        r'board[\s\-_]*of[\s\-_]*director',
        r'/board/',
        r'executive[\s\-_]*team',
        r'/leadership/',
        r'/management/',
        r'/executives/',
        r'leadership[\s\-_]*committee',
        r'supplier[\s\-_]*list',
        r'/suppliers/',
        r'partner[\s\-_]*list',
        r'/partners/',
        r'customer[\s\-_]*list',
        r'/customers/',
        r'strategic[\s\-_]*alliance',
        r'product[\s\-_]*listing',
        r'/products/',
        r'feature[\s\-_]*description',
        r'/features/',
        r'service[\s\-_]*listing',
        r'/services/',
        r'service[\s\-_]*description',
        r'solutions[\s\-_]*overview',
        r'/solutions/',
        r'service[\s\-_]*model',
    ]

    # --- BOTH PDF & HTML document types ---
    BOTH_KEYWORDS = [
        r'press[\s\-_]*release',
        r'/press[\-_]*release',
        r'/pressrelease',
        r'news[\s\-_]*article',
        r'/news/',
        r'/news[\-_]*detail',
        r'/news[\-_]*events',
        r'company[\s\-_]*announcement',
        r'/announcement',
        r'media[\s\-_]*center',
        r'/media/',
        r'newsroom',
        r'/newsroom/',
        r'operating[\s\-_]*metric',
        r'earnings[\s\-_]*(?!presentation)',
        r'profit[\s\-_]*(?:&|and)?[\s\-_]*loss',
        r'shareholding[\s\-_]*pattern',
        r'corporate[\s\-_]*action',
        r'sustainab',
        r'/sustainability/',
        r'corporate[\s\-_]*social[\s\-_]*responsibility',
        r'\bcsr\b',
        r'environmental[\s\-_]*health[\s\-_]*safety',
        r'\behs\b',
        r'carbon[\s\-_]*disclosure',
        r'green[\s\-_]*report',
        r'\btcfd\b',
        r'climate[\s\-_]*risk',
        r'social[\s\-_]*report',
        r'human[\s\-_]*rights',
        r'diversity[\s\-_]*(?:&|and)?[\s\-_]*inclusion',
        r'\bgri\b',
        r'global[\s\-_]*reporting[\s\-_]*initiative',
        r'\bsasb[\s\-_]*report',
        r'\bcdp\b',
        r'carbon[\s\-_]*disclosure[\s\-_]*project',
        r'company[\s\-_]*polic',
        r'/policies/',
        r'/policy/',
        r'charter',
        r'/charter/',
        r'guideline',
        r'/guidelines/',
        r'code[\s\-_]*of[\s\-_]*ethics',
        r'/ethics/',
        r'governance[\s\-_]*polic',
        r'/governance/',
        r'corporate[\s\-_]*impact',
        r'esg[\s\-_]*report',
        r'topic[\s\-_]*specific[\s\-_]*esg',
        r'white[\s\-_]*paper',
        r'/whitepaper',
        r'case[\s\-_]*stud',
        r'/case[\-_]*stud',
        r'industry[\s\-_]*insight',
        r'thought[\s\-_]*leadership',
        r'factsheet',
        r'fact[\s\-_]*sheet',
        r'factbook',
        r'fact[\s\-_]*book',
        r'product[\s\-_]*brochure',
        r'one[\s\-_]*pager',
        r'speech',
        r'/speeches/',
        r'executive[\s\-_]*commentary',
        r'industry[\s\-_]*trend',
        r'leadership[\s\-_]*insight',
        r'leadership[\s\-_]*interview',
        r'customer[\s\-_]*stor',
        r'/customer[\-_]*stories/',
        r'project[\s\-_]*update',
        r'business[\s\-_]*update',
        r'r[\s\-_]*&?[\s\-_]*d[\s\-_]*update',
        r'research[\s\-_]*(?:&|and)?[\s\-_]*development[\s\-_]*update',
        r'activity[\s\-_]*report',
        r'infographic',
        r'results[\s\-_]*announcement',
        r'earnings[\s\-_]*update',
        r'revenue[\s\-_]*report',
        r'sales[\s\-_]*report',
        r'financial[\s\-_]*highlight',
        r'corporate[\s\-_]*action[\s\-_]*update',
        r'funding[\s\-_]*announcement',
        r'product[\s\-_]*launch',
        r'product[\s\-_]*specification',
        r'product[\s\-_]*spec',
        # Investor Relations — always in-scope
        r'investor[\s\-_]*relation',
        r'/ir/',
        r'/investors/',
        # SEC filings — in-scope investor content
        r'sec[\s\-_]*filing',
        r'/sec[\s\-_]*filing',
        # Email alerts — in-scope
        r'email[\s\-_]*alert',
        # Subsidiary pages — in-scope
        r'subsidiar',
        # Credit ratings — in-scope
        r'credit[\s\-_]*rating',
        # Research / Analyst reports — in-scope (with or without hyphen)
        r'analyst[\s\-_]*report',
        r'research[\s\-_]*report',
        r'research[\s\-_]*analyst',
        r'research[\s\-_]*/?[\s\-_]*analyst',
        # Privacy notices (IR disclosure context) — in-scope
        r'privacy[\s\-_]*notice',
    ]

    # --- OUT OF SCOPE patterns ---
    # NOTE: sec-filings, email-alerts, privacy-notices, subsidiary,
    #       credit-ratings, research/analyst-reports are intentionally
    #       kept OUT of this list — they are in-scope IR content.
    OUT_OF_SCOPE_KEYWORDS = [
        r'privacy[\s\-_]*polic',      # privacy policy ≠ privacy notice
        r'terms[\s\-_]*(?:&|and)?[\s\-_]*condition',
        r'/terms/',
        r'/tos/',
        r'accessibility[\s\-_]*statement',
        r'/accessibility/',
        r'legal[\s\-_]*term',
        r'/legal/',
        r'clinical[\s\-_]*trial',
        r'/clinical[\s\-_]*trial',
        r'drug[\s\-_]*prescription',
        r'fda[\s\-_]*correspondence',
        r'safety[\s\-_]*data[\s\-_]*sheet',
        r'/sds/',
        r'recipe',
        r'/careers/',
        r'/jobs/',
        r'job[\s\-_]*posting',
        r'/career/',
        r'/faq/',
        r'/faqs/',
        r'/contact/',
        r'/contact[\s\-_]*us/',
        r'/forum/',
        r'/forums/',
        r'/chat/',
        r'/e[\s\-_]*commerce/',
        r'/shop/',
        r'/cart/',
        r'/checkout/',
        r'/login/',
        r'/signup/',
        r'/register/',
        r'/account/',
        r'/my[\s\-_]*account/',
        r'/sitemap',
        r'/cookie',
        r'/disclaimer',
        r'/search',
        r'/404',
        r'/error',
        r'gartner',
        r'forrester',
        r'\bidc\b',
    ]

    # --- URL path patterns for quick classification ---
    PDF_PATH_PATTERNS = [
        r'/presentation',
        r'/investor[\-_]*day',
        r'/annual[\-_]*report',
        r'/interim[\-_]*report',
        r'/quarterly[\-_]*report',
        r'/proxy',
        r'/prospectus',
        r'/filing',
        r'/regulatory',
        r'/transcript',
        r'/prepared[\-_]*remarks',
        r'/financial[\-_]*report',
        r'/supplemental',
    ]

    HTML_PATH_PATTERNS = [
        r'/about',
        r'/team',
        r'/leadership',
        r'/management',
        r'/board',
        r'/executives',
        r'/corporate[\-_]*profile',
        r'/company[\-_]*overview',
        r'/products',
        r'/services',
        r'/solutions',
        r'/features',
        r'/blog',
        r'/suppliers',
        r'/partners',
        r'/customers',
    ]

    BOTH_PATH_PATTERNS = [
        r'/news',
        r'/press',
        r'/media',
        r'/newsroom',
        r'/announcement',
        r'/sustainability',
        r'/esg',
        r'/governance',
        r'/corporate[\-_]*impact',
        r'/corporate[\-_]*responsibility',
        r'/csr',
        r'/investor',
        r'/ir/',
        r'/events',
        r'/case[\-_]*stud',
        r'/whitepaper',
        r'/white[\-_]*paper',
        r'/reports',
        r'/updates',
        r'/highlights',
        r'/sec[\-_]*filing',
        r'/credit[\-_]*rating',
        r'/analyst',
        r'/research',
        r'/email[\-_]*alert',
        r'/subsidiar',
    ]

    OOS_PATH_PATTERNS = [
        r'/career',
        r'/jobs',
        r'/contact',
        r'/faq',
        r'/login',
        r'/signup',
        r'/register',
        r'/terms',
        r'/legal',
        r'/cookie',
        r'/accessibility',
        r'/sitemap',
        r'/search',
        r'/404',
        r'/error',
        r'/shop',
        r'/cart',
        r'/checkout',
        r'/forum',
        r'/chat',
        r'/account',
        r'/clinical[\-_]*trial',
        r'/sds',
        r'/disclaimer',
        # /privacy/ only OOS if not privacy-notice context — handled via override
        r'/privacy/',
    ]

    # Patterns that override OOS classification → force in-scope
    IN_SCOPE_OVERRIDE_PATTERNS = [
        r'sec[\s\-_]*filing',
        r'email[\s\-_]*alert',
        r'privacy[\s\-_]*notice',
        r'subsidiar',
        r'credit[\s\-_]*rating',
        r'analyst[\s\-_]*report',
        r'research[\s\-_]*report',
        r'research[\s\-_]*analyst',
        r'investor[\s\-_]*relation',
        r'/ir/',
        r'/investors/',
        r'annual[\s\-_]*general[\s\-_]*meeting',
    ]

    @classmethod
    def _matches_in_scope_override(cls, url_lower: str) -> bool:
        for pat in cls.IN_SCOPE_OVERRIDE_PATTERNS:
            if re.search(pat, url_lower, re.IGNORECASE):
                return True
        return False

    @classmethod
    def classify_url(cls, url: str):
        """
        Returns (classification, confidence, matched_pattern)
        classification: 'PDF' | 'HTML' | 'Both' | 'Out of Scope' | 'Unclassified'
        """
        if not isinstance(url, str):
            return "Unclassified", "low", ""

        url_lower = url.lower()
        parsed = urlparse(url)
        path_lower = parsed.path.lower()

        # Step 0: Blocked domains
        if is_blocked_domain(url):
            return "Out of Scope", "high", "blocked domain (s3.amazonaws.com)"

        # Step 1: OOS check — but respect in-scope overrides
        if not cls._matches_in_scope_override(url_lower):
            for pat in cls.OOS_PATH_PATTERNS:
                if re.search(pat, path_lower):
                    return "Out of Scope", "high", pat
            for kw in cls.OUT_OF_SCOPE_KEYWORDS:
                if re.search(kw, url_lower):
                    return "Out of Scope", "high", kw

        # Step 2: File extension
        if path_lower.endswith('.pdf'):
            return "PDF", "high", ".pdf extension"

        # Step 3: Keyword patterns
        for kw in cls.PDF_ONLY_KEYWORDS:
            if re.search(kw, url_lower):
                return "PDF", "medium", kw
        for pat in cls.PDF_PATH_PATTERNS:
            if re.search(pat, path_lower):
                return "PDF", "medium", pat

        for kw in cls.HTML_ONLY_KEYWORDS:
            if re.search(kw, url_lower):
                return "HTML", "medium", kw
        for pat in cls.HTML_PATH_PATTERNS:
            if re.search(pat, path_lower):
                return "HTML", "medium", pat

        for kw in cls.BOTH_KEYWORDS:
            if re.search(kw, url_lower):
                return "Both", "medium", kw
        for pat in cls.BOTH_PATH_PATTERNS:
            if re.search(pat, path_lower):
                return "Both", "medium", pat

        return "Unclassified", "low", ""

    @classmethod
    def is_in_scope(cls, url: str, check_mode: str) -> bool:
        classification, _, _ = cls.classify_url(url)
        if classification == "Out of Scope":
            return False
        if check_mode == "Both":
            return True
        elif check_mode == "PDF":
            return classification in ("PDF", "Both", "Unclassified")
        elif check_mode == "HTML":
            return classification in ("HTML", "Both", "Unclassified")
        return True


# =============================================================================
# URL EXTRACTOR
# =============================================================================
class URLExtractor:

    @staticmethod
    def extract_all_http_urls(raw_url):
        if not isinstance(raw_url, str):
            return []
        urls = []
        http_pattern = r'(https?://[^\s\'"<>\}\)]+)'
        matches = re.findall(http_pattern, raw_url)
        for match in matches:
            cleaned = match.rstrip(',;|')
            cleaned = re.sub(r'\}+$', '', cleaned)
            if cleaned and len(cleaned) > 10 and not is_blocked_domain(cleaned):
                urls.append(cleaned)
        return urls

    @staticmethod
    def extract_regex_patterns(urls):
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
        try:
            parsed = urlparse(url.strip())
            if not parsed.scheme or not parsed.netloc:
                return None
            return urlunparse((parsed.scheme, parsed.netloc, '', '', '', ''))
        except Exception:
            return None

    @staticmethod
    def get_normalized_domain(url):
        try:
            parsed = urlparse(url.strip())
            return parsed.netloc.lower().replace('www.', '')
        except Exception:
            return None

    @staticmethod
    def extract_unique_domain_roots(urls):
        domain_map = {}
        all_http_urls = URLExtractor.get_all_plain_http_urls(urls)
        for url in all_http_urls:
            url = url.strip()
            if not url.startswith('http'):
                continue
            if is_blocked_domain(url):
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
            if is_blocked_domain(url):
                return False
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
                    full = urljoin(url, href)
                    if not is_blocked_domain(full):
                        links.append(full)
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
# URL MATCHING
# =============================================================================
class URLMatcher:

    @staticmethod
    def normalize_for_comparison(url):
        if not url:
            return ""
        return url.strip().rstrip('/').lower().replace('://www.', '://')

    @staticmethod
    def is_url_covered(discovered_url, all_http_urls, regex_patterns):
        norm_discovered = URLMatcher.normalize_for_comparison(discovered_url)

        for http_url in all_http_urls:
            norm_added = URLMatcher.normalize_for_comparison(http_url)
            if norm_discovered == norm_added:
                return True, "Exact match"

        parsed_disc = urlparse(discovered_url)
        disc_domain = parsed_disc.netloc.lower().replace('www.', '')

        for http_url in all_http_urls:
            parsed_added = urlparse(http_url)
            added_domain = parsed_added.netloc.lower().replace('www.', '')
            if disc_domain == added_domain:
                disc_path = parsed_disc.path.rstrip('/')
                added_path = parsed_added.path.rstrip('/')
                if added_path and disc_path == added_path:
                    return True, f"Path match: {added_path}"

        for pat_str in regex_patterns:
            m = re.match(r'^(ev|cp|df|if):\s*\(?(.*?)\)?\s*$', pat_str, re.IGNORECASE)
            if not m:
                m = re.match(r'^(ev|cp|df|if):(.*)', pat_str, re.IGNORECASE)
            if not m:
                continue
            regex_part = m.group(2).strip()
            if not regex_part:
                continue
            regex_part_inner = (
                regex_part[1:-1]
                if regex_part.startswith('(') and regex_part.endswith(')')
                else regex_part
            )
            try:
                if re.search(regex_part, parsed_disc.path):
                    return True, f"Regex: {pat_str[:60]}"
                if regex_part_inner != regex_part:
                    if re.search(regex_part_inner, parsed_disc.path):
                        return True, f"Regex: {pat_str[:60]}"
                if re.search(regex_part, discovered_url):
                    return True, f"Regex: {pat_str[:60]}"
            except re.error:
                continue

        return False, ""


# =============================================================================
# HELPERS
# =============================================================================
def make_clickable(url):
    short = url if len(url) <= 80 else url[:77] + "..."
    return f'<a href="{url}" target="_blank" title="{url}">{short}</a>'


def build_missing_df(missing_rows):
    if not missing_rows:
        return pd.DataFrame(columns=[
            "domain", "seed_url", "missing_url", "depth",
            "doc_classification", "confidence", "matched_pattern", "source_module"
        ])
    return pd.DataFrame(missing_rows)


def parse_url_list(text):
    text = text.strip()
    if not text:
        return None, "Input is empty"
    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            return parsed, None
    except json.JSONDecodeError:
        pass
    try:
        cleaned = re.sub(r',\s*\]', ']', text).replace("'", '"')
        parsed = json.loads(cleaned)
        if isinstance(parsed, list):
            return parsed, None
    except json.JSONDecodeError:
        pass
    lines = [line.strip().strip(',').strip('"').strip("'") for line in text.split('\n')]
    lines = [l for l in lines if l and l not in ('[', ']')]
    if lines:
        return lines, None
    return None, "Could not parse input."


def get_classification_color(cls):
    return {
        "PDF": "🔴", "HTML": "🔵", "Both": "🟣",
        "Out of Scope": "⚫", "Unclassified": "⚪",
    }.get(cls, "⚪")


def get_classification_badge(cls):
    badges = {
        "PDF": '<span style="background:#ffcdd2;color:#b71c1c;padding:2px 8px;border-radius:10px;font-size:11px;">PDF</span>',
        "HTML": '<span style="background:#bbdefb;color:#0d47a1;padding:2px 8px;border-radius:10px;font-size:11px;">HTML</span>',
        "Both": '<span style="background:#e1bee7;color:#6a1b9a;padding:2px 8px;border-radius:10px;font-size:11px;">Both</span>',
        "Out of Scope": '<span style="background:#e0e0e0;color:#424242;padding:2px 8px;border-radius:10px;font-size:11px;">OOS</span>',
        "Unclassified": '<span style="background:#f5f5f5;color:#757575;padding:2px 8px;border-radius:10px;font-size:11px;">N/A</span>',
    }
    return badges.get(cls, badges["Unclassified"])


# =============================================================================
# MAIN APP
# =============================================================================
def main():
    st.title("🔍 Missing URL Identifier")
    st.markdown(
        "**Dual-module input** → Crawl domains → Find missing URLs → "
        "Classified by PDF / HTML scope"
    )
    st.markdown("---")

    with st.expander("ℹ️ How It Works", expanded=False):
        st.markdown("""
        **Two Input Modules:**
        - 🔴 **PDF Module**: Paste URLs where you're extracting **PDF documents** from HTML pages
        - 🔵 **HTML Module**: Paste URLs where you're extracting **HTML pages**
        - You can use one or both modules

        **Check Modes:**
        - **PDF Only**: Finds missing URLs relevant to PDF document types
        - **HTML Only**: Finds missing URLs relevant to HTML page types
        - **Both (Combined)**: Finds ALL missing URLs across both scopes

        **Keyword Exclusion Box:**
        - Enter keywords separated by `|` to hide irrelevant URLs from the results table
        - Hyphens / spaces / underscores are interchangeable in matching
          (e.g. `email-alerts` also matches `email alerts` and `email_alerts`)
        - Excluded URLs are removed from the Missing URLs table only — the raw data is preserved
        - Click **Apply Exclusions** to activate; **Clear Exclusions** to reset

        **Blocked Domains:**
        - `s3.amazonaws.com` (and all subdomains) are **never** crawled or shown as missing URLs

        **In-Scope IR content (never marked Out of Scope):**
        SEC Filings · Email Alerts · Privacy Notices (IR) · Subsidiary pages ·
        Credit Ratings · Research / Analyst Reports · Investor Relations

        **Document Classification** is based on 100+ patterns covering:
        Presentations, Press Releases, Filings, ESG, Sector Content, Company Info,
        Business Updates, Products & Services
        """)

    # Session state initialisation
    for key in ['crawl_summary', 'missing_df', 'parsed_pdf_urls', 'parsed_html_urls',
                'combined_urls', 'domain_map', 'check_mode', 'exclusion_keywords']:
        if key not in st.session_state:
            st.session_state[key] = None

    # =================================================================
    # CHECK MODE
    # =================================================================
    st.subheader("🎯 Check Mode")
    check_mode = st.radio(
        "What type of missing URLs do you want to find?",
        options=["Both (Combined)", "PDF Only", "HTML Only"],
        horizontal=True,
        index=0,
        key="check_mode_radio",
    )
    mode_map = {"Both (Combined)": "Both", "PDF Only": "PDF", "HTML Only": "HTML"}
    selected_mode = mode_map[check_mode]
    st.markdown("---")

    # =================================================================
    # DUAL INPUT MODULES
    # =================================================================
    st.subheader("📝 Input Modules")

    if selected_mode == "PDF":
        st.info("🔴 **PDF Mode**: Only the PDF module is active.")
    elif selected_mode == "HTML":
        st.info("🔵 **HTML Mode**: Only the HTML module is active.")
    else:
        st.info("🟣 **Combined Mode**: Both modules are active.")

    col_pdf, col_html = st.columns(2)

    with col_pdf:
        st.markdown(
            '<div class="module-header pdf-header">'
            '🔴 PDF Module — URLs for PDF Document Extraction</div>',
            unsafe_allow_html=True
        )
        pdf_enabled = selected_mode in ("PDF", "Both")
        pdf_input = st.text_area(
            "Paste PDF extraction URLs (JSON array or one per line):",
            height=250,
            placeholder='[\n  "https://ir.company.com/presentations",\n  "https://company.com/annual-reports"\n]',
            key="pdf_input_area",
            disabled=not pdf_enabled,
        )
        if not pdf_enabled:
            st.caption("⏸️ Disabled in HTML-only mode")

    with col_html:
        st.markdown(
            '<div class="module-header html-header">'
            '🔵 HTML Module — URLs for HTML Page Extraction</div>',
            unsafe_allow_html=True
        )
        html_enabled = selected_mode in ("HTML", "Both")
        html_input = st.text_area(
            "Paste HTML page URLs (JSON array or one per line):",
            height=250,
            placeholder='[\n  "https://company.com/about-us",\n  "https://company.com/sustainability/"\n]',
            key="html_input_area",
            disabled=not html_enabled,
        )
        if not html_enabled:
            st.caption("⏸️ Disabled in PDF-only mode")

    st.markdown("")
    bcol1, bcol2 = st.columns(2)
    with bcol1:
        parse_btn = st.button("📋 Parse & Analyze URLs", type="primary", use_container_width=True)
    with bcol2:
        clear_btn = st.button("🗑️ Clear All", use_container_width=True)

    if clear_btn:
        for key in ['crawl_summary', 'missing_df', 'parsed_pdf_urls', 'parsed_html_urls',
                    'combined_urls', 'domain_map', 'exclusion_keywords']:
            st.session_state[key] = None
        st.rerun()

    # =================================================================
    # PARSE
    # =================================================================
    if parse_btn:
        pdf_urls, html_urls, errors = [], [], []

        if pdf_enabled and pdf_input.strip():
            parsed, err = parse_url_list(pdf_input)
            if err:
                errors.append(f"🔴 PDF Module: {err}")
            else:
                pdf_urls = parsed
                st.session_state.parsed_pdf_urls = parsed

        if html_enabled and html_input.strip():
            parsed, err = parse_url_list(html_input)
            if err:
                errors.append(f"🔵 HTML Module: {err}")
            else:
                html_urls = parsed
                st.session_state.parsed_html_urls = parsed

        for e in errors:
            st.error(f"❌ {e}")

        if not pdf_urls and not html_urls:
            if not errors:
                st.warning("⚠️ Please paste URLs in at least one module!")
        else:
            combined = pdf_urls + html_urls
            st.session_state.combined_urls = combined
            st.session_state.domain_map = DomainUtil.extract_unique_domain_roots(combined)
            st.session_state.crawl_summary = None
            st.session_state.missing_df = None
            st.success(
                f"✅ Parsed **{len(combined)}** total entries "
                f"(🔴 PDF: {len(pdf_urls)} | 🔵 HTML: {len(html_urls)})"
            )

    # =================================================================
    # PARSED INFO
    # =================================================================
    combined = st.session_state.combined_urls
    domain_map = st.session_state.domain_map
    pdf_urls_parsed = st.session_state.parsed_pdf_urls or []
    html_urls_parsed = st.session_state.parsed_html_urls or []

    if combined is not None and domain_map is not None:
        st.markdown("---")
        st.subheader("📊 Parsed URL Analysis")

        all_http = URLExtractor.get_all_plain_http_urls(combined)
        regex_pats = URLExtractor.extract_regex_patterns(combined)
        pdf_http = URLExtractor.get_all_plain_http_urls(pdf_urls_parsed) if pdf_urls_parsed else []
        html_http = URLExtractor.get_all_plain_http_urls(html_urls_parsed) if html_urls_parsed else []
        pdf_regex = URLExtractor.extract_regex_patterns(pdf_urls_parsed) if pdf_urls_parsed else []
        html_regex = URLExtractor.extract_regex_patterns(html_urls_parsed) if html_urls_parsed else []

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Total Entries", len(combined))
        c2.metric("🔴 PDF Entries", len(pdf_urls_parsed))
        c3.metric("🔵 HTML Entries", len(html_urls_parsed))
        c4.metric("HTTP URLs Found", len(all_http))
        c5.metric("Unique Domains", len(domain_map))

        mod_col1, mod_col2 = st.columns(2)
        with mod_col1:
            with st.expander(f"🔴 PDF Module Details ({len(pdf_urls_parsed)} entries)", expanded=False):
                if pdf_urls_parsed:
                    st.write(f"**HTTP URLs:** {len(pdf_http)} | **Regex Patterns:** {len(pdf_regex)}")
                    for i, u in enumerate(pdf_urls_parsed, 1):
                        st.text(f"{i:3d}. {str(u)[:120]}{'...' if len(str(u)) > 120 else ''}")
                else:
                    st.caption("No URLs in PDF module")

        with mod_col2:
            with st.expander(f"🔵 HTML Module Details ({len(html_urls_parsed)} entries)", expanded=False):
                if html_urls_parsed:
                    st.write(f"**HTTP URLs:** {len(html_http)} | **Regex Patterns:** {len(html_regex)}")
                    for i, u in enumerate(html_urls_parsed, 1):
                        st.text(f"{i:3d}. {str(u)[:120]}{'...' if len(str(u)) > 120 else ''}")
                else:
                    st.caption("No URLs in HTML module")

        if domain_map:
            # Warn about blocked domains
            all_raw_http = URLExtractor.get_all_plain_http_urls(combined)
            blocked_found = [u for u in all_raw_http if is_blocked_domain(u)]
            if blocked_found:
                st.warning(
                    f"⚠️ **{len(blocked_found)} URL(s) from blocked domains "
                    f"(s3.amazonaws.com) were detected and excluded from crawling.**"
                )

            st.markdown("**🌐 Domains to Crawl:**")
            domain_display = []
            for norm_domain, root_url in sorted(domain_map.items()):
                count = sum(1 for u in all_http if DomainUtil.get_normalized_domain(u) == norm_domain)
                in_pdf = any(DomainUtil.get_normalized_domain(u) == norm_domain for u in pdf_http)
                in_html = any(DomainUtil.get_normalized_domain(u) == norm_domain for u in html_http)
                modules = []
                if in_pdf:
                    modules.append("🔴 PDF")
                if in_html:
                    modules.append("🔵 HTML")
                domain_display.append({
                    "Domain": norm_domain,
                    "Seed URL": root_url,
                    "URLs in List": count,
                    "Module(s)": " + ".join(modules) if modules else "—",
                })
            st.table(domain_display)
        else:
            st.warning("⚠️ No HTTP URLs found. Cannot crawl.")

        if regex_pats:
            with st.expander(f"🔤 Regex Patterns ({len(regex_pats)})", expanded=False):
                for rp in regex_pats:
                    source = "🔴" if rp in pdf_regex else ("🔵" if rp in html_regex else "⚪")
                    st.code(f"{source} {rp}", language=None)

        # =============================================================
        # CRAWL
        # =============================================================
        if domain_map:
            st.markdown("---")
            mode_emoji = {"PDF": "🔴", "HTML": "🔵", "Both": "🟣"}
            st.header(
                f"🕷️ Crawl & Find Missing URLs "
                f"{mode_emoji.get(selected_mode, '')} [{selected_mode} Mode]"
            )

            with st.expander("⚙️ Crawl Settings", expanded=False):
                s1, s2, s3, s4 = st.columns(4)
                with s1:
                    depth = st.slider("Max Depth", 1, 15, 10, key="cd")
                with s2:
                    pages = st.slider("Max Pages", 100, 3000, 1000, 100, key="cp")
                with s3:
                    workers = st.slider("Threads", 5, 100, 50, 5, key="cw")
                with s4:
                    delay_val = st.slider("Delay (sec)", 0.0, 1.0, 0.1, 0.05, key="cdl")

            crawl_btn = st.button(
                f"🔎 Start Crawl — Find Missing {selected_mode} URLs",
                type="secondary",
                use_container_width=True,
                key="crawl_btn"
            )

            if crawl_btn:
                crawler = ConcurrentDomainCrawler(
                    max_depth=depth, max_pages=pages,
                    max_workers=workers, delay=delay_val
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

                all_http_urls = URLExtractor.get_all_plain_http_urls(combined)
                regex_patterns = URLExtractor.extract_regex_patterns(combined)

                missing_rows = []
                covered_count = 0
                oos_count = 0
                filtered_out_count = 0

                for url, info in sorted(discovered.items()):
                    if is_blocked_domain(url):
                        oos_count += 1
                        continue

                    covered, reason = URLMatcher.is_url_covered(url, all_http_urls, regex_patterns)
                    if covered:
                        covered_count += 1
                        continue

                    doc_class, confidence, matched_pat = DocTypeClassifier.classify_url(url)

                    if not DocTypeClassifier.is_in_scope(url, selected_mode):
                        if doc_class == "Out of Scope":
                            oos_count += 1
                        else:
                            filtered_out_count += 1
                        continue

                    source_modules = []
                    if doc_class in ("PDF", "Both", "Unclassified"):
                        source_modules.append("PDF")
                    if doc_class in ("HTML", "Both", "Unclassified"):
                        source_modules.append("HTML")
                    source_module_str = " + ".join(source_modules) if source_modules else "Unclassified"

                    missing_rows.append({
                        "domain": info["domain"],
                        "seed_url": info["seed"],
                        "missing_url": url,
                        "depth": info["depth"],
                        "doc_classification": doc_class,
                        "confidence": confidence,
                        "matched_pattern": matched_pat[:60] if matched_pat else "",
                        "source_module": source_module_str,
                    })

                st.session_state.crawl_summary = {
                    "total_discovered": len(discovered),
                    "covered_count": covered_count,
                    "missing_count": len(missing_rows),
                    "oos_count": oos_count,
                    "filtered_out_count": filtered_out_count,
                    "domains_crawled": len(domain_map),
                    "check_mode": selected_mode,
                }
                st.session_state.missing_df = build_missing_df(missing_rows)

        # =============================================================
        # RESULTS
        # =============================================================
        if st.session_state.crawl_summary is not None:
            cs = st.session_state.crawl_summary

            if not isinstance(cs, dict) or "total_discovered" not in cs:
                st.warning("⚠️ Crawl data corrupted. Please re-run.")
                st.session_state.crawl_summary = None
                st.session_state.missing_df = None
                st.stop()

            st.markdown("---")
            mode_label = cs.get("check_mode", "Both")
            mode_emoji_map = {"PDF": "🔴", "HTML": "🔵", "Both": "🟣"}
            st.subheader(
                f"📊 Results — {mode_emoji_map.get(mode_label, '')} {mode_label} Mode"
            )

            x1, x2, x3, x4, x5 = st.columns(5)
            x1.metric("Discovered", cs["total_discovered"])
            x2.metric("Covered", cs["covered_count"])
            x3.metric("🔴 Missing (raw)", cs["missing_count"])
            x4.metric("Out of Scope", cs["oos_count"])
            x5.metric("Filtered (mode)", cs["filtered_out_count"])

            df = st.session_state.missing_df

            if df is not None and not df.empty:

                # =================================================
                # ANALYST KEYWORD EXCLUSION BOX
                # =================================================
                st.markdown("---")
                st.markdown(
                    '<div class="exclude-box">'
                    '<b>🔑 Analyst Keyword Exclusion</b><br>'
                    'Enter keywords separated by <code>|</code> to remove matching URLs '
                    'from the Missing URLs table. '
                    'Hyphens, spaces, and underscores are treated as interchangeable '
                    '(e.g. <code>email-alerts</code> also matches <code>email alerts</code> '
                    'and <code>email_alerts</code>).<br><br>'
                    '<i>Examples: '
                    '<code>sec-filings | email-alerts | privacy-notice | subsidiary | '
                    'credit-ratings | analyst-report | research/analyst</code>'
                    '</i>'
                    '</div>',
                    unsafe_allow_html=True
                )

                excl_col1, excl_col2, excl_col3 = st.columns([4, 1, 1])
                with excl_col1:
                    exclusion_input = st.text_input(
                        "Keywords to exclude (separate with |):",
                        value=st.session_state.exclusion_keywords or "",
                        placeholder=(
                            "e.g.  sec-filings | email-alerts | privacy-notice | "
                            "subsidiary | credit-ratings | analyst-report"
                        ),
                        key="exclusion_input_box",
                        label_visibility="collapsed",
                    )
                with excl_col2:
                    apply_excl = st.button(
                        "✂️ Apply",
                        use_container_width=True,
                        key="apply_excl_btn"
                    )
                with excl_col3:
                    clear_excl = st.button(
                        "🔄 Clear",
                        use_container_width=True,
                        key="clear_excl_btn"
                    )

                if apply_excl:
                    st.session_state.exclusion_keywords = exclusion_input.strip()
                    st.rerun()

                if clear_excl:
                    st.session_state.exclusion_keywords = None
                    st.rerun()

                # Build active exclusion regex
                active_excl_kw = st.session_state.exclusion_keywords or ""
                excl_regex = build_exclusion_regex(active_excl_kw)

                if excl_regex is not None:
                    mask_excl = df["missing_url"].apply(
                        lambda u: url_matches_exclusion(u, excl_regex)
                    )
                    df_display = df[~mask_excl].copy()
                    excl_count = int(mask_excl.sum())

                    kw_list = [k.strip() for k in active_excl_kw.split("|") if k.strip()]
                    tags_html = " ".join(
                        f'<span class="keyword-tag">✂️ {k}</span>' for k in kw_list
                    )
                    st.markdown(
                        f"**Active exclusions ({len(kw_list)} keyword(s) | "
                        f"{excl_count} URLs removed):** {tags_html}",
                        unsafe_allow_html=True
                    )

                    if excl_count > 0:
                        with st.expander(
                            f"👁️ View {excl_count} excluded URL(s)", expanded=False
                        ):
                            excl_urls = df[mask_excl]["missing_url"].tolist()
                            for eu in excl_urls:
                                st.markdown(
                                    f'<a href="{eu}" target="_blank">{eu}</a>',
                                    unsafe_allow_html=True
                                )
                else:
                    df_display = df.copy()
                    excl_count = 0
                    if active_excl_kw:
                        st.warning(
                            "⚠️ Could not parse exclusion keywords. "
                            "Check for invalid regex characters."
                        )

                st.markdown("---")
                effective_missing = len(df_display)

                st.markdown(
                    f'<div class="warning-box">'
                    f'<h3>⚠️ {effective_missing} Missing URLs Found ({mode_label} scope)</h3>'
                    f'<p>These URLs exist on the domain but are NOT in your added URL list. '
                    f'{excl_count} URLs hidden by keyword exclusions.</p>'
                    f'</div>',
                    unsafe_allow_html=True
                )

                if df_display.empty:
                    st.success(
                        "✅ All remaining missing URLs have been excluded "
                        "by your keyword filters."
                    )
                else:
                    # --- FILTERS ---
                    st.markdown("**🔧 Filter Results:**")
                    fc1, fc2, fc3, fc4 = st.columns(4)

                    with fc1:
                        domain_opts = sorted(df_display["domain"].unique())
                        domain_filter = st.multiselect(
                            "Domain:", domain_opts, default=domain_opts, key="df_domain"
                        )
                    with fc2:
                        class_opts = sorted(df_display["doc_classification"].unique())
                        class_filter = st.multiselect(
                            "Classification:", class_opts, default=class_opts, key="df_class"
                        )
                    with fc3:
                        depth_opts = sorted(df_display["depth"].unique())
                        depth_filter = st.multiselect(
                            "Depth:", depth_opts, default=depth_opts, key="df_depth"
                        )
                    with fc4:
                        search_text = st.text_input(
                            "Search URL:", value="", key="df_search",
                            placeholder="e.g. /news/ or /investor"
                        )

                    filtered = df_display[
                        (df_display["domain"].isin(domain_filter)) &
                        (df_display["doc_classification"].isin(class_filter)) &
                        (df_display["depth"].isin(depth_filter))
                    ]
                    if search_text.strip():
                        filtered = filtered[
                            filtered["missing_url"].str.contains(
                                search_text.strip(), case=False, na=False
                            )
                        ]

                    removed_by_excl = cs['missing_count'] - effective_missing
                    st.markdown(
                        f"**Showing {len(filtered)} of {effective_missing} missing URLs** "
                        f"*(keyword exclusions removed {removed_by_excl})*"
                    )

                    if not filtered.empty:
                        disp = filtered.copy()
                        disp["missing_url"] = disp["missing_url"].apply(make_clickable)
                        disp["seed_url"] = disp["seed_url"].apply(make_clickable)
                        disp["doc_classification"] = disp["doc_classification"].apply(
                            get_classification_badge
                        )
                        disp["source_module"] = disp["source_module"].apply(
                            lambda x: (
                                x.replace("PDF", "🔴 PDF").replace("HTML", "🔵 HTML")
                                if isinstance(x, str) else x
                            )
                        )

                        rename_map = {
                            "domain": "Domain",
                            "seed_url": "Seed URL",
                            "missing_url": "Missing URL",
                            "depth": "Depth",
                            "doc_classification": "Type",
                            "confidence": "Conf.",
                            "matched_pattern": "Pattern Match",
                            "source_module": "Module",
                        }
                        disp = disp.rename(columns=rename_map)
                        filtered_renamed = filtered.rename(columns=rename_map)

                        sort_options = ["Domain", "Depth", "Type", "Module", "Missing URL"]
                        sort_col = st.selectbox("Sort by:", sort_options, index=0, key="sort_col")
                        sorted_idx = filtered_renamed.sort_values(
                            sort_col, key=lambda x: x.astype(str)
                        ).index
                        disp = disp.loc[sorted_idx].reset_index(drop=True)
                        disp.index = disp.index + 1
                        disp.index.name = "#"

                        with st.expander("📋 Show/Hide Columns", expanded=False):
                            all_cols = list(rename_map.values())
                            show_cols = st.multiselect(
                                "Visible columns:", all_cols,
                                default=["Domain", "Missing URL", "Depth", "Type", "Module"],
                                key="visible_cols"
                            )
                        show_cols = show_cols if show_cols else ["Domain", "Missing URL", "Depth", "Type", "Module"]
                        st.markdown(
                            disp[show_cols].to_html(escape=False, index=True),
                            unsafe_allow_html=True
                        )

                # --- BREAKDOWNS ---
                st.markdown("---")
                if not df_display.empty:
                    bd1, bd2, bd3 = st.columns(3)

                    with bd1:
                        st.markdown("### 📊 By Domain")
                        dc = df_display["domain"].value_counts().reset_index()
                        dc.columns = ["Domain", "Count"]
                        st.table(dc)

                    with bd2:
                        st.markdown("### 📊 By Classification")
                        cc = df_display["doc_classification"].value_counts().reset_index()
                        cc.columns = ["Classification", "Count"]
                        cc["Classification"] = cc["Classification"].apply(
                            lambda x: f"{get_classification_color(x)} {x}"
                        )
                        st.table(cc)

                    with bd3:
                        st.markdown("### 📊 By Module")
                        mc = df_display["source_module"].value_counts().reset_index()
                        mc.columns = ["Module", "Count"]
                        st.table(mc)

                    st.markdown("### 📊 Missing by Depth")
                    dpc = df_display["depth"].value_counts().sort_index().reset_index()
                    dpc.columns = ["Depth", "Count"]
                    st.bar_chart(dpc.set_index("Depth"))

                # --- DOWNLOADS ---
                st.markdown("### 📥 Download Missing URLs")
                dl_rename = {
                    "domain": "Domain", "seed_url": "Seed URL",
                    "missing_url": "Missing URL", "depth": "Depth",
                    "doc_classification": "Doc Type", "confidence": "Confidence",
                    "matched_pattern": "Pattern Match", "source_module": "Module",
                }
                download_df = df_display.rename(columns=dl_rename)
                ts = datetime.now().strftime('%Y%m%d_%H%M%S')

                d1, d2, d3, d4 = st.columns(4)
                with d1:
                    st.download_button(
                        "📥 Full CSV (post-exclusion)",
                        data=download_df.to_csv(index=False),
                        file_name=f"missing_urls_{selected_mode}_{ts}.csv",
                        mime="text/csv",
                        use_container_width=True
                    )
                with d2:
                    st.download_button(
                        "📥 Full JSON (post-exclusion)",
                        data=download_df.to_json(orient="records", indent=2),
                        file_name=f"missing_urls_{selected_mode}_{ts}.json",
                        mime="application/json",
                        use_container_width=True
                    )
                with d3:
                    plain = "\n".join(df_display["missing_url"].tolist())
                    st.download_button(
                        "📥 URLs Only (TXT)",
                        data=plain,
                        file_name=f"missing_urls_{selected_mode}_{ts}.txt",
                        mime="text/plain",
                        use_container_width=True
                    )
                with d4:
                    if not df_display.empty:
                        # filtered view download — use the last `filtered` if available
                        try:
                            filt_dl = filtered.rename(columns=dl_rename)
                            st.download_button(
                                f"📥 Filtered ({len(filtered)})",
                                data=filt_dl.to_csv(index=False),
                                file_name=f"missing_filtered_{ts}.csv",
                                mime="text/csv",
                                use_container_width=True
                            )
                        except Exception:
                            pass

            elif cs["missing_count"] == 0:
                st.markdown(f"""
                    <div class="success-box">
                        <h3>✅ No Missing URLs! ({mode_label} scope)</h3>
                        <p>All discovered URLs relevant to <b>{mode_label}</b> document types
                        are covered by your URL list.</p>
                        <p><b>{cs['oos_count']}</b> URLs were excluded as Out of Scope.
                        <b>{cs['filtered_out_count']}</b> were filtered by mode selection.</p>
                    </div>
                """, unsafe_allow_html=True)

    # =================================================================
    # REFERENCE TABLE
    # =================================================================
    with st.expander("📚 Document Type Classification Reference", expanded=False):
        st.markdown("### Document Types by Category")
        ref_data = [
            ("**1. PRESENTATIONS**", "", ""),
            ("Investor Day / Earnings Presentations", "🔴 PDF", ""),
            ("Supplementary / Non-GAAP / Non-IFRS", "🔴 PDF", ""),
            ("ESG / SASB / Roadshow / AGM Presentations", "🔴 PDF", ""),
            ("Letter to Shareholders", "🔴 PDF", ""),
            ("**2. PRESS RELEASES / NEWS**", "", ""),
            ("Press Releases / News Articles", "🔴 PDF", "🔵 HTML"),
            ("Company Announcements / Newsroom", "🔴 PDF", "🔵 HTML"),
            ("**3. FILINGS**", "", ""),
            ("Annual / Integrated / Interim Report", "🔴 PDF", ""),
            ("Proxy Statements / Prospectus", "🔴 PDF", ""),
            ("SEC Filings", "🟣 Both", "🟣 Both"),
            ("Operating Metrics / Earnings", "🟣 Both", "🟣 Both"),
            ("Shareholding Pattern / Corporate Actions", "🟣 Both", "🟣 Both"),
            ("**4. ESG**", "", ""),
            ("Sustainability / CSR / TCFD / GRI / CDP", "🟣 Both", "🟣 Both"),
            ("Governance / Policies / Charters", "🟣 Both", "🟣 Both"),
            ("ESTMA Reports", "🔴 PDF", ""),
            ("**5. INVESTOR RELATIONS (always in-scope)**", "", ""),
            ("SEC Filings · Email Alerts · Credit Ratings", "🟣 Both", "🟣 Both"),
            ("Research / Analyst Reports · Subsidiary pages", "🟣 Both", "🟣 Both"),
            ("Privacy Notices (IR context)", "🟣 Both", "🟣 Both"),
            ("**6. SECTOR-SPECIFIC**", "", ""),
            ("White Papers / Case Studies / Factsheets", "🟣 Both", "🟣 Both"),
            ("Blogs & Insights", "", "🔵 HTML"),
            ("Prepared Remarks / Transcripts", "🔴 PDF", ""),
            ("**7. COMPANY INFO**", "", ""),
            ("About / History / Leadership / Board", "", "🔵 HTML"),
            ("Partners / Suppliers / Customers", "", "🔵 HTML"),
            ("**8. BUSINESS UPDATES**", "", ""),
            ("All Business Updates & Reports", "🟣 Both", "🟣 Both"),
            ("**9. PRODUCTS & SERVICES**", "", ""),
            ("Product / Service Listings", "", "🔵 HTML"),
            ("Product Launch / Specs / Brochures", "🟣 Both", "🟣 Both"),
        ]
        ref_df = pd.DataFrame(ref_data, columns=["Document Type", "PDF Scope", "HTML Scope"])
        st.table(ref_df)

    st.markdown("---")
    st.markdown(
        '<div style="text-align:center;color:#888;padding:20px;font-size:12px;">'
        'Missing URL Identifier v2.1 — Dual Module | PDF/HTML Classification | '
        'Domain Crawler | Analyst Keyword Exclusion | s3.amazonaws.com blocked'
        '</div>',
        unsafe_allow_html=True
    )


if __name__ == "__main__":
    main()
