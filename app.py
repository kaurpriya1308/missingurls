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

# =============================================================================
# PAGE CONFIG — must be first Streamlit call
# =============================================================================
try:
    st.set_page_config(
        page_title="Missing URL Identifier",
        page_icon="🔍",
        layout="wide",
        initial_sidebar_state="collapsed"
    )
except Exception:
    pass

# =============================================================================
# CSS
# =============================================================================
st.markdown("""
<style>
.main{padding:1rem}
.stTextArea textarea{font-family:'Courier New',monospace;font-size:12px}
.success-box{border-left:4px solid #00c853;padding:15px;margin:10px 0;
    background:#f1f8f4;border-radius:4px}
.warning-box{border-left:4px solid #ff9800;padding:15px;margin:10px 0;
    background:#fff8e1;border-radius:4px}
.exclude-box{border-left:4px solid #f57c00;padding:15px;margin:10px 0;
    background:#fff3e0;border-radius:4px}
.module-header{padding:10px 15px;border-radius:6px;margin-bottom:10px;font-weight:bold}
.pdf-header{background:linear-gradient(90deg,#ffebee,#ffcdd2);color:#b71c1c}
.html-header{background:linear-gradient(90deg,#e3f2fd,#bbdefb);color:#0d47a1}
.keyword-tag{display:inline-block;background:#fff3e0;color:#e65100;
    border:1px solid #ffb74d;border-radius:12px;padding:2px 10px;margin:2px;font-size:12px}
table{font-size:13px}
.dataframe a{color:#1a73e8;text-decoration:none}
.dataframe a:hover{text-decoration:underline}
</style>
""", unsafe_allow_html=True)


# =============================================================================
# CONSTANTS
# =============================================================================
ALWAYS_BLOCKED_DOMAINS = {"s3.amazonaws.com", "amazonaws.com"}

SESSION_KEYS = [
    'crawl_summary', 'missing_df', 'parsed_pdf_urls',
    'parsed_html_urls', 'combined_urls', 'domain_map',
    'exclusion_keywords',
]


# =============================================================================
# SAFE HELPERS
# =============================================================================
def safe_rerun():
    try:
        st.rerun()
    except Exception:
        pass


def init_session():
    for key in SESSION_KEYS:
        if key not in st.session_state:
            st.session_state[key] = None


def get_ss(key, default=None):
    return st.session_state.get(key, default)


def set_ss(key, value):
    st.session_state[key] = value


def reset_results():
    for key in ['crawl_summary', 'missing_df']:
        set_ss(key, None)


def reset_all():
    for key in SESSION_KEYS:
        set_ss(key, None)


# =============================================================================
# BLOCKED DOMAIN CHECK
# =============================================================================
def is_blocked_domain(url: str) -> bool:
    try:
        netloc = urlparse(str(url)).netloc.lower()
        for blocked in ALWAYS_BLOCKED_DOMAINS:
            if netloc == blocked or netloc.endswith("." + blocked):
                return True
    except Exception:
        pass
    return False


# =============================================================================
# EXCLUSION REGEX BUILDER
# =============================================================================
def build_exclusion_regex(keyword_input: str):
    try:
        if not keyword_input or not keyword_input.strip():
            return None
        raw = [k.strip() for k in keyword_input.split("|") if k.strip()]
        if not raw:
            return None
        patterns = []
        for kw in raw:
            escaped = re.escape(kw)
            # make hyphens/spaces/underscores interchangeable
            flexible = re.sub(r'(\\ |\\\-|_)+', r'[\\s\\-_]*', escaped)
            patterns.append(f"(?:{flexible})")
        combined = "|".join(patterns)
        return re.compile(combined, re.IGNORECASE)
    except Exception:
        return None


def url_matches_exclusion(url: str, excl_regex) -> bool:
    try:
        if excl_regex is None:
            return False
        return bool(excl_regex.search(str(url)))
    except Exception:
        return False


# =============================================================================
# DOCUMENT CLASSIFIER
# =============================================================================
class DocTypeClassifier:

    PDF_ONLY = [
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
        r'agm[\s\-_]*notice',
        r'egm[\s\-_]*notice',
        r'reorgani[sz]ation',
        r'restructur',
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
        r'equity[\s\-_]*prospectus',
        r'ipo[\s\-_]*prospectus',
        r'securities[\s\-_]*registration',
        r'debt[\s\-_]*indenture',
        r'credit[\s\-_]*agreement',
        r'notice[\s\-_]*of[\s\-_]*offering',
        r'pre[\s\-_]*ipo',
        r'institutional[\s\-_]*ownership',
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
        r'dividend(?![\s\-_]*reinvestment[\s\-_]*stock)',
        r'auditor[\s\-_]*report',
        r'change[\s\-_]*in[\s\-_]*auditor',
        r'fund[\s\-_]*sheet',
        r'estma[\s\-_]*report',
        r'prepared[\s\-_]*remark',
        r'follow[\s\-_]*up[\s\-_]*transcript',
        r'integrated[\s\-_]*resource[\s\-_]*plan',
        r'scientific[\s\-_]*poster',
        r'scientific[\s\-_]*presentation',
        r'research[\s\-_]*publication',
    ]

    HTML_ONLY = [
        r'/blog(?:s)?/',
        r'/insight(?:s)?/',
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
        r'/suppliers/',
        r'/partners/',
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

    BOTH = [
        r'press[\s\-_]*release',
        r'news[\s\-_]*article',
        r'/news/',
        r'company[\s\-_]*announcement',
        r'/announcement',
        r'media[\s\-_]*center',
        r'/media/',
        r'newsroom',
        r'/newsroom/',
        r'operating[\s\-_]*metric',
        r'profit[\s\-_]*(?:&|and)?[\s\-_]*loss',
        r'shareholding[\s\-_]*pattern',
        r'corporate[\s\-_]*action',
        r'sustainab',
        r'/sustainability/',
        r'corporate[\s\-_]*social[\s\-_]*responsibility',
        r'\bcsr\b',
        r'environmental[\s\-_]*health[\s\-_]*safety',
        r'carbon[\s\-_]*disclosure',
        r'green[\s\-_]*report',
        r'\btcfd\b',
        r'climate[\s\-_]*risk',
        r'social[\s\-_]*report',
        r'human[\s\-_]*rights',
        r'diversity[\s\-_]*(?:&|and)?[\s\-_]*inclusion',
        r'\bgri\b',
        r'global[\s\-_]*reporting[\s\-_]*initiative',
        r'\bcdp\b',
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
        r'white[\s\-_]*paper',
        r'/whitepaper',
        r'case[\s\-_]*stud',
        r'industry[\s\-_]*insight',
        r'thought[\s\-_]*leadership',
        r'fact[\s\-_]*sheet',
        r'fact[\s\-_]*book',
        r'product[\s\-_]*brochure',
        r'one[\s\-_]*pager',
        r'speech',
        r'/speeches/',
        r'executive[\s\-_]*commentary',
        r'industry[\s\-_]*trend',
        r'leadership[\s\-_]*insight',
        r'customer[\s\-_]*stor',
        r'project[\s\-_]*update',
        r'business[\s\-_]*update',
        r'activity[\s\-_]*report',
        r'infographic',
        r'results[\s\-_]*announcement',
        r'earnings[\s\-_]*update',
        r'revenue[\s\-_]*report',
        r'sales[\s\-_]*report',
        r'financial[\s\-_]*highlight',
        r'funding[\s\-_]*announcement',
        r'product[\s\-_]*launch',
        r'product[\s\-_]*specification',
        r'product[\s\-_]*spec',
        # IR — always in scope
        r'investor[\s\-_]*relation',
        r'/ir/',
        r'/investors/',
        r'sec[\s\-_]*filing',
        r'email[\s\-_]*alert',
        r'subsidiar',
        r'credit[\s\-_]*rating',
        r'analyst[\s\-_]*report',
        r'research[\s\-_]*report',
        r'research[\s\-_]*/?[\s\-_]*analyst',
        r'privacy[\s\-_]*notice',
    ]

    OOS_KEYWORDS = [
        r'privacy[\s\-_]*polic',
        r'terms[\s\-_]*(?:&|and)?[\s\-_]*condition',
        r'/terms/',
        r'/tos/',
        r'accessibility[\s\-_]*statement',
        r'/accessibility/',
        r'legal[\s\-_]*term',
        r'/legal/',
        r'clinical[\s\-_]*trial',
        r'drug[\s\-_]*prescription',
        r'fda[\s\-_]*correspondence',
        r'safety[\s\-_]*data[\s\-_]*sheet',
        r'/sds/',
        r'recipe',
        r'/careers/',
        r'/jobs/',
        r'job[\s\-_]*posting',
        r'/career/',
        r'/faq[s]?/',
        r'/contact/',
        r'/forum[s]?/',
        r'/chat/',
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

    OOS_PATHS = [
        r'/career', r'/jobs', r'/contact', r'/faq',
        r'/login', r'/signup', r'/register', r'/terms',
        r'/legal', r'/cookie', r'/accessibility', r'/sitemap',
        r'/search', r'/404', r'/error', r'/shop', r'/cart',
        r'/checkout', r'/forum', r'/chat', r'/account',
        r'/clinical[\-_]*trial', r'/sds', r'/disclaimer', r'/privacy/',
    ]

    PDF_PATHS = [
        r'/presentation', r'/investor[\-_]*day', r'/annual[\-_]*report',
        r'/interim[\-_]*report', r'/quarterly[\-_]*report', r'/proxy',
        r'/prospectus', r'/filing', r'/regulatory', r'/transcript',
        r'/prepared[\-_]*remarks', r'/financial[\-_]*report', r'/supplemental',
    ]

    HTML_PATHS = [
        r'/about', r'/team', r'/leadership', r'/management', r'/board',
        r'/executives', r'/products', r'/services', r'/solutions',
        r'/features', r'/blog', r'/suppliers', r'/partners', r'/customers',
    ]

    BOTH_PATHS = [
        r'/news', r'/press', r'/media', r'/newsroom', r'/announcement',
        r'/sustainability', r'/esg', r'/governance', r'/corporate[\-_]*impact',
        r'/corporate[\-_]*responsibility', r'/csr', r'/investor', r'/ir/',
        r'/events', r'/case[\-_]*stud', r'/whitepaper', r'/white[\-_]*paper',
        r'/reports', r'/updates', r'/highlights', r'/sec[\-_]*filing',
        r'/credit[\-_]*rating', r'/analyst', r'/research',
        r'/email[\-_]*alert', r'/subsidiar',
    ]

    # override: if URL matches any of these, skip OOS check
    IN_SCOPE_OVERRIDES = [
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
    def _match_any(cls, text: str, patterns: list) -> str:
        for pat in patterns:
            try:
                if re.search(pat, text, re.IGNORECASE):
                    return pat
            except re.error:
                continue
        return ""

    @classmethod
    def _is_override(cls, url_lower: str) -> bool:
        return bool(cls._match_any(url_lower, cls.IN_SCOPE_OVERRIDES))

    @classmethod
    def classify(cls, url: str):
        """Returns (label, confidence, pattern)"""
        try:
            if not isinstance(url, str) or not url:
                return "Unclassified", "low", ""

            ul = url.lower()
            try:
                path = urlparse(url).path.lower()
            except Exception:
                path = ""

            if is_blocked_domain(url):
                return "Out of Scope", "high", "blocked:s3.amazonaws.com"

            # OOS check (skip if override matches)
            if not cls._is_override(ul):
                p = cls._match_any(path, cls.OOS_PATHS)
                if p:
                    return "Out of Scope", "high", p
                k = cls._match_any(ul, cls.OOS_KEYWORDS)
                if k:
                    return "Out of Scope", "high", k

            if path.endswith('.pdf'):
                return "PDF", "high", ".pdf"

            k = cls._match_any(ul, cls.PDF_ONLY)
            if k:
                return "PDF", "medium", k
            p = cls._match_any(path, cls.PDF_PATHS)
            if p:
                return "PDF", "medium", p

            k = cls._match_any(ul, cls.HTML_ONLY)
            if k:
                return "HTML", "medium", k
            p = cls._match_any(path, cls.HTML_PATHS)
            if p:
                return "HTML", "medium", p

            k = cls._match_any(ul, cls.BOTH)
            if k:
                return "Both", "medium", k
            p = cls._match_any(path, cls.BOTH_PATHS)
            if p:
                return "Both", "medium", p

            return "Unclassified", "low", ""

        except Exception:
            return "Unclassified", "low", ""

    @classmethod
    def in_scope(cls, url: str, mode: str) -> bool:
        try:
            label, _, _ = cls.classify(url)
            if label == "Out of Scope":
                return False
            if mode == "Both":
                return True
            if mode == "PDF":
                return label in ("PDF", "Both", "Unclassified")
            if mode == "HTML":
                return label in ("HTML", "Both", "Unclassified")
            return True
        except Exception:
            return True


# =============================================================================
# URL EXTRACTOR
# =============================================================================
class URLExtractor:

    HTTP_RE = re.compile(r'(https?://[^\s\'"<>\}\)]+)')

    @classmethod
    def extract_http(cls, raw: str) -> list:
        if not isinstance(raw, str):
            return []
        out = []
        try:
            for m in cls.HTTP_RE.findall(raw):
                cleaned = m.rstrip(',;|')
                cleaned = re.sub(r'\}+$', '', cleaned)
                if len(cleaned) > 10 and not is_blocked_domain(cleaned):
                    out.append(cleaned)
        except Exception:
            pass
        return out

    @classmethod
    def extract_regex_patterns(cls, urls: list) -> list:
        pats = []
        for u in urls:
            try:
                if isinstance(u, str) and re.match(r'^(ev|cp|df|if):', u.strip(), re.IGNORECASE):
                    pats.append(u.strip())
            except Exception:
                continue
        return pats

    @classmethod
    def all_http(cls, urls: list) -> list:
        out = []
        for u in urls:
            out.extend(cls.extract_http(str(u)))
        return list(set(out))


# =============================================================================
# DOMAIN UTILITY
# =============================================================================
class DomainUtil:

    @staticmethod
    def root(url: str):
        try:
            p = urlparse(url.strip())
            if p.scheme and p.netloc:
                return urlunparse((p.scheme, p.netloc, '', '', '', ''))
        except Exception:
            pass
        return None

    @staticmethod
    def norm(url: str):
        try:
            return urlparse(url.strip()).netloc.lower().replace('www.', '')
        except Exception:
            return None

    @staticmethod
    def domain_map(urls: list) -> dict:
        dm = {}
        for url in URLExtractor.all_http(urls):
            try:
                url = url.strip()
                if not url.startswith('http') or is_blocked_domain(url):
                    continue
                r = DomainUtil.root(url)
                n = DomainUtil.norm(url)
                if r and n and n not in dm:
                    dm[n] = r
            except Exception:
                continue
        return dm


# =============================================================================
# CRAWLER
# =============================================================================
class Crawler:

    SKIP_EXT = {
        '.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx',
        '.zip', '.rar', '.tar', '.gz', '.7z', '.jpg', '.jpeg', '.png',
        '.gif', '.bmp', '.svg', '.ico', '.webp', '.mp3', '.mp4', '.avi',
        '.mov', '.wmv', '.flv', '.wav', '.css', '.js', '.woff', '.woff2',
        '.ttf', '.eot', '.otf', '.exe', '.dmg', '.msi', '.apk',
    }

    SKIP_PATH = [
        r'/wp-content/', r'/wp-includes/', r'/wp-admin/',
        r'/assets/', r'/static/', r'/images/', r'/img/',
        r'/fonts/', r'/css/', r'/js/', r'javascript:',
        r'mailto:', r'tel:', r'/cdn-cgi/', r'/feed/', r'/rss/',
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
        self._count = 0

    def _session(self):
        s = requests.Session()
        s.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                          'AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,*/*;q=0.8',
        })
        s.max_redirects = 5
        return s

    def _valid(self, url: str, allowed: set) -> bool:
        try:
            if is_blocked_domain(url):
                return False
            p = urlparse(url)
            if p.scheme not in ('http', 'https') or not p.netloc:
                return False
            if p.netloc.lower().replace('www.', '') not in allowed:
                return False
            pl = p.path.lower()
            for ext in self.SKIP_EXT:
                if pl.endswith(ext):
                    return False
            ul = url.lower()
            for pat in self.SKIP_PATH:
                if re.search(pat, ul):
                    return False
            return True
        except Exception:
            return False

    def _normalize(self, url: str) -> str:
        try:
            p = urlparse(url)
            p = p._replace(fragment='')
            path = p.path.rstrip('/') or '/'
            p = p._replace(path=path)
            if p.query:
                qs = [x for x in p.query.split('&')
                      if x.split('=')[0].lower() not in
                      ('utm_source', 'utm_medium', 'utm_campaign',
                       'utm_term', 'utm_content', 'fbclid', 'gclid')]
                p = p._replace(query='&'.join(qs))
            return p.geturl()
        except Exception:
            return url

    def _links(self, url: str, session) -> list:
        out = []
        try:
            time.sleep(self.delay)
            r = session.get(url, timeout=self.timeout, allow_redirects=True)
            if r.status_code != 200:
                return out
            ct = r.headers.get('Content-Type', '')
            if 'text/html' not in ct:
                return out
            soup = BeautifulSoup(r.text, 'html.parser')
            for a in soup.find_all('a', href=True):
                try:
                    href = a['href'].strip()
                    if href and not href.startswith(('#', 'javascript:')):
                        full = urljoin(url, href)
                        if not is_blocked_domain(full):
                            out.append(full)
                except Exception:
                    continue
        except Exception:
            pass
        return out

    def _batch(self, items, allowed, visited, session):
        new = []
        for url, depth in items:
            try:
                if depth > self.max_depth:
                    continue
                with self._lock:
                    if url in visited:
                        continue
                    visited.add(url)
                    self._count += 1
                    if self._count > self.max_pages:
                        return new
                for link in self._links(url, session):
                    norm = self._normalize(link)
                    if self._valid(norm, allowed):
                        with self._lock:
                            if norm not in visited:
                                new.append((norm, depth + 1))
            except Exception:
                continue
        return new

    def crawl(self, dm: dict, on_progress=None) -> dict:
        try:
            allowed = set(dm.keys())
            seeds = list(dm.values())
            if not seeds:
                return {}

            self._count = 0
            visited = set()
            discovered = {}
            level = []

            for nd, ru in dm.items():
                try:
                    n = self._normalize(ru)
                    discovered[n] = {"seed": ru, "depth": 0, "domain": nd}
                    level.append((n, 0))
                except Exception:
                    continue

            for dlvl in range(self.max_depth + 1):
                if not level or self._count >= self.max_pages:
                    break
                bs = max(1, len(level) // max(self.max_workers, 1))
                batches = [level[i:i + bs] for i in range(0, len(level), bs)]
                nxt = []

                try:
                    with ThreadPoolExecutor(
                        max_workers=min(self.max_workers, max(len(batches), 1))
                    ) as ex:
                        futs = [
                            ex.submit(self._batch, b, allowed, visited, self._session())
                            for b in batches
                        ]
                        for f in as_completed(futs):
                            try:
                                for nu, nd_val in f.result(timeout=120):
                                    if nu not in discovered:
                                        try:
                                            dom = urlparse(nu).netloc.lower().replace('www.', '')
                                            root = dm.get(dom, seeds[0])
                                            discovered[nu] = {
                                                "seed": root, "depth": nd_val, "domain": dom
                                            }
                                            nxt.append((nu, nd_val))
                                        except Exception:
                                            continue
                            except Exception:
                                continue
                except Exception:
                    pass

                if on_progress:
                    try:
                        on_progress(self._count, len(nxt), len(discovered), dlvl)
                    except Exception:
                        pass
                level = nxt

            return discovered
        except Exception:
            return {}


# =============================================================================
# URL MATCHER
# =============================================================================
class URLMatcher:

    @staticmethod
    def _norm(url: str) -> str:
        try:
            return url.strip().rstrip('/').lower().replace('://www.', '://')
        except Exception:
            return ""

    @staticmethod
    def covered(disc_url: str, http_urls: list, regex_pats: list) -> bool:
        try:
            nd = URLMatcher._norm(disc_url)
            for u in http_urls:
                if URLMatcher._norm(u) == nd:
                    return True

            try:
                pd_ = urlparse(disc_url)
                dd = pd_.netloc.lower().replace('www.', '')
                dp = pd_.path.rstrip('/')
            except Exception:
                dd, dp = "", ""

            for u in http_urls:
                try:
                    pa = urlparse(u)
                    ad = pa.netloc.lower().replace('www.', '')
                    ap = pa.path.rstrip('/')
                    if dd == ad and ap and dp == ap:
                        return True
                except Exception:
                    continue

            for pat in regex_pats:
                try:
                    m = re.match(r'^(ev|cp|df|if):(.*)', pat, re.IGNORECASE)
                    if not m:
                        continue
                    rp = m.group(2).strip().strip('()')
                    if not rp:
                        continue
                    if re.search(rp, pd_.path) or re.search(rp, disc_url):
                        return True
                except Exception:
                    continue

        except Exception:
            pass
        return False


# =============================================================================
# UI HELPERS
# =============================================================================
def clickable(url: str) -> str:
    try:
        short = url if len(url) <= 80 else url[:77] + "..."
        return f'<a href="{url}" target="_blank" title="{url}">{short}</a>'
    except Exception:
        return str(url)


def cls_badge(label: str) -> str:
    styles = {
        "PDF": "background:#ffcdd2;color:#b71c1c",
        "HTML": "background:#bbdefb;color:#0d47a1",
        "Both": "background:#e1bee7;color:#6a1b9a",
        "Out of Scope": "background:#e0e0e0;color:#424242",
        "Unclassified": "background:#f5f5f5;color:#757575",
    }
    s = styles.get(label, styles["Unclassified"])
    return (f'<span style="{s};padding:2px 8px;border-radius:10px;'
            f'font-size:11px;">{label}</span>')


def cls_emoji(label: str) -> str:
    return {"PDF": "🔴", "HTML": "🔵", "Both": "🟣",
            "Out of Scope": "⚫", "Unclassified": "⚪"}.get(label, "⚪")


def parse_input(text: str):
    text = text.strip()
    if not text:
        return None, "Input is empty"
    for attempt in [
        lambda t: json.loads(t),
        lambda t: json.loads(re.sub(r',\s*\]', ']', t).replace("'", '"')),
    ]:
        try:
            r = attempt(text)
            if isinstance(r, list):
                return r, None
        except Exception:
            continue
    lines = [ln.strip().strip(',\'"') for ln in text.splitlines()]
    lines = [ln for ln in lines if ln and ln not in ('[', ']')]
    if lines:
        return lines, None
    return None, "Could not parse input"


def build_df(rows: list) -> pd.DataFrame:
    cols = ["domain", "seed_url", "missing_url", "depth",
            "doc_classification", "confidence", "matched_pattern", "source_module"]
    if not rows:
        return pd.DataFrame(columns=cols)
    try:
        return pd.DataFrame(rows)
    except Exception:
        return pd.DataFrame(columns=cols)


# =============================================================================
# MAIN
# =============================================================================
def main():
    init_session()

    st.title("🔍 Missing URL Identifier")
    st.markdown("**Dual-module input → Crawl domains → Find missing URLs → PDF / HTML scope**")
    st.markdown("---")

    # How it works
    with st.expander("ℹ️ How It Works", expanded=False):
        st.markdown("""
**Modules:** 🔴 PDF — paste PDF-extraction URLs | 🔵 HTML — paste HTML-page URLs

**Modes:** Both (Combined) | PDF Only | HTML Only

**Keyword Exclusion:** Enter terms separated by `|` to hide matching URLs.
Hyphens / spaces / underscores are interchangeable.
*(e.g. `email-alerts` matches `email alerts` and `email_alerts`)*

**Always blocked from crawling:** `s3.amazonaws.com`

**Always in-scope (never OOS):**
SEC Filings · Email Alerts · Privacy Notices (IR) · Subsidiary ·
Credit Ratings · Research/Analyst Reports · Investor Relations
        """)

    # ------------------------------------------------------------------
    # MODE
    # ------------------------------------------------------------------
    st.subheader("🎯 Check Mode")
    mode_choice = st.radio(
        "Find missing URLs of type:",
        ["Both (Combined)", "PDF Only", "HTML Only"],
        horizontal=True, index=0, key="mode_radio"
    )
    MODE = {"Both (Combined)": "Both", "PDF Only": "PDF", "HTML Only": "HTML"}[mode_choice]
    st.markdown("---")

    # ------------------------------------------------------------------
    # INPUT
    # ------------------------------------------------------------------
    st.subheader("📝 Input Modules")
    pdf_on = MODE in ("PDF", "Both")
    html_on = MODE in ("HTML", "Both")

    info_msgs = {
        "PDF": "🔴 **PDF Mode** active — HTML module disabled",
        "HTML": "🔵 **HTML Mode** active — PDF module disabled",
        "Both": "🟣 **Combined Mode** — both modules active",
    }
    st.info(info_msgs[MODE])

    col1, col2 = st.columns(2)
    with col1:
        st.markdown('<div class="module-header pdf-header">🔴 PDF Module</div>',
                    unsafe_allow_html=True)
        pdf_raw = st.text_area("PDF URLs", height=220, key="pdf_ta", disabled=not pdf_on,
                               placeholder='["https://ir.company.com/presentations"]')
        if not pdf_on:
            st.caption("⏸️ Disabled in HTML-only mode")

    with col2:
        st.markdown('<div class="module-header html-header">🔵 HTML Module</div>',
                    unsafe_allow_html=True)
        html_raw = st.text_area("HTML URLs", height=220, key="html_ta", disabled=not html_on,
                                placeholder='["https://company.com/about-us"]')
        if not html_on:
            st.caption("⏸️ Disabled in PDF-only mode")

    b1, b2 = st.columns(2)
    with b1:
        parse_btn = st.button("📋 Parse & Analyze", type="primary", use_container_width=True)
    with b2:
        if st.button("🗑️ Clear All", use_container_width=True):
            reset_all()
            safe_rerun()

    # ------------------------------------------------------------------
    # PARSE
    # ------------------------------------------------------------------
    if parse_btn:
        pdf_urls, html_urls = [], []
        errs = []

        if pdf_on and pdf_raw.strip():
            r, e = parse_input(pdf_raw)
            if e:
                errs.append(f"🔴 PDF: {e}")
            else:
                pdf_urls = r or []
                set_ss('parsed_pdf_urls', pdf_urls)

        if html_on and html_raw.strip():
            r, e = parse_input(html_raw)
            if e:
                errs.append(f"🔵 HTML: {e}")
            else:
                html_urls = r or []
                set_ss('parsed_html_urls', html_urls)

        for e in errs:
            st.error(f"❌ {e}")

        if not pdf_urls and not html_urls:
            if not errs:
                st.warning("⚠️ Paste URLs in at least one module")
        else:
            combined = pdf_urls + html_urls
            set_ss('combined_urls', combined)
            set_ss('domain_map', DomainUtil.domain_map(combined))
            reset_results()
            st.success(
                f"✅ Parsed **{len(combined)}** entries "
                f"(🔴 {len(pdf_urls)} PDF | 🔵 {len(html_urls)} HTML)"
            )

    # ------------------------------------------------------------------
    # ANALYSIS SECTION — only shown when we have parsed data
    # ------------------------------------------------------------------
    combined = get_ss('combined_urls')
    dm = get_ss('domain_map')

    if combined is None or dm is None:
        return

    pdf_urls_p = get_ss('parsed_pdf_urls') or []
    html_urls_p = get_ss('parsed_html_urls') or []

    st.markdown("---")
    st.subheader("📊 Parsed URL Analysis")

    all_http = URLExtractor.all_http(combined)
    regex_pats = URLExtractor.extract_regex_patterns(combined)
    pdf_http = URLExtractor.all_http(pdf_urls_p)
    html_http = URLExtractor.all_http(html_urls_p)

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total Entries", len(combined))
    c2.metric("🔴 PDF", len(pdf_urls_p))
    c3.metric("🔵 HTML", len(html_urls_p))
    c4.metric("HTTP URLs", len(all_http))
    c5.metric("Domains", len(dm))

    m1, m2 = st.columns(2)
    with m1:
        with st.expander(f"🔴 PDF entries ({len(pdf_urls_p)})", expanded=False):
            for i, u in enumerate(pdf_urls_p, 1):
                st.text(f"{i:3d}. {str(u)[:110]}")

    with m2:
        with st.expander(f"🔵 HTML entries ({len(html_urls_p)})", expanded=False):
            for i, u in enumerate(html_urls_p, 1):
                st.text(f"{i:3d}. {str(u)[:110]}")

    # Blocked domain warning
    blocked = [u for u in URLExtractor.all_http(combined) if is_blocked_domain(u)]
    if blocked:
        st.warning(f"⚠️ {len(blocked)} URL(s) from s3.amazonaws.com excluded from crawling")

    if not dm:
        st.warning("⚠️ No crawlable HTTP URLs found")
        return

    # Domain table
    st.markdown("**🌐 Domains to Crawl:**")
    rows = []
    for nd, root in sorted(dm.items()):
        cnt = sum(1 for u in all_http if DomainUtil.norm(u) == nd)
        mods = []
        if any(DomainUtil.norm(u) == nd for u in pdf_http):
            mods.append("🔴 PDF")
        if any(DomainUtil.norm(u) == nd for u in html_http):
            mods.append("🔵 HTML")
        rows.append({"Domain": nd, "Seed URL": root,
                     "URLs in list": cnt, "Module(s)": " + ".join(mods) or "—"})
    st.table(rows)

    if regex_pats:
        with st.expander(f"🔤 Regex patterns ({len(regex_pats)})", expanded=False):
            for rp in regex_pats:
                st.code(rp, language=None)

    # ------------------------------------------------------------------
    # CRAWL
    # ------------------------------------------------------------------
    st.markdown("---")
    em = {"PDF": "🔴", "HTML": "🔵", "Both": "🟣"}.get(MODE, "")
    st.header(f"🕷️ Crawl & Find Missing URLs {em} [{MODE} Mode]")

    with st.expander("⚙️ Settings", expanded=False):
        s1, s2, s3, s4 = st.columns(4)
        max_depth = s1.slider("Max Depth", 1, 15, 10, key="s_depth")
        max_pages = s2.slider("Max Pages", 100, 3000, 1000, 100, key="s_pages")
        max_workers = s3.slider("Threads", 5, 100, 50, 5, key="s_workers")
        delay = s4.slider("Delay (s)", 0.0, 1.0, 0.1, 0.05, key="s_delay")

    if st.button(f"🔎 Start Crawl [{MODE} Mode]", type="secondary",
                 use_container_width=True, key="crawl_btn"):
        crawler = Crawler(max_depth=max_depth, max_pages=max_pages,
                          max_workers=max_workers, delay=delay)
        prog = st.progress(0)
        stat = st.empty()

        def on_prog(crawled, queued, found, depth):
            try:
                prog.progress(min(crawled / max(max_pages, 1), 1.0))
                stat.markdown(f"**Crawled:** {crawled} | **Queued:** {queued} | "
                               f"**Found:** {found} | **Depth:** {depth}")
            except Exception:
                pass

        with st.spinner("🕷️ Crawling…"):
            try:
                discovered = crawler.crawl(dm, on_progress=on_prog)
            except Exception as e:
                st.error(f"❌ Crawl error: {e}")
                return

        prog.progress(1.0)
        stat.markdown(f"✅ Done — **{len(discovered)}** URLs discovered")

        # classify
        missing_rows = []
        covered_n = oos_n = filtered_n = 0

        for url, info in sorted(discovered.items()):
            try:
                if is_blocked_domain(url):
                    oos_n += 1
                    continue
                if URLMatcher.covered(url, all_http, regex_pats):
                    covered_n += 1
                    continue
                label, conf, pat = DocTypeClassifier.classify(url)
                if not DocTypeClassifier.in_scope(url, MODE):
                    if label == "Out of Scope":
                        oos_n += 1
                    else:
                        filtered_n += 1
                    continue
                mods = []
                if label in ("PDF", "Both", "Unclassified"):
                    mods.append("PDF")
                if label in ("HTML", "Both", "Unclassified"):
                    mods.append("HTML")
                missing_rows.append({
                    "domain": info.get("domain", ""),
                    "seed_url": info.get("seed", ""),
                    "missing_url": url,
                    "depth": info.get("depth", 0),
                    "doc_classification": label,
                    "confidence": conf,
                    "matched_pattern": (pat or "")[:60],
                    "source_module": " + ".join(mods) if mods else "Unclassified",
                })
            except Exception:
                continue

        set_ss('crawl_summary', {
            "total": len(discovered), "covered": covered_n,
            "missing": len(missing_rows), "oos": oos_n,
            "filtered": filtered_n, "mode": MODE,
        })
        set_ss('missing_df', build_df(missing_rows))

    # ------------------------------------------------------------------
    # RESULTS
    # ------------------------------------------------------------------
    cs = get_ss('crawl_summary')
    if not cs or not isinstance(cs, dict):
        return

    st.markdown("---")
    ml = cs.get('mode', 'Both')
    me = {"PDF": "🔴", "HTML": "🔵", "Both": "🟣"}.get(ml, "")
    st.subheader(f"📊 Results {me} [{ml} Mode]")

    x1, x2, x3, x4, x5 = st.columns(5)
    x1.metric("Discovered", cs.get("total", 0))
    x2.metric("Covered", cs.get("covered", 0))
    x3.metric("Missing (raw)", cs.get("missing", 0))
    x4.metric("Out of Scope", cs.get("oos", 0))
    x5.metric("Filtered", cs.get("filtered", 0))

    df = get_ss('missing_df')

    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        st.markdown(f"""
        <div class="success-box">
        <h3>✅ No Missing URLs ({ml} scope)</h3>
        <p>All discovered URLs are covered. 
        {cs.get('oos', 0)} Out of Scope, {cs.get('filtered', 0)} filtered by mode.</p>
        </div>""", unsafe_allow_html=True)
        return

    # ------------------------------------------------------------------
    # KEYWORD EXCLUSION
    # ------------------------------------------------------------------
    st.markdown("---")
    st.markdown("""<div class="exclude-box">
<b>🔑 Analyst Keyword Exclusion</b><br>
Enter keywords separated by <code>|</code> to remove matching URLs from the table.<br>
Hyphens, spaces, and underscores are interchangeable
(<code>email-alerts</code> = <code>email alerts</code> = <code>email_alerts</code>).<br><br>
<i>Examples: <code>sec-filings | email-alerts | privacy-notice | subsidiary |
credit-ratings | analyst-report | research/analyst</code></i>
</div>""", unsafe_allow_html=True)

    ek1, ek2, ek3 = st.columns([5, 1, 1])
    with ek1:
        excl_val = st.text_input(
            "Exclusion keywords",
            value=get_ss('exclusion_keywords') or "",
            placeholder="sec-filings | email-alerts | credit-ratings | analyst-report",
            key="excl_input",
            label_visibility="collapsed"
        )
    with ek2:
        if st.button("✂️ Apply", key="excl_apply", use_container_width=True):
            set_ss('exclusion_keywords', excl_val.strip())
            safe_rerun()
    with ek3:
        if st.button("🔄 Clear", key="excl_clear", use_container_width=True):
            set_ss('exclusion_keywords', None)
            safe_rerun()

    active_kw = get_ss('exclusion_keywords') or ""
    excl_re = build_exclusion_regex(active_kw)
    excl_n = 0

    if excl_re is not None:
        try:
            mask = df["missing_url"].apply(lambda u: url_matches_exclusion(u, excl_re))
            df_show = df[~mask].copy()
            excl_n = int(mask.sum())
            kws = [k.strip() for k in active_kw.split("|") if k.strip()]
            tags = " ".join(f'<span class="keyword-tag">✂️ {k}</span>' for k in kws)
            st.markdown(
                f"**Active: {len(kws)} keyword(s) | {excl_n} URLs removed** {tags}",
                unsafe_allow_html=True
            )
            if excl_n:
                with st.expander(f"👁️ {excl_n} excluded URLs", expanded=False):
                    for eu in df[mask]["missing_url"].tolist():
                        st.markdown(f'<a href="{eu}" target="_blank">{eu}</a>',
                                    unsafe_allow_html=True)
        except Exception as ex:
            st.warning(f"Exclusion error: {ex}")
            df_show = df.copy()
    else:
        df_show = df.copy()
        if active_kw:
            st.warning("⚠️ Could not parse exclusion keywords")

    st.markdown("---")
    eff = len(df_show)
    st.markdown(f"""<div class="warning-box">
<h3>⚠️ {eff} Missing URLs ({ml} scope)</h3>
<p>Exist on domain but not in your URL list. {excl_n} hidden by exclusions.</p>
</div>""", unsafe_allow_html=True)

    if df_show.empty:
        st.success("✅ All missing URLs excluded by keyword filters")
        return

    # ------------------------------------------------------------------
    # FILTERS
    # ------------------------------------------------------------------
    st.markdown("**🔧 Filters:**")
    f1, f2, f3, f4 = st.columns(4)

    try:
        d_opts = sorted(df_show["domain"].unique().tolist())
        d_sel = f1.multiselect("Domain", d_opts, default=d_opts, key="f_domain")
    except Exception:
        d_sel = []

    try:
        c_opts = sorted(df_show["doc_classification"].unique().tolist())
        c_sel = f2.multiselect("Type", c_opts, default=c_opts, key="f_class")
    except Exception:
        c_sel = []

    try:
        dep_opts = sorted(df_show["depth"].unique().tolist())
        dep_sel = f3.multiselect("Depth", dep_opts, default=dep_opts, key="f_depth")
    except Exception:
        dep_sel = []

    srch = f4.text_input("Search URL", value="", key="f_search",
                         placeholder="/news/ or /investor")

    try:
        filtered = df_show.copy()
        if d_sel:
            filtered = filtered[filtered["domain"].isin(d_sel)]
        if c_sel:
            filtered = filtered[filtered["doc_classification"].isin(c_sel)]
        if dep_sel:
            filtered = filtered[filtered["depth"].isin(dep_sel)]
        if srch.strip():
            filtered = filtered[
                filtered["missing_url"].str.contains(srch.strip(), case=False, na=False)
            ]
    except Exception:
        filtered = df_show.copy()

    st.markdown(f"**Showing {len(filtered)} of {eff}** "
                f"*(exclusions removed {cs.get('missing', 0) - eff})*")

    # TABLE
    if not filtered.empty:
        try:
            disp = filtered.copy()
            disp["missing_url"] = disp["missing_url"].apply(clickable)
            disp["seed_url"] = disp["seed_url"].apply(clickable)
            disp["doc_classification"] = disp["doc_classification"].apply(cls_badge)
            disp["source_module"] = disp["source_module"].apply(
                lambda x: x.replace("PDF", "🔴 PDF").replace("HTML", "🔵 HTML")
                if isinstance(x, str) else x
            )
            rmap = {
                "domain": "Domain", "seed_url": "Seed URL",
                "missing_url": "Missing URL", "depth": "Depth",
                "doc_classification": "Type", "confidence": "Conf.",
                "matched_pattern": "Pattern", "source_module": "Module",
            }
            disp = disp.rename(columns=rmap)
            fr = filtered.rename(columns=rmap)

            s_col = st.selectbox("Sort by", ["Domain", "Depth", "Type", "Module", "Missing URL"],
                                 key="sort_col")
            try:
                idx = fr.sort_values(s_col, key=lambda x: x.astype(str)).index
                disp = disp.loc[idx].reset_index(drop=True)
            except Exception:
                disp = disp.reset_index(drop=True)
            disp.index = disp.index + 1
            disp.index.name = "#"

            all_c = list(rmap.values())
            default_c = ["Domain", "Missing URL", "Depth", "Type", "Module"]
            with st.expander("📋 Columns", expanded=False):
                show_c = st.multiselect("Show", all_c, default=default_c, key="vis_cols")
            if not show_c:
                show_c = default_c

            # only show cols that exist
            show_c = [c for c in show_c if c in disp.columns]
            st.markdown(disp[show_c].to_html(escape=False, index=True),
                        unsafe_allow_html=True)
        except Exception as e:
            st.error(f"Table render error: {e}")
            st.dataframe(filtered)

    # ------------------------------------------------------------------
    # BREAKDOWNS
    # ------------------------------------------------------------------
    st.markdown("---")
    try:
        b1, b2, b3 = st.columns(3)
        with b1:
            st.markdown("### By Domain")
            t = df_show["domain"].value_counts().reset_index()
            t.columns = ["Domain", "Count"]
            st.table(t)
        with b2:
            st.markdown("### By Type")
            t = df_show["doc_classification"].value_counts().reset_index()
            t.columns = ["Type", "Count"]
            t["Type"] = t["Type"].apply(lambda x: f"{cls_emoji(x)} {x}")
            st.table(t)
        with b3:
            st.markdown("### By Module")
            t = df_show["source_module"].value_counts().reset_index()
            t.columns = ["Module", "Count"]
            st.table(t)
    except Exception:
        pass

    try:
        st.markdown("### By Depth")
        t = df_show["depth"].value_counts().sort_index().reset_index()
        t.columns = ["Depth", "Count"]
        st.bar_chart(t.set_index("Depth"))
    except Exception:
        pass

    # ------------------------------------------------------------------
    # DOWNLOADS
    # ------------------------------------------------------------------
    st.markdown("### 📥 Download")
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    rn = {"domain": "Domain", "seed_url": "Seed URL", "missing_url": "Missing URL",
          "depth": "Depth", "doc_classification": "Doc Type",
          "confidence": "Confidence", "matched_pattern": "Pattern", "source_module": "Module"}

    dl1, dl2, dl3, dl4 = st.columns(4)

    with dl1:
        try:
            st.download_button("📥 CSV (all)",
                               df_show.rename(columns=rn).to_csv(index=False),
                               f"missing_{MODE}_{ts}.csv", "text/csv",
                               use_container_width=True)
        except Exception:
            pass

    with dl2:
        try:
            st.download_button("📥 JSON (all)",
                               df_show.rename(columns=rn).to_json(orient="records", indent=2),
                               f"missing_{MODE}_{ts}.json", "application/json",
                               use_container_width=True)
        except Exception:
            pass

    with dl3:
        try:
            st.download_button("📥 URLs (TXT)",
                               "\n".join(df_show["missing_url"].tolist()),
                               f"missing_{MODE}_{ts}.txt", "text/plain",
                               use_container_width=True)
        except Exception:
            pass

    with dl4:
        try:
            if not filtered.empty:
                st.download_button(f"📥 Filtered ({len(filtered)})",
                                   filtered.rename(columns=rn).to_csv(index=False),
                                   f"filtered_{ts}.csv", "text/csv",
                                   use_container_width=True)
        except Exception:
            pass

    # ------------------------------------------------------------------
    # FOOTER
    # ------------------------------------------------------------------
    st.markdown("---")
    st.markdown(
        '<div style="text-align:center;color:#888;font-size:12px;padding:10px">'
        'Missing URL Identifier v2.2 | Dual Module | Keyword Exclusion | '
        's3.amazonaws.com blocked</div>',
        unsafe_allow_html=True
    )


if __name__ == "__main__":
    main()
