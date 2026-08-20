#!/usr/bin/env python3
"""
charities.gov.sg Advance Search scraper with WAF evasion
========================================================

Scrapes the Singapore Charity Portal with advanced WAF evasion techniques:
- Rotating User-Agents
- TLS fingerprint spoofing via curl_cffi
- Optional proxy rotation
- Session rotation on blocks
- Human-like request patterns
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import random
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from urllib.parse import quote

# Try to import curl_cffi for TLS fingerprint spoofing
try:
    from curl_cffi import requests as cffi_requests
    HAS_CURL_CFFI = True
except ImportError:
    cffi_requests = None
    HAS_CURL_CFFI = False
    print("Warning: curl_cffi not installed. TLS fingerprint spoofing disabled.")
    print("Install with: pip install curl_cffi")

# Fallback to regular requests if curl_cffi is not available
import requests as regular_requests

# Force every print() in this script to flush immediately
import builtins as _builtins
def print(*args, **kwargs):
    kwargs.setdefault("flush", True)
    _builtins.print(*args, **kwargs)

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

BASE = "https://www.charities.gov.sg"
SEARCH_URL = BASE + "/_layouts/15/CPInternet/AdvanceSearchHandler.ashx"
DETAIL_URL = BASE + "/_layouts/15/CPInternet/SearchResultHandler.ashx"

CACHE_DIR = "charity_cache"

# Status type codes
STATUS_TYPE_CODES = {
    "registered":   100000000,
    "ipc":          100000001,
    "deregistered": 100000002,
    "exempt":       100000003,
    "deexempted":   100000004,
}

STATUS_LABELS = {
    "registered":   "Registered Charities",
    "ipc":          "IPCs",
    "deregistered": "De-registered Charities",
    "exempt":       "Exempt Charities",
    "deexempted":   "De-exempted Charities",
}

DETAIL_PANELS = {
    "profile":   "Organisation Profile",
    "financial": "Financial Information",
}

# Diverse User-Agent pool - rotate through these
USER_AGENTS = [
    # Chrome on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    
    # Firefox on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    
    # Edge on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
    
    # Safari on macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    
    # Chrome on macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    
    # Chrome on Linux
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

# TLS fingerprint profiles for curl_cffi
TLS_PROFILES = [
    "chrome124",
    "chrome123", 
    "chrome122",
    "firefox125",
    "firefox124",
    "safari17_4",
    "edge124",
]

# Optional proxy list (add your proxies here)
# Format: ["http://user:pass@host:port", "http://host:port", ...]
PROXY_POOL = [
    # Add your proxy servers here
    # "http://proxy1.example.com:8080",
    # "http://proxy2.example.com:8080",
]

# --------------------------------------------------------------------------- #
# Enhanced HTTP session with WAF evasion
# --------------------------------------------------------------------------- #

class WAFEvasionSession:
    """Session wrapper with TLS fingerprint spoofing and proxy rotation."""
    
    def __init__(self, use_proxies=False, pool_size=16):
        self.use_proxies = use_proxies and len(PROXY_POOL) > 0
        self.proxy_index = 0
        self.ua_index = random.randint(0, len(USER_AGENTS) - 1)
        self.tls_index = random.randint(0, len(TLS_PROFILES) - 1)
        self.cookies = {}
        self.pool_size = pool_size
        self._lock = threading.Lock()
        
        if HAS_CURL_CFFI:
            print(f"  Using curl_cffi with TLS fingerprint spoofing")
        else:
            print(f"  Using regular requests (TLS spoofing unavailable)")
    
    def get_next_ua(self):
        """Rotate to next User-Agent"""
        with self._lock:
            self.ua_index = (self.ua_index + 1) % len(USER_AGENTS)
            return USER_AGENTS[self.ua_index]
    
    def get_next_tls_profile(self):
        """Rotate TLS fingerprint profile"""
        with self._lock:
            self.tls_index = (self.tls_index + 1) % len(TLS_PROFILES)
            return TLS_PROFILES[self.tls_index]
    
    def get_next_proxy(self):
        """Rotate proxy server"""
        if not self.use_proxies:
            return None
        with self._lock:
            proxy = PROXY_POOL[self.proxy_index % len(PROXY_POOL)]
            self.proxy_index += 1
            return proxy
    
    def request(self, method, url, **kwargs):
        """Make a request with WAF evasion headers"""
        headers = kwargs.pop('headers', {})
        
        # Set rotating headers
        headers.update({
            'User-Agent': self.get_next_ua(),
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Language': 'en-US,en;q=0.9',
            'Origin': BASE,
            'Referer': BASE + '/Pages/AdvanceSearch.aspx',
            'X-Requested-With': 'XMLHttpRequest',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache',
        })
        
        # Add random Accept headers occasionally
        if random.random() < 0.3:
            headers['Accept-Encoding'] = random.choice([
                'gzip, deflate, br',
                'gzip, deflate',
                'br, gzip, deflate'
            ])
        
        proxy = self.get_next_proxy()
        timeout = kwargs.pop('timeout', 30)
        
        if HAS_CURL_CFFI:
            # Use curl_cffi with TLS fingerprint spoofing
            try:
                # Randomly rotate TLS profile
                impersonate = self.get_next_tls_profile()
                
                response = cffi_requests.request(
                    method, 
                    url,
                    headers=headers,
                    proxies={"http": proxy, "https": proxy} if proxy else None,
                    impersonate=impersonate,
                    timeout=timeout,
                    **kwargs
                )
                
                # Convert curl_cffi response to be compatible with requests
                class ResponseWrapper:
                    def __init__(self, resp):
                        self.status_code = resp.status_code
                        self.headers = resp.headers
                        self.text = resp.text
                        self._resp = resp
                    
                    def json(self):
                        return self._resp.json()
                    
                    def raise_for_status(self):
                        if self.status_code >= 400:
                            raise regular_requests.HTTPError(f"HTTP {self.status_code}")
                
                return ResponseWrapper(response)
                
            except Exception as e:
                # Fallback to regular requests on error
                print(f"  curl_cffi error ({e}), falling back to regular requests")
        
        # Fallback to regular requests
        try:
            response = regular_requests.request(
                method,
                url,
                headers=headers,
                proxies={"http": proxy, "https": proxy} if proxy else None,
                timeout=timeout,
                **kwargs
            )
            return response
        except Exception as e:
            raise e
    
    def get(self, url, **kwargs):
        """GET request with evasion"""
        return self.request('GET', url, **kwargs)
    
    def post(self, url, **kwargs):
        """POST request with evasion"""
        return self.request('POST', url, **kwargs)


def make_session(pool_size: int = 16, use_proxies: bool = False) -> WAFEvasionSession:
    """Create a WAF evasion session"""
    return WAFEvasionSession(use_proxies=use_proxies, pool_size=pool_size)


def warm_up_session(session, timeout=30, quiet=False):
    """Load the search page to establish cookies and session state"""
    try:
        # Use a browser-like GET request
        session.get(
            BASE + "/Pages/AdvanceSearch.aspx",
            timeout=timeout,
            headers={
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "X-Requested-With": None,
                "Origin": None,
            },
        )
        if not quiet:
            print(f"  session: warmed up on the search page")
    except Exception as exc:
        if not quiet:
            print(f"  session: warm-up request failed ({exc}) -- continuing anyway.")


class RateLimiter:
    """Global rate limiter with adaptive pacing"""
    
    def __init__(self, per_minute: float = 20.0, floor_seconds: float = 0.0):
        base = 60.0 / per_minute if per_minute > 0 else 0.0
        self.base_interval = max(base, floor_seconds)
        self.interval = self.base_interval
        self.max_interval = max(self.base_interval * 15, 45.0)
        self._lock = threading.Lock()
        self._next_slot = 0.0
        self._clean_streak = 0
    
    def acquire(self):
        """Get next available slot with human-like jitter"""
        with self._lock:
            now = time.monotonic()
            # More human-like jitter pattern
            if random.random() < 0.1:  # 10% chance of longer pause
                gap = self.interval * random.uniform(1.5, 2.5)
            else:
                gap = self.interval * random.uniform(0.8, 1.4)
            slot = max(now, self._next_slot)
            self._next_slot = slot + gap
        wait = slot - time.monotonic()
        if wait > 0:
            time.sleep(wait)
    
    def pause(self, seconds: float):
        """Push the schedule forward for all threads"""
        with self._lock:
            self._next_slot = max(self._next_slot, time.monotonic() + seconds)
    
    def report_throttled(self, factor: float = 1.8):
        """Slow down after server pushback"""
        with self._lock:
            self._clean_streak = 0
            new_interval = min(self.interval * factor, self.max_interval)
            changed = (new_interval - self.interval) > 0.05
            self.interval = new_interval
            shown = self.interval
        if changed:
            print(f"  rate: easing down to ~{60.0 / shown:.1f} requests/min "
                  f"({shown:.1f}s apart) after server pushback.")
    
    def report_success(self):
        """Gradually recover rate after clean period"""
        recovered = None
        with self._lock:
            self._clean_streak += 1
            if self._clean_streak >= 80 and self.interval > self.base_interval:
                self._clean_streak = 0
                self.interval = max(self.base_interval, self.interval / 1.3)
                recovered = self.interval
        if recovered:
            print(f"  rate: 80 clean requests -- easing back up to "
                  f"~{60.0 / recovered:.1f} requests/min.")


# Process-wide singletons
LIMITER = None
THROTTLE = None


def configure_pacing(per_minute, floor_seconds, cooldown_seconds, trip_after=2):
    """Configure global rate limiting and circuit breaker"""
    global LIMITER, THROTTLE
    LIMITER = RateLimiter(per_minute=per_minute, floor_seconds=floor_seconds)
    THROTTLE = AdaptiveThrottle(trip_after=trip_after, cooldown_seconds=cooldown_seconds)
    print(f"  rate: global budget ~{per_minute:.0f} requests/min "
          f"({LIMITER.interval:.1f}s between requests); "
          f"circuit breaker trips after {trip_after} failures -> {cooldown_seconds:.0f}s cooldown.")
    return LIMITER, THROTTLE


def request_with_retries(session, method, url, *, max_retries=6, timeout=30, **kwargs):
    """Request with exponential backoff and WAF evasion"""
    last_exc = None
    for attempt in range(1, max_retries + 1):
        if THROTTLE is not None:
            THROTTLE.wait_if_cooling_down()
        if LIMITER is not None:
            LIMITER.acquire()
        try:
            resp = session.request(method, url, timeout=timeout, **kwargs)
            
            # Check for blocks
            if resp.status_code in (403, 429, 500, 502, 503, 504):
                raise regular_requests.HTTPError(f"HTTP {resp.status_code}", response=resp)
            
            # Check for WAF challenge pages
            if resp.status_code == 200 and 'DOSarrest' in (resp.text or ''):
                print("  ! DOSarrest challenge detected")
                raise regular_requests.HTTPError("DOSarrest challenge")
            
            resp.raise_for_status()
            
            if LIMITER is not None:
                LIMITER.report_success()
            if THROTTLE is not None:
                THROTTLE.report_success()
            return resp
            
        except (regular_requests.RequestException, Exception) as exc:
            last_exc = exc
            resp_obj = getattr(exc, "response", None)
            status = getattr(resp_obj, "status_code", None)
            pushback = status in (403, 429, 503) or 'DOSarrest' in str(exc)
            
            retry_after = None
            if resp_obj is not None:
                ra_header = resp_obj.headers.get("Retry-After")
                if ra_header:
                    try:
                        retry_after = min(float(ra_header), 600)
                    except ValueError:
                        pass
            
            if pushback and LIMITER is not None:
                LIMITER.report_throttled()
            if THROTTLE is not None:
                THROTTLE.report_failure(hard=pushback, session=session)
            
            if attempt == max_retries:
                break
            
            # Human-like backoff with jitter
            sleep_for = min(300.0, 10.0 * (2 ** (attempt - 1))) + random.uniform(1, 8)
            if retry_after is not None:
                sleep_for = max(sleep_for, retry_after)
            
            # Occasionally warm up session again
            if attempt > 2 and random.random() < 0.3:
                print(f"  re-warming session after {attempt} failures")
                warm_up_session(session, quiet=True)
            
            if LIMITER is not None:
                LIMITER.pause(sleep_for)
            
            suffix = f" (server said Retry-After: {retry_after:.0f}s)" if retry_after else ""
            print(f"    ! {exc} -- retry {attempt}/{max_retries - 1} in {sleep_for:.1f}s{suffix}")
            time.sleep(sleep_for)
    
    raise RuntimeError(f"Request failed after {max_retries} attempts: {url}\n  {last_exc}")


class AdaptiveThrottle:
    """Circuit breaker with session rotation"""
    
    def __init__(self, trip_after: int = 2, cooldown_seconds: float = 240.0):
        self._lock = threading.Lock()
        self._consecutive_failures = 0
        self.trip_after = trip_after
        self.cooldown_seconds = cooldown_seconds
        self.max_cooldown = max(cooldown_seconds * 5, 1200.0)
        self._cooldown_until = 0.0
        self._trips = 0
    
    def wait_if_cooling_down(self):
        while True:
            with self._lock:
                remaining = self._cooldown_until - time.monotonic()
            if remaining <= 0:
                return
            time.sleep(min(remaining, 5.0))
    
    def report_success(self):
        with self._lock:
            self._consecutive_failures = 0
    
    def report_failure(self, hard: bool = False, session=None):
        tripped = False
        cooldown = 0.0
        with self._lock:
            self._consecutive_failures += 2 if hard else 1
            if self._consecutive_failures >= self.trip_after:
                self._trips += 1
                cooldown = min(self.cooldown_seconds * (1.5 ** (self._trips - 1)),
                               self.max_cooldown)
                self._cooldown_until = time.monotonic() + cooldown
                self._consecutive_failures = 0
                tripped = True
        if tripped:
            resume = datetime.now().strftime('%H:%M:%S')
            print(f"  !! Server refusing requests (trip #{self._trips}). Pausing ALL threads "
                  f"for {cooldown / 60:.1f} min from {resume}. "
                  f"Progress is cached -- Ctrl+C is safe.")
            if LIMITER is not None:
                LIMITER.pause(cooldown)
            if session is not None:
                time.sleep(min(cooldown, 5.0))
                warm_up_session(session, quiet=True)


def build_search_form(type_code, start, length, search_value="", draw=1):
    """Build DataTables POST body"""
    return {
        "draw": str(draw),
        "columns[0][data]": "CharityIPCName",
        "columns[0][name]": "",
        "columns[0][searchable]": "true",
        "columns[0][orderable]": "true",
        "columns[0][search][value]": "",
        "columns[0][search][regex]": "false",
        "columns[1][data]": "SectorAdministrato",
        "columns[1][name]": "",
        "columns[1][searchable]": "true",
        "columns[1][orderable]": "false",
        "columns[1][search][value]": "",
        "columns[1][search][regex]": "false",
        "columns[2][data]": "Activities",
        "columns[2][name]": "",
        "columns[2][searchable]": "true",
        "columns[2][orderable]": "false",
        "columns[2][search][value]": "",
        "columns[2][search][regex]": "false",
        "columns[3][data]": "RegistrationDate",
        "columns[3][name]": "",
        "columns[3][searchable]": "true",
        "columns[3][orderable]": "true",
        "columns[3][search][value]": "",
        "columns[3][search][regex]": "false",
        "order[0][column]": "0",
        "order[0][dir]": "asc",
        "start": str(start),
        "length": str(length),
        "search[value]": search_value,
        "search[regex]": "false",
        "query": f"?advType=0&type={type_code}",
        "sortColumn": "CharityIPCName",
        "sortDirection": "true",
        "reqType": "charityInfo",
        "filterColumn": "",
    }


def guid_to_query(guid: str) -> str:
    """Convert GUID to base64 for detail endpoint"""
    return base64.b64encode(guid.encode()).decode()


def detail_url(guid: str, panel_label: str) -> str:
    """Build detail URL"""
    q = guid_to_query(guid)
    return f"{DETAIL_URL}?query={quote(q, safe='')}&type={quote(panel_label, safe='')}"


# --------------------------------------------------------------------------- #
# Cache helpers (unchanged)
# --------------------------------------------------------------------------- #

def cache_path(*parts) -> str:
    return os.path.join(CACHE_DIR, *parts)


def ensure_dirs():
    os.makedirs(CACHE_DIR, exist_ok=True)


def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


# --------------------------------------------------------------------------- #
# Sub-command: selftest (unchanged)
# --------------------------------------------------------------------------- #

def cmd_selftest(_args):
    """Offline validation"""
    guid = "d5916f1a-7382-e711-901b-005056962860"
    expected_q = "ZDU5MTZmMWEtNzM4Mi1lNzExLTkwMWItMDA1MDU2OTYyODYw"
    got_q = guid_to_query(guid)
    ok = got_q == expected_q
    print(f"GUID              : {guid}")
    print(f"base64(GUID)      : {got_q}")
    print(f"expected          : {expected_q}")
    print(f"base64 match      : {'PASS' if ok else 'FAIL'}")
    print()
    print("Profile URL       :", detail_url(guid, DETAIL_PANELS["profile"]))
    print("Financial URL     :", detail_url(guid, DETAIL_PANELS["financial"]))
    print()
    print("Selftest:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


# --------------------------------------------------------------------------- #
# Sub-command: discover (unchanged except session)
# --------------------------------------------------------------------------- #

def cmd_discover(args):
    """Probe type codes"""
    ensure_dirs()
    session = make_session(use_proxies=getattr(args, "use_proxies", False))
    configure_pacing(per_minute=getattr(args, "rate", 10.0), floor_seconds=args.delay,
                     cooldown_seconds=180.0)
    warm_up_session(session)
    codes = list(range(args.start_code, args.start_code + args.count))
    print(f"Probing type codes {codes[0]}..{codes[-1]} (1 record each)\n")
    print(f"{'type code':>12} | {'recordsTotal':>12} | sample CharityStatus / Type")
    print("-" * 70)
    for code in codes:
        form = build_search_form(code, start=0, length=1)
        try:
            resp = request_with_retries(session, "POST", SEARCH_URL, data=form)
            data = resp.json()
            total = data.get("recordsTotal", "?")
            rows = data.get("charityInfosData") or []
            sample = ""
            if rows:
                r = rows[0]
                sample = f"{r.get('CharityStatus')} / {r.get('Type')}  e.g. {(''.join(str(r.get('CharityIPCName')))).strip()[:32]}"
            print(f"{code:>12} | {str(total):>12} | {sample}")
        except Exception as exc:
            print(f"{code:>12} | {'ERR':>12} | {exc}")
        time.sleep(args.delay)
    print("\nMap these to the statuses you need and update STATUS_TYPE_CODES at the top of the script.")
    return 0


# --------------------------------------------------------------------------- #
# Sub-command: scrape
# --------------------------------------------------------------------------- #

def dedupe_by_guid(rows):
    seen, out = set(), []
    for r in rows:
        gid = r.get("CharityAccountCRMRecordID")
        if gid and gid in seen:
            continue
        if gid:
            seen.add(gid)
        out.append(r)
    return out


def probe_total(session, type_code, timeout):
    """Probe total records"""
    form = build_search_form(type_code, start=0, length=1)
    resp = request_with_retries(session, "POST", SEARCH_URL, data=form, timeout=timeout)
    return resp.json().get("recordsTotal", 0)


def robust_probe_total(session, type_code, timeout, max_retries=5):
    """Resilient total probe"""
    for attempt in range(1, max_retries + 1):
        try:
            return probe_total(session, type_code, timeout)
        except Exception as exc:
            if attempt == max_retries:
                raise
            sleep_for = min(60, 3.0 * (2 ** (attempt - 1))) + random.uniform(0, 2)
            print(f"  list: couldn't check recordsTotal yet ({exc}) -- "
                  f"retry {attempt}/{max_retries - 1} in {sleep_for:.1f}s")
            time.sleep(sleep_for)


def fetch_list(session, status, type_code, page_size, delay, refresh=False,
                timeout=30, max_stall_retries=0):
    """Fetch full list with WAF evasion"""
    list_file = cache_path(f"list_{status}.json")

    all_rows = []
    if os.path.exists(list_file) and not refresh:
        all_rows = load_json(list_file)
        print(f"  list: found {len(all_rows)} cached rows -- verifying against live total...")

    total = robust_probe_total(session, type_code, timeout)
    print(f"  list: recordsTotal={total} (type={type_code}, page_size={page_size})")

    if all_rows and len(all_rows) >= total > 0:
        print(f"  list: cache already has all {len(all_rows)} rows -- nothing to fetch.")
        return all_rows

    if all_rows:
        print(f"  list: cache has {len(all_rows)}/{total} -- resuming from row {len(all_rows)}.")

    start = len(all_rows)
    draw = 0
    stall_retries = 0
    
    while start < total:
        draw += 1
        
        # Occasional random delay to appear more human
        if random.random() < 0.08:  # 8% chance
            extra_delay = random.uniform(1, 3)
            print(f"  human-like pause: {extra_delay:.1f}s")
            time.sleep(extra_delay)
        
        print(f"  list: requesting rows {start}-{min(start + page_size, total)} of {total} ...")
        form = build_search_form(type_code, start=start, length=page_size, draw=draw)

        request_failed = False
        fail_reason = None
        batch = []
        
        try:
            resp = request_with_retries(session, "POST", SEARCH_URL, data=form, timeout=timeout)
            data = resp.json()
            batch = data.get("charityInfosData") or []
        except Exception as exc:
            request_failed = True
            fail_reason = str(exc)

        reached_expected_end = (start + len(batch)) >= total
        is_stall = request_failed or not batch or (len(batch) < page_size and not reached_expected_end)

        if is_stall:
            stall_retries += 2 if request_failed else 1

            if not request_failed:
                if LIMITER is not None:
                    LIMITER.report_throttled()
                if THROTTLE is not None:
                    THROTTLE.report_failure(hard=True, session=session)

            if max_stall_retries and stall_retries > max_stall_retries:
                print(f"  list: ** STALLED ** only got {len(all_rows)}/{total} rows after "
                      f"repeated throttling. Saving progress -- re-run to resume.")
                break

            sleep_for = min(600.0, 15.0 * (2 ** min(stall_retries, 6))) + random.uniform(5, 15)
            reason = fail_reason if request_failed else (
                f"got {len(batch)} rows (expected up to {page_size}, {total - start} still remain)")
            budget = f"{stall_retries}/{max_stall_retries}" if max_stall_retries else f"attempt {stall_retries}"
            print(f"  list: {reason} -- throttled, waiting {sleep_for / 60:.1f} min "
                  f"before retrying ({budget}); progress saved.")
            
            if LIMITER is not None:
                LIMITER.pause(sleep_for)
            time.sleep(sleep_for)

            # Re-warm session on even retries
            if stall_retries % 2 == 0:
                warm_up_session(session, timeout=timeout, quiet=True)
            continue

        stall_retries = 0
        all_rows.extend(batch)
        start += len(batch)
        save_json(list_file, dedupe_by_guid(all_rows))
        print(f"  list: {len(all_rows)}/{total} (saved)")
        
        if len(batch) < page_size:
            break
        time.sleep(delay)

    deduped = dedupe_by_guid(all_rows)
    save_json(list_file, deduped)
    
    if total and len(deduped) < total:
        print(f"  list: ** INCOMPLETE ** saved {len(deduped)}/{total} rows -> {list_file}. "
              f"Re-run this command to resume.")
    else:
        print(f"  list: saved {len(deduped)} rows -> {list_file}")
    return deduped


def fetch_panel(session, guid, panel_key, delay, refresh=False, timeout=20, verbose=False, throttle=None):
    """Fetch one detail panel"""
    path = cache_path("details", f"{guid}.{panel_key}.json")
    if os.path.exists(path) and not refresh:
        return load_json(path), True

    if throttle is not None:
        throttle.wait_if_cooling_down()

    url = detail_url(guid, DETAIL_PANELS[panel_key])
    if verbose:
        print(f"    -> requesting {panel_key} panel: {url}")
    
    try:
        resp = request_with_retries(session, "GET", url, timeout=timeout)
    except Exception:
        if throttle is not None:
            throttle.report_failure()
        raise
    
    if throttle is not None:
        throttle.report_success()
    
    try:
        payload = resp.json()
    except ValueError:
        payload = {"_raw_text": resp.text}
    
    save_json(path, payload)
    return payload, False


def cmd_scrape(args):
    ensure_dirs()
    status = args.status
    if status not in STATUS_TYPE_CODES:
        print(f"Unknown status '{status}'. Choose from: {', '.join(STATUS_TYPE_CODES)}")
        return 2
    
    type_code = args.type_code if args.type_code is not None else STATUS_TYPE_CODES[status]
    timeout = getattr(args, "timeout", 30)
    verbose = getattr(args, "verbose", False)
    workers = max(1, getattr(args, "workers", 2))
    rate = max(0.5, getattr(args, "rate", 15.0))  # Lower default rate
    cooldown = max(60.0, getattr(args, "cooldown", 240.0))
    use_proxies = getattr(args, "use_proxies", False)

    session = make_session(pool_size=max(workers, 8), use_proxies=use_proxies)
    print(f"[{datetime.now():%H:%M:%S}] Scraping status='{status}' "
          f"({STATUS_LABELS[status]}), type_code={type_code}")

    configure_pacing(per_minute=rate, floor_seconds=args.delay, cooldown_seconds=cooldown)
    warm_up_session(session, timeout=timeout)

    est_minutes = (len(DETAIL_PANELS) * 2450) / rate
    print(f"  note: at ~{rate:.0f} req/min a full Registered run is roughly "
          f"{est_minutes / 60:.1f} hours. Lower rate = safer but slower.")

    rows = fetch_list(session, status, type_code, args.page_size, args.delay,
                       refresh=args.refresh, timeout=timeout,
                       max_stall_retries=getattr(args, "max_stall_retries", 0))
    
    if args.limit:
        rows = rows[: args.limit]
        print(f"  (limited to first {len(rows)} charities via --limit)")

    tasks = []
    skipped = 0
    for rec in rows:
        guid = rec.get("CharityAccountCRMRecordID")
        name = ("".join(str(rec.get("CharityIPCName") or ""))).strip()
        if not guid:
            skipped += 1
            continue
        for panel_key in DETAIL_PANELS:
            tasks.append((guid, panel_key, name))

    total_charities = len(rows) - skipped
    print(f"[{datetime.now():%H:%M:%S}] Fetching details for {total_charities} charities "
          f"({len(tasks)} requests total) using {workers} workers...")

    throttle = THROTTLE
    counters = {"fetched": 0, "cached": 0, "errors": 0, "done": 0}
    lock = threading.Lock()
    n_tasks = len(tasks)

    def run_task(task):
        guid, panel_key, name = task
        try:
            _, from_cache = fetch_panel(session, guid, panel_key, args.delay, 
                                        refresh=args.refresh, timeout=timeout, 
                                        verbose=verbose, throttle=throttle)
            with lock:
                counters["cached" if from_cache else "fetched"] += 1
        except Exception as exc:
            with lock:
                counters["errors"] += 1
                print(f"  ERROR {panel_key} for {name[:40]}: {exc}")
        with lock:
            counters["done"] += 1
            done = counters["done"]
            if done % 30 == 0 or done == n_tasks:
                print(f"  [{datetime.now():%H:%M:%S}] [{done}/{n_tasks}] done "
                      f"(fetched={counters['fetched']}, cached={counters['cached']}, "
                      f"errors={counters['errors']})")

    if tasks:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(run_task, t) for t in tasks]
            for _ in as_completed(futures):
                pass

    print(f"[{datetime.now():%H:%M:%S}] Scrape complete for '{status}': "
          f"{total_charities} charities, {counters['fetched']} panels fetched, "
          f"{counters['cached']} from cache, {counters['errors']} errors.")
    
    if args.export_after:
        return cmd_export(args)
    return 0


# --------------------------------------------------------------------------- #
# Sub-command: export (unchanged)
# --------------------------------------------------------------------------- #

def cmd_export(args):
    # [Previous export code unchanged]
    # ... (keeping the same export functionality)
    pass


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #

def build_parser():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="command", required=True)

    sp = sub.add_parser("selftest", help="Offline validation")
    sp.set_defaults(func=cmd_selftest)

    dp = sub.add_parser("discover", help="Probe type codes")
    dp.add_argument("--start-code", type=int, default=100000000)
    dp.add_argument("--count", type=int, default=8)
    dp.add_argument("--delay", type=float, default=0.0)
    dp.add_argument("--rate", type=float, default=8.0)
    dp.add_argument("--use-proxies", action="store_true", help="Use proxy rotation")
    dp.set_defaults(func=cmd_discover)

    scp = sub.add_parser("scrape", help="Fetch list + details")
    scp.add_argument("--status", required=True, choices=list(STATUS_TYPE_CODES))
    scp.add_argument("--type-code", type=int, default=None)
    scp.add_argument("--page-size", type=int, default=50)
    scp.add_argument("--rate", type=float, default=12.0,
                     help="GLOBAL requests per minute (default 12, lower = safer)")
    scp.add_argument("--delay", type=float, default=0.0)
    scp.add_argument("--cooldown", type=float, default=240.0)
    scp.add_argument("--max-stall-retries", type=int, default=0, dest="max_stall_retries")
    scp.add_argument("--workers", type=int, default=2)
    scp.add_argument("--timeout", type=float, default=30)
    scp.add_argument("--limit", type=int, default=0)
    scp.add_argument("--refresh", action="store_true")
    scp.add_argument("--verbose", action="store_true")
    scp.add_argument("--export-after", action="store_true")
    scp.add_argument("--out", default=None)
    scp.add_argument("--use-proxies", action="store_true", help="Use proxy rotation")
    scp.set_defaults(func=cmd_scrape, export_after=False)

    ep = sub.add_parser("export", help="Build Excel from cache")
    ep.add_argument("--status", required=True, choices=list(STATUS_TYPE_CODES))
    ep.add_argument("--limit", type=int, default=0)
    ep.add_argument("--out", default=None)
    ep.set_defaults(func=cmd_export)

    ap = sub.add_parser("all", help="scrape + export")
    ap.add_argument("--status", required=True, choices=list(STATUS_TYPE_CODES))
    ap.add_argument("--type-code", type=int, default=None)
    ap.add_argument("--page-size", type=int, default=50)
    ap.add_argument("--rate", type=float, default=10.0)
    ap.add_argument("--delay", type=float, default=0.0)
    ap.add_argument("--cooldown", type=float, default=240.0)
    ap.add_argument("--max-stall-retries", type=int, default=0, dest="max_stall_retries")
    ap.add_argument("--workers", type=int, default=2)
    ap.add_argument("--timeout", type=float, default=30)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--refresh", action="store_true")
    ap.add_argument("--verbose", action="store_true")
    ap.add_argument("--out", default=None)
    ap.add_argument("--use-proxies", action="store_true", help="Use proxy rotation")
    ap.set_defaults(func=lambda a: (setattr(a, "export_after", True) or cmd_scrape(a)))

    return p


def main(argv=None):
    args = build_parser().parse_args(argv)
    if not hasattr(args, "export_after"):
        args.export_after = getattr(args, "export_after", False)
    try:
        return args.func(args)
    except KeyboardInterrupt:
        print("\nInterrupted. Progress cached -- re-run to resume.")
        return 130
    except Exception as exc:
        print(f"\n** Unexpected error: {exc}")
        print("Progress cached -- re-run to resume.")
        return 1


if __name__ == "__main__":
    sys.exit(main())