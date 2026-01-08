
import time
import os
import sys
import tempfile

import requests
import config


##################################################################

def iRailRequest(url:str, params=None, etag=None):
    
    IGNORE_404_ERROR = 1
    
    if params is None:
        params = {}
    
    params['format'] = 'json'
    params['land'] = 'en'
    
    headers = config.HEADERS
    
    if etag is not None:
        headers["If-None-Match"] = etag  # eTag for cache
        
    try:
        
        ########################################################
        response = requests.get(url, params=params, headers=headers, timeout=10)
        ########################################################
    
    except requests.exceptions.RequestException as e:
        # Network-level errors: DNS, TLS, connection issues, timeouts
        print('Network error:', e)
        raise SystemExit(1)
    
    # Handle HTTP response status codes:
    
    if response.status_code == 304:
        return None, None  # use cached data    
    
    if response.status_code == 429:
        # Rate limit exceeded (too many requests per second)
        print('Error: Too Many Requests (429). You are being rate limited.')
        raise SystemExit()

    if response.status_code == 404:
        if not IGNORE_404_ERROR:
            print('404:URL not found')
            raise SystemExit()
        else:
            return None        
    
    elif response.status_code >= 500:
        # Server-side error on iRail infrastructure
        print(f'Server error: HTTP {response.status_code}')
        #raise SystemExit()
        return None
        
    elif response.status_code != 200:
        # Any unexpected non-success status code
        print(f'Unexpected status code: HTTP {response.status_code}')
        raise SystemExit()
    
    # Parse JSON response
    try:
        data = response.json()
    except ValueError:
        print('Error: Response is not valid JSON')
        raise SystemExit()
    
    ###################################
    
    time.sleep(0.5)  # set timeout

    ###################################
    
    etag = response.headers.get("ETag")
    
    if etag is not None:
        pass
    
    return data

##################################################################

def get_lock_path(app_name="myapp"):
    if os.name == "posix":
        # Linux / Debian
        for path in ("/run", "/var/lock"):
            if os.path.isdir(path) and os.access(path, os.W_OK):
                return os.path.join(path, f"{app_name}.lock")
        # fallback
        return os.path.join(tempfile.gettempdir(), f"{app_name}.lock")
    else:
        # Windows
        return os.path.join(tempfile.gettempdir(), f"{app_name}.lock")

##################################################################



