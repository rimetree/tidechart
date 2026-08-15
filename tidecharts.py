import os
from flask import Flask, render_template, request, jsonify
from flask_cors import CORS
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import json
import time
from typing import Optional

app = Flask(__name__)
CORS(app)

# NOAA API configuration
NOAA_API_BASE = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"
NOAA_STATIONS_METADATA_URL = "https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/stations.json?type=tidepredictions"
ALLOWED_STATES = ["NH", "ME", "MA"]

# No custom headers needed - NOAA accepts the default python-requests user-agent.
# Spoofed browser headers (User-Agent, Referer) cause 403 responses from NOAA.
NOAA_REQUEST_HEADERS = {}

# Reuse a single requests.Session to reduce connection overhead
NOAA_SESSION = requests.Session()
NOAA_SESSION.headers.update(NOAA_REQUEST_HEADERS)

# NOAA intermittently returns a 403 (and occasionally 429/5xx) even for
# perfectly valid, unthrottled requests - it seems to be transient flakiness
# on their end rather than a real permissions problem, and simply retrying
# clears it almost every time. These statuses are treated as retryable.
NOAA_RETRYABLE_STATUSES = {403, 429, 500, 502, 503, 504}

STATIC_NOAA_STATIONS = {
    "8423898": {"name": "Fort Point, New Hampshire", "state": "NH"},
    "8418150": {"name": "Portland, Maine", "state": "ME"},
    "8443970": {"name": "Boston, Massachusetts", "state": "MA"},
    "8419317": {"name": "Gloucester, Massachusetts", "state": "MA"},
    "8419319": {"name": "Newburyport, Massachusetts", "state": "MA"},
}

_station_cache = None


def fetch_noaa_json(url, params=None, max_attempts=5, request_timeout=8, base_backoff=0.5, max_backoff=6):
    """
    GET a NOAA endpoint, retrying with exponential backoff on transient
    failures (network errors, and the retryable status codes above,
    including the occasional 403). Returns (data, error_message, status_code):
    on success error_message is None; on final failure data is None and
    error_message/status_code describe what went wrong.
    """
    last_error = None
    last_status = 502

    for attempt in range(max_attempts):
        try:
            response = NOAA_SESSION.get(url, params=params, timeout=request_timeout)
        except requests.exceptions.RequestException as e:
            last_error = str(e)
            last_status = 502
        else:
            if response.status_code == 200:
                try:
                    return response.json(), None, 200
                except ValueError:
                    last_error = 'Invalid (non-JSON) response from NOAA'
                    last_status = 502
            elif response.status_code in NOAA_RETRYABLE_STATUSES:
                last_error = f'NOAA returned HTTP {response.status_code}'
                last_status = response.status_code
            else:
                # Non-transient client error (e.g. bad params) - fail fast, no point retrying.
                try:
                    data = response.json()
                    message = data.get('error', {}).get('message') if isinstance(data, dict) else None
                    if not message:
                        message = data.get('message') if isinstance(data, dict) else response.text
                except ValueError:
                    message = response.text
                return None, message or f'NOAA request failed with status {response.status_code}', response.status_code

        if attempt < max_attempts - 1:
            sleep_for = min(base_backoff * (2 ** attempt), max_backoff)
            time.sleep(sleep_for)

    return None, last_error or 'NOAA request failed after multiple retries', last_status

@app.route('/')
def index():
    return render_template('index.html')

def station_has_predictions(station_id, date_to_check):
    params = {
        'station': station_id,
        'product': 'predictions',
        'datum': 'MLLW',
        'units': 'english',
        'time_zone': 'lst_ldt',
        'format': 'json',
        'interval': 'hilo',
        'begin_date': date_to_check,
        'end_date': date_to_check
    }
    # Use shorter timeouts/retries for per-station checks to avoid long blocking
    data, error_message, _ = fetch_noaa_json(NOAA_API_BASE, params=params, max_attempts=2, request_timeout=4)
    if error_message:
        return False
    return 'predictions' in data and bool(data['predictions'])


def fetch_noaa_stations():
    global _station_cache
    if _station_cache is not None:
        return _station_cache

    # Fetch stations metadata with conservative timeouts/retries so startup isn't long
    data, error_message, _ = fetch_noaa_json(NOAA_STATIONS_METADATA_URL, max_attempts=2, request_timeout=5)
    if error_message:
        print(f"[tidecharts] WARNING: Failed to fetch NOAA station metadata: {error_message}. Falling back to static station list.")
        _station_cache = STATIC_NOAA_STATIONS.copy()
        return _station_cache

    try:
        state_rank = {state: idx for idx, state in enumerate(ALLOWED_STATES)}
        items = []

        stations_list = data.get('stations', []) if isinstance(data, dict) else []
        print(f"[tidecharts] NOAA stations metadata returned {len(stations_list)} stations")

        for station in stations_list:
            state = station.get('state')
            if state not in ALLOWED_STATES:
                continue
            station_id = station.get('id')
            if not station_id:
                continue
            name = station.get('name', station_id)
            items.append((state_rank[state], name, station_id, state))

        if not items:
            _station_cache = STATIC_NOAA_STATIONS.copy()
            return _station_cache

        # Trim very large lists to a reasonable size to avoid long-running checks
        if len(items) > 200:
            print(f"[tidecharts] Trimming station list from {len(items)} to 200 to avoid long startup time")
            items = items[:200]

        today = datetime.now().date()
        check_date = today.strftime('%Y%m%d')

        valid_stations = {}

        # Avoid submitting thousands of futures at once (can OOM on small containers).
        # Process station checks in batches, keeping a small number of outstanding futures.
        batch_size = 50
        max_workers = 5
        print(f"[tidecharts] Stations metadata contains {len(items)} entries; checking in batches of {batch_size} with {max_workers} workers")
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for i in range(0, len(items), batch_size):
                batch = items[i:i+batch_size]
                future_to_station = {
                    executor.submit(station_has_predictions, station_id, check_date): (station_id, state, name)
                    for _, name, station_id, state in batch
                }
                for future in as_completed(future_to_station):
                    station_id, state, name = future_to_station[future]
                    try:
                        if future.result():
                            valid_stations[station_id] = {"name": name, "state": state}
                    except Exception:
                        # Fail individual station checks silently; proceed with others
                        continue

        if not valid_stations:
            valid_stations = STATIC_NOAA_STATIONS.copy()

        _station_cache = valid_stations
    except Exception as e:
        print(f"[tidecharts] WARNING: Failed to parse NOAA station metadata: {e}. Falling back to static station list.")
        _station_cache = STATIC_NOAA_STATIONS.copy()

    return _station_cache

@app.route('/api/stations')
def get_stations():
    return jsonify(fetch_noaa_stations())

@app.route('/api/tides', methods=['GET'])
def get_tides():
    """
    Fetch tide data from NOAA API
    Query params: station, begin_date, end_date, units (metric/english)
    """
    station = request.args.get('station')
    begin_date = request.args.get('begin_date', datetime.now().strftime('%Y%m%d'))
    end_date = request.args.get('end_date', begin_date)
    units = request.args.get('units', 'metric')
    
    stations = fetch_noaa_stations()
    if not station or station not in stations:
        return jsonify({'error': 'Invalid station'}), 400
    
    try:
        begin_dt = datetime.strptime(begin_date, '%Y%m%d')
        end_dt = datetime.strptime(end_date, '%Y%m%d')
        request_begin_date = (begin_dt - timedelta(days=1)).strftime('%Y%m%d')
        request_end_date = (end_dt + timedelta(days=1)).strftime('%Y%m%d')
    except ValueError:
        return jsonify({'error': 'Invalid date format'}), 400

    params = {
        'station': station,
        'begin_date': request_begin_date,
        'end_date': request_end_date,
        'product': 'predictions',
        'datum': 'MLLW',
        'units': units,
        'time_zone': 'lst_ldt',
        'interval': 'hilo',
        'format': 'json'
    }

    data, error_message, status_code = fetch_noaa_json(NOAA_API_BASE, params=params, max_attempts=5)

    if error_message:
        return jsonify({'error': f'NOAA request failed: {error_message}'}), status_code

    if 'predictions' in data:
        return jsonify({
            'station': stations[station],
            'station_id': station,
            'predictions': data['predictions']
        })
    else:
        return jsonify({'error': 'No tide predictions available for this station/date.'}), 404

@app.route('/api/favorites', methods=['GET', 'POST', 'DELETE'])
def manage_favorites():
    """
    Manage favorite stations (stored in session/local storage on client)
    """
    if request.method == 'GET':
        favorites = request.args.get('favorites', '[]')
        return jsonify(json.loads(favorites))
    return jsonify({'success': True})

if __name__ == '__main__':
    app.run(debug=True, port=5000)
