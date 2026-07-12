import os
from flask import Flask, render_template, request, jsonify
from flask_cors import CORS
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import json

app = Flask(__name__)
CORS(app)

# NOAA API configuration
NOAA_API_BASE = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"
NOAA_STATIONS_METADATA_URL = "https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/stations.json?type=tidepredictions"
ALLOWED_STATES = ["NH", "ME", "MA"]

NOAA_REQUEST_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Referer': 'https://tidesandcurrents.noaa.gov/',
}

STATIC_NOAA_STATIONS = {
    "8423898": {"name": "Fort Point, New Hampshire", "state": "NH"},
    "8418150": {"name": "Portland, Maine", "state": "ME"},
    "8443970": {"name": "Boston, Massachusetts", "state": "MA"},
    "8419317": {"name": "Gloucester, Massachusetts", "state": "MA"},
    "8419319": {"name": "Newburyport, Massachusetts", "state": "MA"},
}

_station_cache = None

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
    try:
        response = requests.get(NOAA_API_BASE, params=params, headers=NOAA_REQUEST_HEADERS, timeout=10)
        if response.status_code != 200:
            return False
        data = response.json()
        return 'predictions' in data and bool(data['predictions'])
    except (requests.exceptions.RequestException, ValueError):
        return False


def fetch_noaa_stations():
    global _station_cache
    if _station_cache is not None:
        return _station_cache

    try:
        response = requests.get(NOAA_STATIONS_METADATA_URL, headers=NOAA_REQUEST_HEADERS, timeout=10)
        response.raise_for_status()
        data = response.json()
        state_rank = {state: idx for idx, state in enumerate(ALLOWED_STATES)}
        items = []

        for station in data.get('stations', []):
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

        today = datetime.now().date()
        check_date = today.strftime('%Y%m%d')

        valid_stations = {}
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_station = {
                executor.submit(station_has_predictions, station_id, check_date): (station_id, state, name)
                for _, name, station_id, state in items
            }
            for future in as_completed(future_to_station):
                station_id, state, name = future_to_station[future]
                try:
                    if future.result():
                        valid_stations[station_id] = {"name": name, "state": state}
                except Exception:
                    continue

        if not valid_stations:
            valid_stations = STATIC_NOAA_STATIONS.copy()

        _station_cache = valid_stations
    except requests.exceptions.RequestException as e:
        print(f"[tidecharts] WARNING: Failed to fetch NOAA station metadata: {e}. Falling back to static station list.")
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

    try:
        params = {
            'station': station,
            'begin_date': request_begin_date,
            'end_date': request_end_date,
            'product': 'predictions',
            'datum': 'MLLW',
            'units': units,
            'time_zone': 'gmt',
            'interval': 'hilo',
            'format': 'json'
        }
        
        response = requests.get(NOAA_API_BASE, params=params, headers=NOAA_REQUEST_HEADERS, timeout=10)
        try:
            data = response.json()
        except ValueError:
            return jsonify({'error': 'Invalid response from NOAA'}), 502
        
        if response.status_code != 200:
            error_message = data.get('error', {}).get('message') if isinstance(data, dict) else None
            if not error_message:
                error_message = data.get('message') if isinstance(data, dict) else response.text
            return jsonify({'error': f'NOAA request failed: {error_message}'}), response.status_code
        
        if 'predictions' in data:
            return jsonify({
                'station': stations[station],
                'station_id': station,
                'predictions': data['predictions']
            })
        else:
            return jsonify({'error': 'No tide predictions available for this station/date.'}), 404
            
    except requests.exceptions.RequestException as e:
        return jsonify({'error': f'NOAA request failed: {e}'}), 502

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
