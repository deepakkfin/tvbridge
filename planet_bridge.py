# planet_bridge.py
from flask import Flask, jsonify, Response, request
import requests
import pandas as pd
import io

app = Flask(__name__)

# === CONFIG: put your Apps Script web app base URL here (without ?planet=) ===
# Example: "https://script.google.com/macros/s/AKfycbx.../exec"
APPS_SCRIPT_BASE_URL = "https://script.google.com/macros/s/AKfycbyvVGq3d8_bKD_DcXIUo6ZP6lsmUld4pz9tfpAgEqloMOZWIWKUspMhSkVVBJDm45Lrcg/exec"

# helper to fetch the apps script JSON for a planet
def fetch_planet_json(planet_name):
    url = f"{APPS_SCRIPT_BASE_URL}?planet={planet_name}"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    data = r.json()   # expects list of rows: [{"Date":"1994-01-01","Open":"...","Close":"..."}...]
    return data

# normalize to DataFrame and convert strings to numeric, ensure date format yyyy-mm-dd
def planet_dataframe(planet_name):
    data = fetch_planet_json(planet_name)
    if not isinstance(data, list) or len(data) == 0:
        return pd.DataFrame(columns=["Date","Open","High","Low","Close"])
    df = pd.DataFrame(data)
    # Normalize headers if they might be capitalized differently
    df.columns = [c.strip() for c in df.columns]
    # Ensure Date column exists
    if "Date" not in df.columns and "date" in df.columns:
        df.rename(columns={"date":"Date"}, inplace=True)
    # Convert Date
    df["Date"] = pd.to_datetime(df["Date"], dayfirst=False, errors='coerce').dt.strftime("%Y-%m-%d")
    # Ensure OHLC columns exist and are numeric
    for col in ["Open","High","Low","Close"]:
        if col not in df.columns:
            # try lowercase
            if col.lower() in df.columns:
                df.rename(columns={col.lower():col}, inplace=True)
        # if still missing, try to fallback to 'Close' or zeros
        if col not in df.columns:
            df[col] = None
        df[col] = pd.to_numeric(df[col], errors='coerce')
    # If OHLC are all NaN but Close exists in original input as string numbers, already handled above
    # Drop rows where Date or Close is NaN
    df = df.dropna(subset=["Date","Close"])
    df = df[["Date","Open","High","Low","Close"]]
    # For safety, sort by date ascending
    df = df.sort_values(by="Date").reset_index(drop=True)
    return df

@app.route("/planet/<planet_name>.json")
def planet_json(planet_name):
    df = planet_dataframe(planet_name)
    # convert back to JSON records
    recs = df.to_dict(orient="records")
    return jsonify(recs)

@app.route("/planet/<planet_name>.csv")
def planet_csv(planet_name):
    df = planet_dataframe(planet_name)
    # create CSV in memory
    csv_buf = df.to_csv(index=False)
    return Response(csv_buf, mimetype="text/csv",
                    headers={"Content-disposition": f"attachment; filename={planet_name}_LONG.csv"})

@app.route("/planet/<planet_name>")
def planet_default(planet_name):
    # default to JSON for convenience
    return planet_json(planet_name)

if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 5000))  # Render provides PORT env var
    app.run(host="0.0.0.0", port=port, debug=False)

from flask import request

@app.route("/config")
def config():
    return jsonify({
        "supports_search": True,
        "supports_group_request": False,
        "supports_marks": False,
        "supports_timescale_marks": False,
        "supports_time": True,
        "supported_resolutions": ["D"]
    })

@app.route("/symbols")
def symbols():
    symbol = request.args.get("symbol", "").upper()
    return jsonify({
        "name": symbol,
        "ticker": symbol,
        "description": f"{symbol} Planet Longitude",
        "type": "planet",
        "session": "24x7",
        "exchange": "Ephemeris",
        "listed_exchange": "Ephemeris",
        "timezone": "Etc/UTC",
        "minmov": 1,
        "pricescale": 100,
        "has_intraday": False,
        "supported_resolutions": ["D"],
        "has_no_volume": True
    })

@app.route("/history")
def history():
    symbol = request.args.get("symbol", "").upper()
    resolution = request.args.get("resolution", "D")
    from_ts = int(request.args.get("from", "0"))
    to_ts = int(request.args.get("to", "9999999999"))

    df = planet_dataframe(symbol.capitalize())  # e.g. Earth → Earth.csv
    df["time"] = pd.to_datetime(df["Date"]).astype("int64") // 10**9

    df = df[(df["time"] >= from_ts) & (df["time"] <= to_ts)]

    return jsonify({
        "s": "ok",
        "t": df["time"].tolist(),
        "o": df["Open"].tolist(),
        "h": df["High"].tolist(),
        "l": df["Low"].tolist(),
        "c": df["Close"].tolist()
    })

