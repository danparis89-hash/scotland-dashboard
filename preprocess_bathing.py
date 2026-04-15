#!/usr/bin/env python3
"""Pre-process bathing water CSV into compact JSON for the dashboard."""
import csv
import json
import io
import re
from collections import defaultdict
from datetime import datetime

CSV_PATH = "data/Bathing Waters Application - All data 2005 - present.csv"
OUT_PATH = "data/bathing_water.json"

# Approximate WGS84 coordinates for all 89 Scottish designated bathing waters
COORDS = {
    "Aberdeen": (57.1497, -2.0803),
    "Aberdour (Silversands)": (56.0531, -3.2988),
    "Aberdour Harbour (Black Sands)": (56.0546, -3.3035),
    "Achmelvich": (58.1723, -5.3061),
    "Anstruther (Billow Ness)": (56.2231, -2.6988),
    "Arbroath (West Links)": (56.5571, -2.5963),
    "Ayr (South Beach)": (55.4560, -4.6350),
    "Balmedie": (57.2300, -2.0400),
    "Barassie Bay": (55.5530, -4.6400),
    "Brighouse Bay": (54.8000, -4.1200),
    "Broad Sands": (57.7100, -3.4300),
    "Broughty Ferry": (56.4650, -2.8700),
    "Burntisland": (56.0570, -3.2330),
    "Carnoustie": (56.5000, -2.7100),
    "Carrick": (55.3200, -4.8100),
    "Coldingham": (55.8990, -2.1350),
    "Collieston": (57.3410, -1.9540),
    "Crail (Roome Bay)": (56.2600, -2.6290),
    "Cruden Bay": (57.4070, -1.8560),
    "Cullen Bay": (57.6910, -2.8200),
    "Culzean": (55.3530, -4.7900),
    "Dhoon Bay": (54.8700, -4.0600),
    "Dores": (57.3960, -4.3200),
    "Dornoch": (57.8780, -4.0070),
    "Dunbar (Belhaven)": (55.9920, -2.5100),
    "Dunbar (East)": (56.0020, -2.5100),
    "Dunnet": (58.6100, -3.3800),
    "Elie (Harbour) and Earlsferry": (56.1890, -2.8190),
    "Elie (Ruby Bay)": (56.1900, -2.8100),
    "Ettrick Bay": (55.8700, -5.1800),
    "Eyemouth": (55.8720, -2.0870),
    "Findhorn": (57.6570, -3.6170),
    "Fisherrow Sands": (55.9410, -3.0700),
    "Fraserburgh (Philorth)": (57.6900, -1.9800),
    "Fraserburgh (Tiger Hill)": (57.6930, -2.0050),
    "Gairloch Beach": (57.7270, -5.6940),
    "Ganavan": (56.4200, -5.4800),
    "Girvan": (55.2430, -4.8620),
    "Gullane": (56.0380, -2.8290),
    "Heads of Ayr": (55.4300, -4.6600),
    "Inverboyndie": (57.6700, -2.5500),
    "Irvine": (55.6100, -4.7050),
    "Kinghorn (Harbour Beach)": (56.0700, -3.1730),
    "Kinghorn (Pettycur)": (56.0670, -3.1600),
    "Kingsbarns": (56.2810, -2.6470),
    "Kirkcaldy (Seafield)": (56.1200, -3.1450),
    "Largs (Pencil Beach)": (55.7930, -4.8660),
    "Leven": (56.1950, -2.9990),
    "Loch Morlich": (57.1660, -3.7050),
    "Longniddry": (55.9760, -2.8900),
    "Lossiemouth (East)": (57.7280, -3.2700),
    "Lower Largo": (56.2100, -2.9400),
    "Lunan Bay": (56.6700, -2.5000),
    "Lunderston Bay": (55.9200, -4.8700),
    "Luss Bay": (56.1000, -4.6370),
    "Machrihanish": (55.4300, -5.7400),
    "Maidens": (55.3360, -4.8150),
    "Millport Bay": (55.7520, -4.9270),
    "Monifieth": (56.4840, -2.8120),
    "Montrose": (56.7080, -2.4590),
    "Mossyard": (54.8400, -4.2100),
    "Nairn (Central)": (57.5880, -3.8650),
    "Nairn (East)": (57.5900, -3.8500),
    "North Berwick (Milsey Bay)": (56.0590, -2.7150),
    "North Berwick (West)": (56.0580, -2.7300),
    "Pease Bay": (55.9200, -2.3400),
    "Peterhead (Lido)": (57.5060, -1.7800),
    "Portobello (Central)": (55.9530, -3.1140),
    "Portobello (West)": (55.9530, -3.1250),
    "Prestwick": (55.5000, -4.6230),
    "Rockcliffe": (54.8700, -3.8200),
    "Rosehearty": (57.6980, -2.1080),
    "Rosemarkie": (57.5960, -4.1170),
    "Saltcoats/Ardrossan": (55.6380, -4.7820),
    "Sand Beach": (57.6870, -2.7700),
    "Sandyhills": (54.8700, -3.8800),
    "Seacliff": (56.0560, -2.6500),
    "Seamill": (55.6930, -4.8140),
    "Seton Sands": (55.9680, -2.9520),
    "Southerness": (54.8680, -3.6010),
    "St Andrews (East Sands)": (56.3360, -2.7860),
    "St Andrews (West Sands)": (56.3500, -2.8100),
    "Stonehaven": (56.9570, -2.2040),
    "Thorntonloch": (55.9580, -2.4200),
    "Thurso": (58.5960, -3.5260),
    "Troon (South Beach)": (55.5380, -4.6700),
    "Wardie Beach": (55.9700, -3.2300),
    "Whitesands": (54.9060, -3.6090),
    "Yellow Craig": (56.0430, -2.7750),
}


def parse_number(val):
    """Parse a numeric value, handling '<' prefixes."""
    if not val or not val.strip():
        return None
    val = val.strip()
    val = val.lstrip("<> ")
    try:
        return float(val)
    except ValueError:
        return None


def parse_date(val):
    """Parse date string like '17 May 2005'."""
    if not val or not val.strip():
        return None
    val = val.strip()
    for fmt in ("%d %B %Y", "%d %b %Y"):
        try:
            return datetime.strptime(val, fmt)
        except ValueError:
            continue
    return None


def classify_bathing_water(samples, medium):
    """
    Simplified Bathing Water Directive classification.
    Uses available samples (ideally 4 years, but works with whatever is available).
    Returns: Excellent, Good, Sufficient, or Poor.
    """
    ecoli_vals = [s["ecoli"] for s in samples if s["ecoli"] is not None]
    ie_vals = [s["ie"] for s in samples if s["ie"] is not None]

    if len(ecoli_vals) < 4 and len(ie_vals) < 4:
        return "Insufficient data"

    ecoli_vals.sort()
    ie_vals.sort()

    def percentile(vals, pct):
        if not vals:
            return 999999
        k = (len(vals) - 1) * pct / 100
        f = int(k)
        c = f + 1
        if c >= len(vals):
            return vals[f]
        return vals[f] + (k - f) * (vals[c] - vals[f])

    is_coastal = medium.lower() in ("coastal", "transitional")

    if is_coastal:
        ec95 = percentile(ecoli_vals, 95)
        ie95 = percentile(ie_vals, 95)
        ec90 = percentile(ecoli_vals, 90)
        ie90 = percentile(ie_vals, 90)

        if ec95 <= 250 and ie95 <= 100:
            return "Excellent"
        elif ec95 <= 500 and ie95 <= 200:
            return "Good"
        elif ec90 <= 500 and ie90 <= 185:
            return "Sufficient"
        else:
            return "Poor"
    else:
        ec90 = percentile(ecoli_vals, 90)
        ie90 = percentile(ie_vals, 90)

        if ec90 <= 500 and ie90 <= 200:
            return "Excellent"
        elif ec90 <= 1000 and ie90 <= 400:
            return "Good"
        elif ec90 <= 900 and ie90 <= 330:
            return "Sufficient"
        else:
            return "Poor"


def main():
    # Read UTF-16 CSV
    with open(CSV_PATH, "r", encoding="utf-16-le") as f:
        text = f.read()

    # Remove BOM if present
    text = text.lstrip("\ufeff")

    reader = csv.DictReader(io.StringIO(text), delimiter="\t")

    # Collect all samples per site
    sites = defaultdict(lambda: {"medium": None, "samples": []})

    for row in reader:
        name = row.get("Bathing water", "").strip()
        if not name:
            continue

        medium = row.get("Medium", "").strip()
        date = parse_date(row.get("Date", ""))
        if not date:
            continue

        ecoli = parse_number(row.get("E. coli", ""))
        ie = parse_number(row.get("IE", ""))

        sites[name]["medium"] = medium
        sites[name]["samples"].append({
            "date": date,
            "year": date.year,
            "ecoli": ecoli,
            "ie": ie,
        })

    # Build output
    output = []
    for name, data in sorted(sites.items()):
        coords = COORDS.get(name)
        if not coords:
            print(f"WARNING: No coordinates for '{name}'")
            continue

        samples = sorted(data["samples"], key=lambda s: s["date"])
        medium = data["medium"] or "Coastal"

        # Get years present
        years = sorted(set(s["year"] for s in samples))

        # Build yearly summaries
        yearly = {}
        for year in years:
            year_samples = [s for s in samples if s["year"] == year]
            ecoli_vals = [s["ecoli"] for s in year_samples if s["ecoli"] is not None]
            ie_vals = [s["ie"] for s in year_samples if s["ie"] is not None]

            yearly[year] = {
                "n": len(year_samples),
                "ecoli_mean": round(sum(ecoli_vals) / len(ecoli_vals)) if ecoli_vals else None,
                "ecoli_max": max(ecoli_vals) if ecoli_vals else None,
                "ie_mean": round(sum(ie_vals) / len(ie_vals)) if ie_vals else None,
                "ie_max": max(ie_vals) if ie_vals else None,
            }

        # Classification using last 4 years of data
        recent_years = years[-4:] if len(years) >= 4 else years
        recent_samples = [s for s in samples if s["year"] in recent_years]
        classification = classify_bathing_water(recent_samples, medium)

        # Latest season samples (most recent year with data)
        latest_year = years[-1] if years else None
        latest_samples = []
        if latest_year:
            for s in samples:
                if s["year"] == latest_year:
                    latest_samples.append({
                        "date": s["date"].strftime("%d %b %Y"),
                        "ecoli": s["ecoli"],
                        "ie": s["ie"],
                    })

        # Build yearly classification history (rolling 4-year windows)
        class_history = []
        for i, year in enumerate(years):
            window_years = [y for y in years if y <= year and y >= year - 3]
            window_samples = [s for s in samples if s["year"] in window_years]
            cls = classify_bathing_water(window_samples, medium)
            if cls != "Insufficient data":
                class_history.append({"year": year, "class": cls})

        output.append({
            "name": name,
            "lat": coords[0],
            "lng": coords[1],
            "medium": medium,
            "classification": classification,
            "latestYear": latest_year,
            "latestSamples": latest_samples,
            "classHistory": class_history[-10:],  # last 10 years of classification
            "yearly": {str(k): v for k, v in list(yearly.items())[-5:]},  # last 5 years of summaries
        })

    with open(OUT_PATH, "w") as f:
        json.dump(output, f, separators=(",", ":"))

    print(f"Wrote {len(output)} sites to {OUT_PATH}")
    # Show file size
    import os
    size = os.path.getsize(OUT_PATH)
    print(f"File size: {size:,} bytes ({size/1024:.1f} KB)")


if __name__ == "__main__":
    main()
