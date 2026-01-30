from __future__ import annotations

import math
import re
from dataclasses import dataclass
from typing import Optional

import pandas as pd
import requests


RECENT_DIR = (
    "https://opendata.dwd.de/climate_environment/CDC/"
    "observations_germany/climate/hourly/air_temperature/recent/"
)
STATIONS_FILE = "TU_Stundenwerte_Beschreibung_Stationen.txt"
STATIONS_URL = RECENT_DIR + STATIONS_FILE


@dataclass(frozen=True)
class Station:
    station_id: int
    name: str
    state: str
    lat: float
    lon: float
    height_m: Optional[int] = None


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Great-circle distance between two points on Earth.
    """
    r = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return r * c


def load_stations() -> pd.DataFrame:
    """
    Load DWD TU station description file.

    The file is whitespace-separated, but 'Stationsname' can contain spaces.
    Format (per line):
      Stations_id von_datum bis_datum Stationshoehe geoBreite geoLaenge Stationsname... Bundesland Abgabe
    """
    r = requests.get(STATIONS_URL, timeout=90)
    r.raise_for_status()
    text = r.content.decode("latin-1", errors="replace")

    rows = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        # skip headers/separators
        if line.startswith("Stations_id") or line.startswith("-----------"):
            continue

        parts = line.split()
        # need at least: id, von, bis, height, lat, lon, state, abgabe (and maybe station name)
        if len(parts) < 8:
            continue

        station_id = parts[0]
        von_datum = parts[1]
        bis_datum = parts[2]
        height = parts[3]
        lat = parts[4]
        lon = parts[5]

        # last two tokens are state and abgabe
        bundesland = parts[-2]
        abgabe = parts[-1]

        # station name is whatever is between lon and bundesland
        station_name_tokens = parts[6:-2]
        station_name = " ".join(station_name_tokens).strip()

        rows.append(
            {
                "Stations_id": station_id,
                "von_datum": von_datum,
                "bis_datum": bis_datum,
                "Stationshoehe": height,
                "geoBreite": lat,
                "geoLaenge": lon,
                "Stationsname": station_name,
                "Bundesland": bundesland,
                "Abgabe": abgabe,
            }
        )

    df = pd.DataFrame(rows)

    # Clean + types
    df["Stations_id"] = pd.to_numeric(df["Stations_id"], errors="coerce")
    df["geoBreite"] = pd.to_numeric(df["geoBreite"], errors="coerce")
    df["geoLaenge"] = pd.to_numeric(df["geoLaenge"], errors="coerce")
    df["Stationshoehe"] = pd.to_numeric(df["Stationshoehe"], errors="coerce")

    df["Stationsname"] = df["Stationsname"].astype(str).str.strip()
    df["Bundesland"] = df["Bundesland"].astype(str).str.strip()

    df = df.dropna(subset=["Stations_id", "geoBreite", "geoLaenge"]).reset_index(drop=True)
    return df


def find_nearest_station(lat: float, lon: float) -> Station:
    df = load_stations()

    if df.empty:
        raise RuntimeError("Station metadata parsed as empty.")

    available_ids = list_recent_station_ids()
    if not available_ids:
        raise RuntimeError("Could not find any station ZIPs in the DWD recent directory listing.")

    # Keep only stations that have a recent ZIP
    df = df[df["Stations_id"].astype(int).isin(available_ids)].copy()
    if df.empty:
        raise RuntimeError("No stations from metadata have a corresponding recent ZIP file.")

    # Compute distance to each station
    df["distance_km"] = df.apply(
        lambda r: _haversine_km(lat, lon, float(r["geoBreite"]), float(r["geoLaenge"])),
        axis=1,
    )

    best = df.sort_values("distance_km").iloc[0]

    height_val = best["Stationshoehe"]
    height_m = int(height_val) if pd.notna(height_val) else None

    return Station(
        station_id=int(best["Stations_id"]),
        name=str(best["Stationsname"]),
        state=str(best["Bundesland"]),
        lat=float(best["geoBreite"]),
        lon=float(best["geoLaenge"]),
        height_m=height_m,
    )



def build_recent_zip_url(station_id: int) -> str:
    """
    recent zip naming pattern: stundenwerte_TU_00044_akt.zip
    """
    return RECENT_DIR + f"stundenwerte_TU_{station_id:05d}_akt.zip"

def list_recent_station_ids() -> set[int]:
    """
    Returns station IDs that have a TU 'recent' ZIP available.
    We parse the directory listing for files like:
      stundenwerte_TU_06254_akt.zip
    """
    r = requests.get(RECENT_DIR, timeout=60)
    r.raise_for_status()
    html = r.text

    ids = set()
    for m in re.finditer(r"stundenwerte_TU_(\d{5})_akt\.zip", html):
        ids.add(int(m.group(1)))
    return ids
