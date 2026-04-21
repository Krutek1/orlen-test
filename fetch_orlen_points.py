"""
Orlen Paczka points fetcher → points.json

Pobiera wszystkie punkty PUDO/APM przez SOAP API i zapisuje jako
odchudzony JSON dla frontendu (Leaflet).

Uruchamiany z GitHub Actions codziennie po 6:00 (po dziennej
aktualizacji bazy po stronie Orlenu).

ENV:
    ORLEN_PARTNER_ID   — Twój PartnerID z panelu Orlen Paczka
    ORLEN_PARTNER_KEY  — PartnerKey (klucz do API)
    ORLEN_WSDL         — (opcjonalnie) URL WSDL, jeśli zmieni się w przyszłości

Wymaga:  pip install zeep
"""
import json
import os
import sys
from pathlib import Path

from zeep import Client
from zeep.exceptions import Fault, TransportError

# UWAGA: Ten URL może wymagać aktualizacji — sprawdź w dokumentacji
# API_ORLENPaczka_v_1_24_001_PL.pdf (rozdział 4.x — endpoint WSDL).
# Stary endpoint sprzed rebrandu: ws.stacjazpaczka.pl
# Użyj sandbox URL w trybie testowym.
DEFAULT_WSDL = "https://ws.stacjazpaczka.pl/parcel_api2/endpoint?wsdl"

WSDL_URL = os.environ.get("ORLEN_WSDL", DEFAULT_WSDL)
PARTNER_ID = os.environ.get("ORLEN_PARTNER_ID")
PARTNER_KEY = os.environ.get("ORLEN_PARTNER_KEY")

OUT_FILE = Path("points.json")


def die(msg: str, code: int = 1) -> None:
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(code)


def fetch_raw_points():
    """Wywołuje GiveMeAllLocationWithAllDataWithZipcode."""
    if not PARTNER_ID or not PARTNER_KEY:
        die("Brak ORLEN_PARTNER_ID lub ORLEN_PARTNER_KEY w środowisku.")

    print(f"[fetch] Łączenie z {WSDL_URL}")
    try:
        client = Client(WSDL_URL)
    except TransportError as e:
        die(f"Nie udało się pobrać WSDL: {e}")

    print("[fetch] Wywołanie GiveMeAllLocationWithAllDataWithZipcode…")
    try:
        response = client.service.GiveMeAllLocationWithAllDataWithZipcode(
            PartnerID=PARTNER_ID,
            PartnerKey=PARTNER_KEY,
        )
    except Fault as e:
        die(f"SOAP Fault: {e}")

    return response


def normalize(raw) -> list[dict]:
    """
    Normalizuje odpowiedź SOAP do odchudzonego JSON-a dla frontendu.
    Pole names mogą się nieznacznie różnić — sprawdź rzeczywistą
    strukturę odpowiedzi przy pierwszym uruchomieniu i dostosuj.
    """
    points = []

    # API może zwracać listę bezpośrednio lub zagnieżdżoną
    iterable = raw
    if hasattr(raw, "LocationInfo"):
        iterable = raw.LocationInfo
    elif isinstance(raw, dict) and "LocationInfo" in raw:
        iterable = raw["LocationInfo"]

    if not iterable:
        die("API zwróciło pustą odpowiedź.")

    for p in iterable:
        try:
            lat = float(_get(p, "Latitude"))
            lng = float(_get(p, "Longitude"))
        except (TypeError, ValueError):
            continue

        # Sanity check — punkty poza granicami PL odrzucamy
        if not (49.0 <= lat <= 55.0 and 14.0 <= lng <= 24.5):
            continue

        code = str(_get(p, "LocationCode") or "").strip()
        if not code:
            continue

        points.append({
            "id": code,
            "name": str(_get(p, "LocationDescription")
                        or _get(p, "LocationName")
                        or code).strip()[:80],
            "street": str(_get(p, "LocationStreet") or "").strip()[:80],
            "postCode": str(_get(p, "LocationZipCode") or "").strip()[:10],
            "city": str(_get(p, "LocationCity") or "").strip()[:60],
            "lat": round(lat, 6),
            "lng": round(lng, 6),
            "type": str(_get(p, "LocationType") or "").strip()[:20],
            "hours": str(_get(p, "LocationDescriptionExt") or "").strip()[:150],
        })

    return points


def _get(obj, key, default=None):
    """SOAP zwraca zeep objects — pobieramy pole bezpiecznie."""
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def main() -> None:
    raw = fetch_raw_points()
    points = normalize(raw)

    if not points:
        die("Normalizacja zwróciła 0 punktów — sprawdź strukturę API.")

    print(f"[ok] Pobrano {len(points)} punktów.")

    OUT_FILE.write_text(
        json.dumps(points, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    size_kb = OUT_FILE.stat().st_size / 1024
    print(f"[ok] Zapisano {OUT_FILE} ({size_kb:.1f} KB)")


if __name__ == "__main__":
    main()
