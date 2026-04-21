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

# Aktualny produkcyjny WSDL (zweryfikowany 2026-04).
# Stary URL ws.stacjazpaczka.pl przestał działać po rebrandingu.
# Sandbox: https://sandbox-api.paczkawruchu.pl/WebServicePwR/WebServicePwR.asmx?wsdl
DEFAULT_WSDL = "https://api.paczkawruchu.pl/WebServicePwRProd/WebServicePwR.asmx?wsdl"

WSDL_URL = os.environ.get("ORLEN_WSDL", DEFAULT_WSDL)
PARTNER_ID = os.environ.get("ORLEN_PARTNER_ID")
PARTNER_KEY = os.environ.get("ORLEN_PARTNER_KEY")

OUT_FILE = Path("points.json")


def die(msg: str, code: int = 1) -> None:
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(code)


def fetch_raw_points():
    """Wywołuje GiveMeAllLocationWithAllDataWithZipCode.

    Uwaga: ta metoda nie wymaga credentials — punkty odbioru to dane
    publiczne. PartnerID/Key są potrzebne dopiero do awizacji przesyłek.
    """
    print(f"[fetch] Łączenie z {WSDL_URL}")
    try:
        client = Client(WSDL_URL)
    except TransportError as e:
        die(f"Nie udało się pobrać WSDL: {e}")

    print("[fetch] Wywołanie GiveMeAllLocationWithAllDataWithZipCode…")
    try:
        response = client.service.GiveMeAllLocationWithAllDataWithZipCode()
    except Fault as e:
        die(f"SOAP Fault: {e}")

    return response


def normalize_items(items: list) -> list[dict]:
    """Normalizuje punkty dla frontendu Leaflet."""
    points = []
    lat_field_seen = set()  # do debugu jeśli brak współrzędnych

    for loc in items:
        # Spróbuj kilku możliwych nazw pól dla lat/lng
        lat_raw = (
            loc.get('Latitude') or loc.get('Lat') or
            loc.get('latitude') or loc.get('lat')
        )
        lng_raw = (
            loc.get('Longitude') or loc.get('Lon') or loc.get('Lng') or
            loc.get('longitude') or loc.get('lon') or loc.get('lng')
        )
        if lat_raw is None or lng_raw is None:
            lat_field_seen.update(loc.keys())
            continue

        try:
            lat = float(str(lat_raw).replace(',', '.'))
            lng = float(str(lng_raw).replace(',', '.'))
        except (TypeError, ValueError):
            continue

        if not (49.0 <= lat <= 55.0 and 14.0 <= lng <= 24.5):
            continue

        code = str(loc.get('DestinationCode') or '').strip()
        if not code:
            continue

        street_parts = [
            str(loc.get('StreetName') or '').strip(),
            str(loc.get('BuildingNumber') or '').strip(),
        ]
        street = ' '.join(p for p in street_parts if p)

        points.append({
            "id": code,
            "name": str(loc.get('Description')
                        or loc.get('LocationDescription')
                        or loc.get('PointName')
                        or code).strip()[:80],
            "street": street[:80],
            "postCode": str(loc.get('Zipcode') or loc.get('ZipCode') or '').strip()[:10],
            "city": str(loc.get('City') or '').strip()[:60],
            "lat": round(lat, 6),
            "lng": round(lng, 6),
            "type": str(loc.get('PointType') or loc.get('LocationType') or '').strip()[:20],
            "hours": str(loc.get('DescriptionExt')
                         or loc.get('LocationDescriptionExt')
                         or loc.get('OpenHours') or '').strip()[:150],
        })

    if not points and lat_field_seen:
        print(f"[warn] Żaden punkt nie miał rozpoznanych współrzędnych.")
        print(f"[warn] Dostępne pola w przykładowym punkcie: {sorted(lat_field_seen)}")

    return points


def main() -> None:
    raw = fetch_raw_points()

    # Wyciągnij listę punktów z zagnieżdżonej struktury
    from zeep.helpers import serialize_object
    serialized = serialize_object(raw)

    items = []
    try:
        # raw._value_1._value_1 → lista dictów z kluczem 'LocationWithAllData2'
        inner = serialized['_value_1']['_value_1']
        for wrapper in inner:
            loc = wrapper.get('LocationWithAllData2') if isinstance(wrapper, dict) else None
            if loc:
                items.append(loc)
    except (KeyError, TypeError) as e:
        die(f"Nieoczekiwana struktura odpowiedzi: {e}")

    print(f"[info] Wypakowano {len(items)} punktów z odpowiedzi.")

    # Zapisz próbkę pierwszego punktu do debug_sample.json
    if items:
        import json as _json
        sample_path = Path("debug_sample.json")
        sample_path.write_text(
            _json.dumps(items[0], indent=2, ensure_ascii=False, default=str),
            encoding="utf-8"
        )
        print(f"[info] Próbka pierwszego punktu zapisana do {sample_path}")
        print(f"[info] Klucze pierwszego punktu: {list(items[0].keys())}")

    points = normalize_items(items)

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
