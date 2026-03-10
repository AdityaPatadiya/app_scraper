import json
import re
from datetime import datetime, timezone
from pathlib import Path

import requests

import bb_product as bb


LOCATIONS = [
    {"pincode": "700001", "name": "Kolkata (BBD Bagh)", "lat": "22.5726", "lng": "88.3639"},
    {"pincode": "110001", "name": "New Delhi (Connaught Place)", "lat": "28.6315", "lng": "77.2167"},
    {"pincode": "400001", "name": "Mumbai (Fort / GPO)", "lat": "18.9388", "lng": "72.8354"},
    {"pincode": "600001", "name": "Chennai (George Town / GPO)", "lat": "13.0837", "lng": "80.2707"},
]

OUT_DIR = Path(__file__).resolve().parent / "batch_outputs"


def extract_product_ids() -> list[int]:
    """Read bb_product.py and pull every PRODUCT_ID value, including commented examples."""
    script_path = Path(bb.__file__).resolve()
    text = script_path.read_text(encoding="utf-8")
    ids = [int(x) for x in re.findall(r"PRODUCT_ID\s*=\s*(\d+)", text)]
    deduped = []
    for pid in ids:
        if pid not in deduped:
            deduped.append(pid)
    return deduped


def save_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def run_one(location: dict, product_id: int) -> dict:
    bb.PINCODE = location["pincode"]
    bb.LAT = location["lat"]
    bb.LNG = location["lng"]
    bb.ADDRESS_ID = int(location["pincode"])
    bb.PRODUCT_ID = int(product_id)

    session = requests.Session()
    started_at = datetime.now(timezone.utc).isoformat()

    result = {
        "started_at_utc": started_at,
        "location": location,
        "product_id": product_id,
        "status": "unknown",
    }

    address_result = bb.run_set_address(session)
    if address_result is None:
        result["status"] = "address_failed"
        return result

    bb_vid, bb_aid = address_result
    product_data = bb.run_product_query(session, bb_vid, bb_aid)
    result["status"] = "ok" if product_data else "product_failed"
    result["response"] = product_data
    return result


def main() -> None:
    product_ids = extract_product_ids()
    if not product_ids:
        raise RuntimeError("No PRODUCT_ID entries found in bb_product.py")

    print(f"Detected product IDs from bb_product.py: {product_ids}")
    print(f"Output directory: {OUT_DIR}")

    summary = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "products": product_ids,
        "locations": LOCATIONS,
        "results": [],
    }

    for location in LOCATIONS:
        for product_id in product_ids:
            print("\n" + "=" * 72)
            print(f"Running pincode={location['pincode']} product_id={product_id}")
            print("=" * 72)
            try:
                result = run_one(location, product_id)
            except Exception as exc:
                result = {
                    "started_at_utc": datetime.now(timezone.utc).isoformat(),
                    "location": location,
                    "product_id": product_id,
                    "status": "exception",
                    "error": str(exc),
                }

            out_file = OUT_DIR / f"bb_{location['pincode']}_{product_id}.json"
            save_json(out_file, result)
            print(f"Saved: {out_file}")

            summary["results"].append(
                {
                    "pincode": location["pincode"],
                    "product_id": product_id,
                    "status": result.get("status"),
                    "file": str(out_file),
                }
            )

    summary_path = OUT_DIR / "summary.json"
    save_json(summary_path, summary)
    print(f"\nSummary saved: {summary_path}")


if __name__ == "__main__":
    main()
