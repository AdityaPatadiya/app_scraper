#!/usr/bin/env python3
"""
Extract product details from raw intercepted Instamart product payload files.

Input: raw product payload files (e.g. `product`, `response_1`, `discover_1`)
Output: structured product JSON.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


PERCENT_RE = re.compile(r"([0-9]{1,3})%\s*OFF", re.IGNORECASE)
UNIT_PRICE_RE = re.compile(r"\b[0-9]+(?:\.[0-9]+)?/[0-9]+(?:\.[0-9]+)?\s*[A-Za-z]+\b")


def get_first(values: Any, default: Any = None) -> Any:
    if isinstance(values, list) and values:
        return values[0]
    return default


def strip_html(html: str) -> str:
    text = re.sub(r"<br\s*/?>", "\n", html, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    return text.replace("&nbsp;", " ").strip()


def walk_nodes(node: Any) -> Iterable[Dict[str, Any]]:
    if isinstance(node, dict):
        yield node
        for value in node.values():
            if isinstance(value, list):
                for item in value:
                    yield from walk_nodes(item)
            else:
                yield from walk_nodes(value)
    elif isinstance(node, list):
        for item in node:
            yield from walk_nodes(item)


def read_varint(buf: bytes, i: int) -> Tuple[Optional[int], int]:
    value = 0
    shift = 0
    start = i
    for _ in range(10):
        if i >= len(buf):
            return None, start
        b = buf[i]
        i += 1
        value |= (b & 0x7F) << shift
        if not (b & 0x80):
            return value, i
        shift += 7
    return None, start


def extract_inr_values(raw: bytes) -> List[int]:
    values: List[int] = []
    i = 0
    while True:
        idx = raw.find(b"INR", i)
        if idx == -1:
            break
        probe = idx + 3
        if probe < len(raw) and raw[probe] == 0x10:
            value, end = read_varint(raw, probe + 1)
            if value is not None and 0 < value < 1_000_000:
                values.append(value)
                i = end
                continue
        i = idx + 3
    return values


def parse_price_blob(blob: str) -> Dict[str, Any]:
    raw = blob.encode("latin-1", errors="ignore")
    text = raw.decode("utf-8", errors="ignore")
    inr_values = extract_inr_values(raw)
    discount = PERCENT_RE.search(text)
    unit_price = UNIT_PRICE_RE.search(text)

    mrp = None
    selling_price = None
    savings = None
    if len(inr_values) >= 2:
        first, second = inr_values[0], inr_values[1]
        mrp = max(first, second)
        selling_price = min(first, second)
        if len(inr_values) >= 3 and 0 < inr_values[2] <= mrp:
            savings = inr_values[2]
        elif mrp >= selling_price:
            savings = mrp - selling_price
    elif len(inr_values) == 1:
        selling_price = inr_values[0]

    return {
        "currency": "INR",
        "mrp": mrp,
        "selling_price": selling_price,
        "savings": savings,
        "discount_text": discount.group(0) if discount else None,
        "unit_price_text": unit_price.group(0) if unit_price else None,
        "raw_inr_values": inr_values,
    }


def parse_rating_blob(blob: str) -> Dict[str, Any]:
    clean = re.sub(r"[\x00-\x1f]+", " ", blob)
    nums = re.findall(r"\b[0-9]+(?:\.[0-9]+)?k?\b", clean, flags=re.IGNORECASE)

    rating_value: Optional[float] = None
    rating_count_text: Optional[str] = None
    rating_count: Optional[int] = None

    for tok in nums:
        if tok.lower().endswith("k"):
            rating_count_text = tok
            try:
                rating_count = int(float(tok[:-1]) * 1000)
            except ValueError:
                rating_count = None
            continue
        try:
            value = float(tok)
        except ValueError:
            continue
        if rating_value is None and 0 <= value <= 5:
            rating_value = value
        elif rating_count is None:
            rating_count = int(value)
            rating_count_text = tok

    return {
        "rating": rating_value,
        "rating_count_text": rating_count_text,
        "rating_count_estimate": rating_count,
    }


def parse_delivery_blob(blob: str) -> Dict[str, Any]:
    clean = re.sub(r"[\x00-\x1f]+", " ", blob)
    m = re.search(r"\b(\d+)\s*(MINS?|MINUTES?)\b", clean, flags=re.IGNORECASE)
    if m:
        return {"delivery_time_value": int(m.group(1)), "delivery_time_unit": m.group(2).upper()}
    return {"delivery_time_value": None, "delivery_time_unit": None}


def parse_max_order_limit(blob: str) -> Optional[int]:
    clean = re.sub(r"[\x00-\x1f]+", " ", blob)
    m = re.search(r"Only\s+(\d+)\s+unit\(s\)", clean, flags=re.IGNORECASE)
    if m:
        return int(m.group(1))
    return None


def parse_seller_html(html: str) -> Dict[str, Any]:
    text = strip_html(html)

    seller_name = None
    fssai = None
    address = None
    customer_care = None

    for line in [ln.strip() for ln in text.splitlines() if ln.strip()]:
        if line.lower().startswith("seller name:"):
            seller_name = line.split(":", 1)[1].strip()
        elif line.lower().startswith("fssai number:"):
            fssai = line.split(":", 1)[1].strip()
        elif line.lower().startswith("address:"):
            address = line.split(":", 1)[1].strip()
        elif line.lower().startswith("customer care:"):
            customer_care = line.split(":", 1)[1].strip()

    return {
        "seller_name": seller_name,
        "fssai_number": fssai,
        "address": address,
        "customer_care": customer_care,
        "raw_text": text,
    }


def extract_seller_details(root: Dict[str, Any]) -> Dict[str, Any]:
    for node in walk_nodes(root):
        title = get_first(node.get("1"))
        html = get_first(node.get("2"))
        if title == "Seller Details" and isinstance(html, str):
            return parse_seller_html(html)
    return {
        "seller_name": None,
        "fssai_number": None,
        "address": None,
        "customer_care": None,
        "raw_text": None,
    }


def extract_typed_payloads(root: Dict[str, Any], type_url_suffix: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for node in walk_nodes(root):
        type_value = get_first(node.get("1"))
        payload = get_first(node.get("2"))
        if not isinstance(type_value, str):
            continue
        if not type_value.endswith(type_url_suffix):
            continue
        if isinstance(payload, dict):
            out.append(payload)
    return out


def extract_info_cards(root: Dict[str, Any]) -> List[Dict[str, Any]]:
    cards = []
    payloads = extract_typed_payloads(root, "swiggy.im.v1.InfoCard")
    for p in payloads:
        title = get_first(p.get("1"))
        html = get_first(p.get("2"))
        cards.append(
            {
                "title": title,
                "html": html,
                "text": strip_html(html) if isinstance(html, str) else None,
                "raw_fields": p,
            }
        )
    return cards


def parse_event_payload(payload: str) -> Dict[str, Any]:
    cleaned = re.sub(r"[\x00-\x1f]+", " ", payload).strip()
    data: Dict[str, Any] = {"raw": payload}
    key_match = re.search(r"\b(variantAttributeArray|skuAvailable|sla|storeIDflag|ppid|oosFlag)\b", cleaned)
    if key_match:
        data["key"] = key_match.group(1)
        remainder = cleaned[key_match.end() :].strip()
        if remainder:
            data["value_text"] = remainder
    variant_match = re.search(r"(\[[^\]]+\])", cleaned)
    if variant_match:
        try:
            data["value_json"] = json.loads(variant_match.group(1))
        except json.JSONDecodeError:
            pass
    return data


def is_product_node(node: Dict[str, Any]) -> bool:
    name = get_first(node.get("1"))
    brand = get_first(node.get("2"))
    details = get_first(node.get("5"))
    return (
        isinstance(name, str)
        and isinstance(brand, str)
        and isinstance(details, dict)
        and isinstance(get_first(details.get("1")), str)
        and isinstance(get_first(details.get("2")), str)
    )


def parse_product_node(node: Dict[str, Any]) -> Dict[str, Any]:
    details = get_first(node.get("5"), {})
    price_blob = get_first(details.get("9"), "")
    rating_blob = get_first(details.get("27"), "")
    delivery_blob = get_first(details.get("28"), "")
    max_limit_blob = get_first(details.get("13"), "")

    parsed = {
        "name": get_first(node.get("1")),
        "brand": get_first(node.get("2")),
        "item_id": get_first(details.get("1")),
        "spin_id": get_first(details.get("2")),
        "quantity": get_first(details.get("3")),
        "description": get_first(details.get("21")),
        "category": get_first(details.get("36")) or get_first(details.get("10")),
        "pod_id": get_first(details.get("19")),
        "max_order_limit_units": parse_max_order_limit(max_limit_blob) if isinstance(max_limit_blob, str) else None,
        "price": parse_price_blob(price_blob) if isinstance(price_blob, str) else {},
        "rating": parse_rating_blob(rating_blob) if isinstance(rating_blob, str) else {},
        "delivery": parse_delivery_blob(delivery_blob) if isinstance(delivery_blob, str) else {},
        "image_ids": details.get("6", []) if isinstance(details.get("6"), list) else [],
        "raw_fields": {
            "product_node": node,
            "details_node": details,
        },
    }

    # Fallback to SLA from analytics blob if delivery widget is missing.
    if parsed["delivery"].get("delivery_time_value") is None:
        event_node = get_first(node.get("8"), {})
        if isinstance(event_node, dict):
            for payload in event_node.get("7", []):
                if not isinstance(payload, str):
                    continue
                m = re.search(r"\b(\d+)\s*(MINS?|MINUTES?)\b", payload, flags=re.IGNORECASE)
                if m:
                    parsed["delivery"] = {
                        "delivery_time_value": int(m.group(1)),
                        "delivery_time_unit": m.group(2).upper(),
                    }
                    break

    event_node = get_first(node.get("8"), {})
    if isinstance(event_node, dict):
        event_payloads = event_node.get("7", []) if isinstance(event_node.get("7"), list) else []
        parsed["event_payloads"] = [
            parse_event_payload(payload) for payload in event_payloads if isinstance(payload, str)
        ]
        parsed["event_metadata"] = {
            "event_name": get_first(event_node.get("1")),
            "event_context_json": get_first(event_node.get("2")),
            "impression": get_first(event_node.get("4")),
            "click_event": get_first(event_node.get("5")),
        }
    else:
        parsed["event_payloads"] = []
        parsed["event_metadata"] = {}

    return parsed


def dedupe_products(products: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    out = []
    for product in products:
        key = product.get("item_id") or f"{product.get('name')}|{product.get('quantity')}"
        if key in seen:
            continue
        seen.add(key)
        out.append(product)
    return out


def validate_current_product(product: Dict[str, Any]) -> List[Dict[str, str]]:
    errors: List[Dict[str, str]] = []
    checks = [
        ("price.selling_price", product.get("price", {}).get("selling_price")),
        ("price.mrp", product.get("price", {}).get("mrp")),
        ("delivery.delivery_time_value", product.get("delivery", {}).get("delivery_time_value")),
        ("rating.rating", product.get("rating", {}).get("rating")),
        ("quantity", product.get("quantity")),
    ]
    for field, value in checks:
        if value is None:
            errors.append(
                {
                    "field": field,
                    "message": f"Missing {field}; keep as null and optionally fall back to OCR/UI text.",
                }
            )
    return errors


def build_product_output(decoded: Dict[str, Any]) -> Dict[str, Any]:
    root = decoded.get("decoded_raw_tree", {})
    if not isinstance(root, dict):
        return {
            "source_file": decoded.get("source_file"),
            "status": "error",
            "error": "decoded_raw_tree is missing or invalid",
            "current_product": None,
            "seller_details": None,
            "other_products": [],
        }

    products = [parse_product_node(node) for node in walk_nodes(root) if is_product_node(node)]
    products = dedupe_products(products)

    current_product = products[0] if products else None
    other_products = products[1:] if len(products) > 1 else []
    seller_details = extract_seller_details(root)
    info_cards = extract_info_cards(root)
    other_information = next((c for c in info_cards if c.get("title") == "Other Information"), None)
    description_card = next((c for c in info_cards if c.get("title") == "Description"), None)

    typed_nodes = []
    for node in walk_nodes(root):
        t = get_first(node.get("1"))
        payload = get_first(node.get("2"))
        if isinstance(t, str) and t.startswith("type.googleapis.com/"):
            typed_nodes.append({"type_url": t, "payload": payload})

    result = {
        "source_file": decoded.get("source_file"),
        "status": "ok" if current_product else "partial",
        "decoded_meta": {
            "format": decoded.get("format"),
            "summary": decoded.get("summary"),
            "type_urls": decoded.get("type_urls", []),
        },
        "current_product": current_product,
        "seller_details": seller_details,
        "description": description_card,
        "other_information": other_information,
        "other_products_count": len(other_products),
        "other_products": other_products,
        "all_data": {
            "all_products_count": len(products),
            "all_products": products,
            "info_cards": info_cards,
            "typed_nodes": typed_nodes,
            "image_paths": decoded.get("image_paths", []),
            "uuids": decoded.get("uuids", []),
            "colors": decoded.get("colors", []),
            "indexes": {
                "by_item_id": {p.get("item_id"): p for p in products if p.get("item_id")},
                "by_spin_id": {p.get("spin_id"): p for p in products if p.get("spin_id")},
            },
        },
        "fallback": {
            "missing_fields": validate_current_product(current_product) if current_product else [],
            "notes": [
                "If any field is null, use decoded_raw_text regex fallback or OCR from screenshot.",
                "For app_location captures that are text-transformed, seller and exact prices may be incomplete.",
            ],
        },
    }
    return result


def decode_raw_input(input_path: Path, require_decoded_tree: bool = True) -> Dict[str, Any]:
    decoder_script = Path(__file__).with_name("decode_instamart.py")
    if not decoder_script.exists():
        raise RuntimeError("decode_instamart.py not found next to extract_product_details.py")

    with tempfile.TemporaryDirectory(prefix="instamart_decode_") as tmp:
        result = subprocess.run(
            ["python3", str(decoder_script), str(input_path), "--out-dir", tmp],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(f"Decoding failed: {result.stderr or result.stdout}")
        out_path = Path(tmp) / f"{input_path.name}.structured.json"
        if not out_path.exists():
            raise RuntimeError("Decoder ran but structured output file was not generated")
        data = json.loads(out_path.read_text(encoding="utf-8"))
        if require_decoded_tree and "decoded_raw_tree" not in data:
            raise RuntimeError(
                "Decoded file does not contain decoded_raw_tree. "
                "Use protobuf binary product captures for full structured output."
            )
        return data


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract details from one raw product payload file")
    parser.add_argument("input", help="Path to raw product payload file (e.g. product, response_1, discover_1)")
    parser.add_argument(
        "--out",
        default=None,
        help="Output JSON path. Defaults to <input>.details.json",
    )
    args = parser.parse_args()

    in_path = Path(args.input)
    decoded_any = decode_raw_input(in_path, require_decoded_tree=True)
    output = build_product_output(decoded_any)
    out_default = in_path.with_suffix(in_path.suffix + ".product_details.json")

    out_path = Path(args.out) if args.out else out_default
    out_path.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    print(out_path)


if __name__ == "__main__":
    main()
