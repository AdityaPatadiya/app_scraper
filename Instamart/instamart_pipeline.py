#!/usr/bin/env python3
"""
instamart_pipeline.py

Full end-to-end pipeline:
  1. Run first curl  -> save raw protobuf to RESPONSE_FILE_1
  2. Decode RESPONSE_FILE_1 -> extract spin_id
  3. Run second curl (with spin_id) -> save raw protobuf to RESPONSE_FILE_2
  4. Decode RESPONSE_FILE_2 -> extract structured product details
  5. Save final JSON to OUTPUT_JSON

Edit the CONFIG section below before running.
"""

from __future__ import annotations

import ast
import json
import re
import struct
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


# ---------------------------------------------------------------------------
# CONFIG - edit these before running
# ---------------------------------------------------------------------------

# LAT = "19.014795"
# LNG = "72.845451"
# PRODUCT_ID = "4RL2JFCWQG"
# PAGE_NAME = "product"

# LAT = "12.9789"
# LNG = "77.591333"
# PRODUCT_ID = "4RL2JFCWQG"
# PAGE_NAME = "product"

# LAT = "12.9789"
# LNG = "77.591333"
# PRODUCT_ID = "Q3J3C2YRB4"
# PAGE_NAME = "product"

LAT = "12.9789"
LNG = "77.591333"
PRODUCT_ID = "YBL1T3JO24"
PAGE_NAME = "product"



# Output paths (relative to this script's directory)
RESPONSE_FILE_1 = "response"       # first curl output (no spin_id)
RESPONSE_FILE_2 = "response_1"     # second curl output (with spin_id)
OUTPUT_JSON = "decoded/response1.json"

BASE_DIR = Path(__file__).parent


# ---------------------------------------------------------------------------
# STEP 1: curl helpers
# ---------------------------------------------------------------------------

COMMON_HEADERS = [
    ("screen-name", "store-item-view-instamart"),
    ("user-agent", "Swiggy-Android"),
    ("content-type", "application/json; charset=utf-8"),
    ("version-code", "1693"),
    ("app-version", "4.103.3"),
    ("latitude", LAT),
    ("longitude", LNG),
    ("os-version", "13"),
    ("accessibility_enabled", "false"),
    ("current-latitude", "12.9128764"),
    ("current-longitude", "77.6521342"),
    ("x-network-quality", "MODERATE"),
    ("accept-encoding", "gzip"),
    ("faw-flags", "1354"),
    ("pl-version", "127"),
    ("cache-control", "no-store"),
    ("accept", "application/x-protobuf"),
]


def _header_flags(headers: list[tuple[str, str]]) -> list[str]:
    flags: list[str] = []
    for name, value in headers:
        flags += ["-H", f"{name}: {value}"]
    return flags


def fetch_without_spin_id(out_path: Path) -> None:
    """First call: no preferred_spin_id."""
    url = (
        f"https://disc.swiggy.com/api/v1/instamart/product"
        f"?lat={LAT}&lng={LNG}&product.product_id={PRODUCT_ID}&page_name={PAGE_NAME}"
    )
    cmd = (
        ["curl", "--compressed", "-X", "GET", url]
        + _header_flags(COMMON_HEADERS)
        + ["-o", str(out_path)]
    )
    print(f"[1/4] Fetching product page (no spin_id) -> {out_path.name}")
    result = subprocess.run(cmd, capture_output=True)
    if result.returncode != 0:
        stderr = result.stderr.decode("utf-8", errors="replace")
        raise RuntimeError(f"First curl failed (exit {result.returncode}):\n{stderr}")
    print(f"      Saved {out_path.stat().st_size} bytes")


def fetch_with_spin_id(spin_id: str, out_path: Path) -> None:
    """Second call: includes the spin_id obtained from the first response."""
    url = (
        f"https://disc.swiggy.com/api/v1/instamart/product"
        f"?lat={LAT}&lng={LNG}&product.product_id={PRODUCT_ID}"
        f"&page_name={PAGE_NAME}&product.preferred_spin_id={spin_id}"
    )
    cmd = (
        ["curl", "--compressed", "-X", "GET", url]
        + _header_flags(COMMON_HEADERS)
        + ["-o", str(out_path)]
    )
    print(f"[3/4] Fetching product detail (spin_id={spin_id}) -> {out_path.name}")
    result = subprocess.run(cmd, capture_output=True)
    if result.returncode != 0:
        stderr = result.stderr.decode("utf-8", errors="replace")
        raise RuntimeError(f"Second curl failed (exit {result.returncode}):\n{stderr}")
    print(f"      Saved {out_path.stat().st_size} bytes")


# ---------------------------------------------------------------------------
# Protobuf pure-Python decoder (from decode_instamart.py)
# ---------------------------------------------------------------------------

def _pb_read_varint(buf: bytes, pos: int) -> Tuple[int, int]:
    result = 0
    shift = 0
    while pos < len(buf):
        b = buf[pos]
        pos += 1
        result |= (b & 0x7F) << shift
        if not (b & 0x80):
            return result, pos
        shift += 7
    raise ValueError("truncated varint")


def _pb_escape_bytes(b: bytes) -> str:
    out = []
    for byte in b:
        if byte == ord('"'):
            out.append('\\"')
        elif byte == ord('\\'):
            out.append('\\\\')
        elif byte == ord('\n'):
            out.append('\\n')
        elif byte == ord('\r'):
            out.append('\\r')
        elif byte == ord('\t'):
            out.append('\\t')
        elif 32 <= byte < 127:
            out.append(chr(byte))
        else:
            out.append(f'\\{byte:03o}')
    return ''.join(out)


def _pb_decode_message(buf: bytes, indent: int = 0) -> Optional[List[str]]:
    lines: List[str] = []
    pos = 0
    end = len(buf)
    prefix = "  " * indent
    while pos < end:
        try:
            tag, pos = _pb_read_varint(buf, pos)
        except ValueError:
            return None
        field_no = tag >> 3
        wtype = tag & 0x7
        if field_no == 0 or wtype not in (0, 1, 2, 5):
            return None
        if wtype == 0:
            try:
                val, pos = _pb_read_varint(buf, pos)
            except ValueError:
                return None
            lines.append(f"{prefix}{field_no}: {val}")
        elif wtype == 1:
            if pos + 8 > end:
                return None
            val = struct.unpack_from("<Q", buf, pos)[0]
            pos += 8
            lines.append(f"{prefix}{field_no}: 0x{val:016x}")
        elif wtype == 2:
            try:
                length, pos = _pb_read_varint(buf, pos)
            except ValueError:
                return None
            if length < 0 or pos + length > end:
                return None
            chunk = buf[pos: pos + length]
            pos += length
            if length == 0:
                lines.append(f'{prefix}{field_no}: ""')
            else:
                sub = _pb_decode_message(chunk, indent + 1)
                if sub is not None:
                    lines.append(f"{prefix}{field_no} {{")
                    lines.extend(sub)
                    lines.append(f"{prefix}}}")
                else:
                    lines.append(f'{prefix}{field_no}: "{_pb_escape_bytes(chunk)}"')
        elif wtype == 5:
            if pos + 4 > end:
                return None
            val = struct.unpack_from("<I", buf, pos)[0]
            pos += 4
            lines.append(f"{prefix}{field_no}: 0x{val:08x}")
    if pos != end:
        return None
    return lines


def decode_raw_binary_python(raw_bytes: bytes) -> Optional[str]:
    lines = _pb_decode_message(raw_bytes)
    if lines is None:
        return None
    return "\n".join(lines)


def try_decode_raw_protobuf(raw_bytes: bytes) -> Optional[str]:
    try:
        result = subprocess.run(
            ["protoc", "--decode_raw"],
            input=raw_bytes,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
    except FileNotFoundError:
        return None
    if result.returncode != 0:
        return None
    return result.stdout.decode("utf-8", errors="replace")


# ---------------------------------------------------------------------------
# Protobuf text -> Python dict tree
# ---------------------------------------------------------------------------

def parse_scalar_value(raw_value: str) -> Any:
    value = raw_value.strip()
    if value.startswith('"') and value.endswith('"'):
        try:
            return ast.literal_eval(value)
        except Exception:
            return value
    if value.startswith("0x"):
        try:
            as_int = int(value, 16)
            as_float = struct.unpack("<f", as_int.to_bytes(4, "little"))[0]
            return {"hex": value, "uint32": as_int, "float32": as_float}
        except Exception:
            return value
    if re.fullmatch(r"-?\d+", value):
        try:
            return int(value)
        except ValueError:
            return value
    if re.fullmatch(r"-?\d+\.\d+", value):
        try:
            return float(value)
        except ValueError:
            return value
    return value


def _add_field(node: Dict[str, Any], field_no: str, value: Any) -> None:
    if field_no not in node:
        node[field_no] = [value]
    else:
        node[field_no].append(value)


def parse_decode_raw_text(decoded_text: str) -> Dict[str, Any]:
    root: Dict[str, Any] = {}
    stack: List[Dict[str, Any]] = [root]
    open_re = re.compile(r"^(\d+)\s*\{$")
    scalar_re = re.compile(r"^(\d+):\s*(.+)$")
    for raw_line in decoded_text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line == "}":
            if len(stack) > 1:
                stack.pop()
            continue
        m = open_re.match(line)
        if m:
            child: Dict[str, Any] = {}
            _add_field(stack[-1], m.group(1), child)
            stack.append(child)
            continue
        m = scalar_re.match(line)
        if m:
            _add_field(stack[-1], m.group(1), parse_scalar_value(m.group(2)))


    return root


def decode_binary_file(path: Path) -> Optional[Dict[str, Any]]:
    """Decode a raw protobuf binary file into a structured dict tree."""
    raw_bytes = path.read_bytes()

    # Try JSON first (unlikely for protobuf responses, but just in case)
    try:
        return json.loads(raw_bytes.decode("utf-8", errors="replace"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        pass

    decoded_text = try_decode_raw_protobuf(raw_bytes)
    if decoded_text is None:
        decoded_text = decode_raw_binary_python(raw_bytes)
    if not decoded_text:
        return None

    tree = parse_decode_raw_text(decoded_text)
    return {
        "decoded_raw_tree": tree,
        "decoded_raw_text": decoded_text,
        "format": "protobuf_binary_decode_raw",
        "source_file": str(path),
    }


# ---------------------------------------------------------------------------
# Product extraction (from extract_product_details.py)
# ---------------------------------------------------------------------------

PERCENT_RE = re.compile(r"([0-9]{1,3})%\s*OFF", re.IGNORECASE)
UNIT_PRICE_RE = re.compile(r"\b[0-9]+(?:\.[0-9]+)?/[0-9]+(?:\.[0-9]+)?\s*[A-Za-z]+\b")


def _get_first(values: Any, default: Any = None) -> Any:
    if isinstance(values, list) and values:
        return values[0]
    return default


def _strip_html(html: str) -> str:
    text = re.sub(r"<br\s*/?>", "\n", html, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    return text.replace("&nbsp;", " ").strip()


def _walk_nodes(node: Any) -> Iterable[Dict[str, Any]]:
    if isinstance(node, dict):
        yield node
        for value in node.values():
            if isinstance(value, list):
                for item in value:
                    yield from _walk_nodes(item)
            else:
                yield from _walk_nodes(value)
    elif isinstance(node, list):
        for item in node:
            yield from _walk_nodes(item)


def _read_varint(buf: bytes, i: int) -> Tuple[Optional[int], int]:
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


def _extract_inr_values(raw: bytes) -> List[int]:
    values: List[int] = []
    i = 0
    while True:
        idx = raw.find(b"INR", i)
        if idx == -1:
            break
        probe = idx + 3
        if probe < len(raw) and raw[probe] == 0x10:
            value, end = _read_varint(raw, probe + 1)
            if value is not None and 0 < value < 1_000_000:
                values.append(value)
                i = end
                continue
        i = idx + 3
    return values


def _parse_price_blob(blob: str) -> Dict[str, Any]:
    raw = blob.encode("latin-1", errors="ignore")
    text = raw.decode("utf-8", errors="ignore")
    inr_values = _extract_inr_values(raw)
    discount = PERCENT_RE.search(text)
    unit_price = UNIT_PRICE_RE.search(text)
    mrp = selling_price = savings = None
    if len(inr_values) >= 2:
        mrp = max(inr_values[0], inr_values[1])
        selling_price = min(inr_values[0], inr_values[1])
        if len(inr_values) >= 3 and 0 < inr_values[2] <= mrp:
            savings = inr_values[2]
        else:
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


def _parse_rating_blob(blob: str) -> Dict[str, Any]:
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
                pass
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


def _parse_delivery_blob(blob: str) -> Dict[str, Any]:
    clean = re.sub(r"[\x00-\x1f]+", " ", blob)
    m = re.search(r"\b(\d+)\s*(MINS?|MINUTES?)\b", clean, flags=re.IGNORECASE)
    if m:
        return {"delivery_time_value": int(m.group(1)), "delivery_time_unit": m.group(2).upper()}
    return {"delivery_time_value": None, "delivery_time_unit": None}


def _parse_max_order_limit(blob: str) -> Optional[int]:
    clean = re.sub(r"[\x00-\x1f]+", " ", blob)
    m = re.search(r"Only\s+(\d+)\s+unit\(s\)", clean, flags=re.IGNORECASE)
    return int(m.group(1)) if m else None


# ---------------------------------------------------------------------------
# Structured dict parsers (protobuf fully decoded into nested dicts)
# ---------------------------------------------------------------------------

def _money_amount(obj: Any) -> Optional[int]:
    """Extract integer amount from {1: ["INR"], 2: [amount]} money dict."""
    if not isinstance(obj, dict):
        return None
    amt = _get_first(obj.get("2"))
    if isinstance(amt, int):
        return amt
    try:
        return int(amt)
    except (TypeError, ValueError):
        return None


def _parse_price_dict(price_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Parse price when field 9 is a structured dict (fully decoded protobuf).

    Layout (field numbers inside the price message):
      1 -> MRP money object        {1: [currency], 2: [amount]}
      2 -> selling price object
      3 -> unit price text string  e.g. "55/piece"
      4 -> savings money object
      5 -> discount label object   {1: ["25% OFF"], ...}
    """
    mrp = _money_amount(_get_first(price_dict.get("1")))
    selling_price = _money_amount(_get_first(price_dict.get("2")))
    savings = _money_amount(_get_first(price_dict.get("4")))
    unit_price_text = _get_first(price_dict.get("3"))
    discount_obj = _get_first(price_dict.get("5"))
    discount_text = _get_first(discount_obj.get("1")) if isinstance(discount_obj, dict) else None

    if savings is None and mrp is not None and selling_price is not None and mrp >= selling_price:
        savings = mrp - selling_price

    return {
        "currency": "INR",
        "mrp": mrp,
        "selling_price": selling_price,
        "savings": savings,
        "discount_text": discount_text,
        "unit_price_text": unit_price_text if isinstance(unit_price_text, str) else None,
        "raw_inr_values": [v for v in [mrp, selling_price, savings] if v is not None],
    }


def _parse_delivery_dict(delivery_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Parse delivery when field 28 is a structured dict.

    Layout:
      1 -> time value string  e.g. "12"
      2 -> unit string        e.g. "MINS"
    """
    time_val = _get_first(delivery_dict.get("1"))
    unit = _get_first(delivery_dict.get("2"))
    try:
        time_val = int(time_val)
    except (TypeError, ValueError):
        time_val = None
    return {
        "delivery_time_value": time_val,
        "delivery_time_unit": unit.upper() if isinstance(unit, str) else None,
    }


def _parse_max_order_limit_dict(limit_dict: Dict[str, Any]) -> Optional[int]:
    """Parse max order limit when field 13 is a structured dict.

    Layout:
      1 -> count int
      2 -> message string  e.g. "Only 3 unit(s) of this item..."
    """
    count = _get_first(limit_dict.get("1"))
    if isinstance(count, int) and count > 0:
        return count
    # fallback: parse the message string
    msg = _get_first(limit_dict.get("2"))
    if isinstance(msg, str):
        m = re.search(r"Only\s+(\d+)\s+unit\(s\)", msg, flags=re.IGNORECASE)
        if m:
            return int(m.group(1))
    return None


def _recover_qty_from_pb_dict(qty_dict: Dict[str, Any]) -> Optional[str]:
    """Recover a quantity string (e.g. '1 Piece x 3') that was mistakenly decoded
    as a protobuf sub-message by the raw decoder.

    The string '1 Piece x 3' (11 bytes) happens to parse as a valid protobuf:
      field 6 (wire-type 1, 64-bit): bytes " Piece x"
      field 4 (wire-type 0, varint): 51  (= ord('3'))
    We reconstruct by re-encoding those fields back to bytes.
    """
    field_6 = _get_first(qty_dict.get("6"))
    field_4 = _get_first(qty_dict.get("4"))
    if isinstance(field_4, int) and isinstance(field_6, str) and field_6.startswith("0x"):
        try:
            tag6 = (6 << 3) | 1   # 0x31 = '1' in ASCII
            tag4 = (4 << 3) | 0   # 0x20 = ' ' in ASCII
            mid_bytes = int(field_6, 16).to_bytes(8, "little")
            raw = bytes([tag6]) + mid_bytes + bytes([tag4, field_4])
            s = raw.decode("ascii")
            if re.search(r"[Pp]iece|[Pp]kg|[Gg]m|[Kk]g|[Ll]\b|[Mm]l\b", s):
                return s
        except Exception:
            pass
    return None


def _parse_seller_html(html: str) -> Dict[str, Any]:
    text = _strip_html(html)
    seller_name = fssai = address = customer_care = None
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


def _extract_seller_details(root: Dict[str, Any]) -> Dict[str, Any]:
    for node in _walk_nodes(root):
        title = _get_first(node.get("1"))
        html = _get_first(node.get("2"))
        if title == "Seller Details" and isinstance(html, str):
            return _parse_seller_html(html)
    return {"seller_name": None, "fssai_number": None, "address": None, "customer_care": None, "raw_text": None}


def _extract_info_cards(root: Dict[str, Any]) -> List[Dict[str, Any]]:
    cards = []
    for node in _walk_nodes(root):
        t = _get_first(node.get("1"))
        payload = _get_first(node.get("2"))
        if not isinstance(t, str) or not t.endswith("swiggy.im.v1.InfoCard"):
            continue
        if not isinstance(payload, dict):
            continue
        title = _get_first(payload.get("1"))
        html = _get_first(payload.get("2"))
        cards.append({
            "title": title,
            "html": html,
            "text": _strip_html(html) if isinstance(html, str) else None,
        })
    return cards


def _is_product_node(node: Dict[str, Any]) -> bool:
    name = _get_first(node.get("1"))
    brand = _get_first(node.get("2"))
    details = _get_first(node.get("5"))
    return (
        isinstance(name, str)
        and isinstance(brand, str)
        and isinstance(details, dict)
        and isinstance(_get_first(details.get("1")), str)
        and isinstance(_get_first(details.get("2")), str)
    )


def _parse_event_payload(payload: str) -> Dict[str, Any]:
    cleaned = re.sub(r"[\x00-\x1f]+", " ", payload).strip()
    data: Dict[str, Any] = {"raw": payload}
    key_match = re.search(
        r"\b(variantAttributeArray|skuAvailable|sla|storeIDflag|ppid|oosFlag)\b", cleaned
    )
    if key_match:
        data["key"] = key_match.group(1)
        remainder = cleaned[key_match.end():].strip()
        if remainder:
            data["value_text"] = remainder
    variant_match = re.search(r"(\[[^\]]+\])", cleaned)
    if variant_match:
        try:
            data["value_json"] = json.loads(variant_match.group(1))
        except json.JSONDecodeError:
            pass
    return data


def _parse_product_node(node: Dict[str, Any]) -> Dict[str, Any]:
    details = _get_first(node.get("5"), {})
    price_field = _get_first(details.get("9"), "")
    rating_blob = _get_first(details.get("27"), "")
    delivery_field = _get_first(details.get("28"), "")
    max_limit_field = _get_first(details.get("13"), "")

    # quantity: prefer plain string; if field 3 was mis-decoded as a sub-message
    # (e.g. "1 Piece x 3" parsed as protobuf), try to recover it, then fall back
    # to field 18 (size description).
    qty_raw = _get_first(details.get("3"))
    if isinstance(qty_raw, str):
        quantity = qty_raw
    elif isinstance(qty_raw, dict):
        quantity = _recover_qty_from_pb_dict(qty_raw) or _get_first(details.get("18"))
    else:
        quantity = _get_first(details.get("18"))

    # price: structured dict (fully decoded) or legacy blob string
    if isinstance(price_field, dict):
        price = _parse_price_dict(price_field)
    elif isinstance(price_field, str):
        price = _parse_price_blob(price_field)
    else:
        price = {}

    # delivery: structured dict or legacy blob string
    if isinstance(delivery_field, dict):
        delivery = _parse_delivery_dict(delivery_field)
    elif isinstance(delivery_field, str):
        delivery = _parse_delivery_blob(delivery_field)
    else:
        delivery = {}

    # max order limit: structured dict or legacy blob string
    if isinstance(max_limit_field, dict):
        max_order_limit = _parse_max_order_limit_dict(max_limit_field)
    elif isinstance(max_limit_field, str):
        max_order_limit = _parse_max_order_limit(max_limit_field)
    else:
        max_order_limit = None

    parsed = {
        "name": _get_first(node.get("1")),
        "brand": _get_first(node.get("2")),
        "item_id": _get_first(details.get("1")),
        "spin_id": _get_first(details.get("2")),
        "quantity": quantity,
        "description": _get_first(details.get("21")),
        "category": _get_first(details.get("36")) or _get_first(details.get("10")),
        "pod_id": _get_first(details.get("19")),
        "max_order_limit_units": max_order_limit,
        "price": price,
        "rating": _parse_rating_blob(rating_blob) if isinstance(rating_blob, str) else {},
        "delivery": delivery,
        "image_ids": details.get("6", []) if isinstance(details.get("6"), list) else [],
    }

    # Fallback SLA from analytics blob
    if parsed["delivery"].get("delivery_time_value") is None:
        event_node = _get_first(node.get("8"), {})
        if isinstance(event_node, dict):
            for pl in event_node.get("7", []):
                if not isinstance(pl, str):
                    continue
                m = re.search(r"\b(\d+)\s*(MINS?|MINUTES?)\b", pl, flags=re.IGNORECASE)
                if m:
                    parsed["delivery"] = {
                        "delivery_time_value": int(m.group(1)),
                        "delivery_time_unit": m.group(2).upper(),
                    }
                    break

    event_node = _get_first(node.get("8"), {})
    if isinstance(event_node, dict):
        event_payloads = event_node.get("7", []) if isinstance(event_node.get("7"), list) else []
        parsed["event_payloads"] = [_parse_event_payload(pl) for pl in event_payloads if isinstance(pl, str)]
        parsed["event_metadata"] = {
            "event_name": _get_first(event_node.get("1")),
            "event_context_json": _get_first(event_node.get("2")),
            "impression": _get_first(event_node.get("4")),
            "click_event": _get_first(event_node.get("5")),
        }
    else:
        parsed["event_payloads"] = []
        parsed["event_metadata"] = {}

    return parsed


def _dedupe_products(products: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen: set = set()
    out = []
    for p in products:
        key = p.get("item_id") or f"{p.get('name')}|{p.get('quantity')}"
        if key not in seen:
            seen.add(key)
            out.append(p)
    return out


def _validate_product(product: Dict[str, Any]) -> List[Dict[str, str]]:
    errors = []
    checks = [
        ("price.selling_price", product.get("price", {}).get("selling_price")),
        ("price.mrp", product.get("price", {}).get("mrp")),
        ("delivery.delivery_time_value", product.get("delivery", {}).get("delivery_time_value")),
        ("rating.rating", product.get("rating", {}).get("rating")),
        ("quantity", product.get("quantity")),
    ]
    for field, value in checks:
        if value is None:
            errors.append({"field": field, "message": f"Missing {field}; keep as null and optionally fall back to OCR/UI text."})
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

    # Each matching card node may hold multiple variants in its field-5 list.
    # Iterate over every variant dict so the single-piece and 3-pack are both captured.
    products = []
    for n in _walk_nodes(root):
        if not _is_product_node(n):
            continue
        variants = n.get("5", [])
        if not isinstance(variants, list):
            variants = [variants]
        for vd in variants:
            if not isinstance(vd, dict):
                continue
            pseudo = dict(n)
            pseudo["5"] = [vd]
            products.append(_parse_product_node(pseudo))
    products = _dedupe_products(products)

    current = products[0] if products else None
    others = products[1:] if len(products) > 1 else []
    seller = _extract_seller_details(root)
    info_cards = _extract_info_cards(root)
    description_card = next((c for c in info_cards if c.get("title") == "Description"), None)
    other_info_card = next((c for c in info_cards if c.get("title") == "Other Information"), None)

    typed_nodes = []
    for node in _walk_nodes(root):
        t = _get_first(node.get("1"))
        payload = _get_first(node.get("2"))
        if isinstance(t, str) and t.startswith("type.googleapis.com/"):
            typed_nodes.append({"type_url": t, "payload": payload})

    return {
        "source_file": decoded.get("source_file"),
        "status": "ok" if current else "partial",
        "decoded_meta": {
            "format": decoded.get("format"),
        },
        "current_product": current,
        "seller_details": seller,
        "description": description_card,
        "other_information": other_info_card,
        "other_products_count": len(others),
        "other_products": others,
        "all_data": {
            "all_products_count": len(products),
            "all_products": products,
            "info_cards": info_cards,
            "typed_nodes": typed_nodes,
        },
        "fallback": {
            "missing_fields": _validate_product(current) if current else [],
            "notes": [
                "If any field is null, use decoded_raw_text regex fallback or OCR from screenshot.",
            ],
        },
    }


# ---------------------------------------------------------------------------
# spin_id extraction from first response
# ---------------------------------------------------------------------------

def extract_spin_id_from_file(path: Path) -> Optional[str]:
    """Decode a raw protobuf binary and return the first spin_id found."""
    data = decode_binary_file(path)
    if not data:
        return None
    root = data.get("decoded_raw_tree") if isinstance(data, dict) else data
    if not isinstance(root, dict):
        return None
    for node in _walk_nodes(root):
        if _is_product_node(node):
            details = _get_first(node.get("5"), {})
            spin_id = _get_first(details.get("2"))
            if isinstance(spin_id, str) and spin_id:
                return spin_id
    return None


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def main() -> None:
    resp1 = BASE_DIR / RESPONSE_FILE_1
    resp2 = BASE_DIR / RESPONSE_FILE_2
    out_path = BASE_DIR / OUTPUT_JSON

    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Step 1: first curl (no spin_id)
    fetch_without_spin_id(resp1)

    # Step 2: decode first response -> get spin_id
    print(f"[2/4] Extracting spin_id from {resp1.name} ...")
    spin_id = extract_spin_id_from_file(resp1)
    if not spin_id:
        print("      WARNING: could not extract spin_id from first response; aborting.", file=sys.stderr)
        sys.exit(1)
    print(f"      spin_id = {spin_id}")

    # Step 3: second curl (with spin_id)
    fetch_with_spin_id(spin_id, resp2)

    # Step 4: decode second response -> structured JSON
    print(f"[4/4] Decoding {resp2.name} -> {out_path} ...")
    decoded = decode_binary_file(resp2)
    if not decoded:
        print("      ERROR: could not decode second response binary.", file=sys.stderr)
        sys.exit(1)
    decoded["source_file"] = str(resp2)

    output = build_product_output(decoded)
    out_path.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"      Done -> {out_path}")

    # Quick summary
    all_products = output.get("all_data", {}).get("all_products", [])
    if all_products:
        print(f"\n{'─'*55}")
        for i, p in enumerate(all_products):
            label = "current" if i == 0 else f"variant {i}"
            price = p.get("price", {})
            delivery = p.get("delivery", {})
            print(f"[{label}] {p.get('name')}  ({p.get('quantity')})")
            print(f"  item_id  : {p.get('item_id')}  spin_id: {p.get('spin_id')}")
            print(f"  price    : MRP={price.get('mrp')}  Selling={price.get('selling_price')}  "
                  f"Unit={price.get('unit_price_text')}  Discount={price.get('discount_text')}")
            print(f"  delivery : {delivery.get('delivery_time_value')} {delivery.get('delivery_time_unit')}  "
                  f"max_order={p.get('max_order_limit_units')}")
        print(f"{'─'*55}")
    else:
        print("\nWARNING: no products extracted (status=partial)")


if __name__ == "__main__":
    main()
