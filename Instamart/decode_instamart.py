#!/usr/bin/env python3
"""
Decode and structure Instamart payload files for scraping.

Supports:
- JSON responses (keeps metadata + extracts key entities)
- app_location style text dumps containing type.googleapis.com markers
"""

from __future__ import annotations

import argparse
import ast
import json
import re
import subprocess
import struct
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


TYPE_URL_RE = re.compile(r"type\.googleapis\.com/[A-Za-z0-9._]+")
IMAGE_RE = re.compile(
    r"(?:[A-Z_]+/IMAGES/[A-Za-z0-9/_\-.]+|https?://[^\s\"']+\.(?:png|jpg|jpeg|webp))",
    re.IGNORECASE,
)
UUID_RE = re.compile(
    r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b"
)
HEX_COLOR_RE = re.compile(r"#[0-9A-Fa-f]{6,8}")
MONEY_RE = re.compile(r"INR\.?\s*([0-9]+(?:\.[0-9]+)?)")
PERCENT_RE = re.compile(r"([0-9]{1,3})%\s*OFF", re.IGNORECASE)
UNIT_PRICE_RE = re.compile(r"\b[0-9]+(?:\.[0-9]+)?/[0-9]+(?:\.[0-9]+)?\s*[A-Za-z]+\b")


def uniq(items: Iterable[str]) -> List[str]:
    seen = set()
    out = []
    for item in items:
        if item not in seen:
            seen.add(item)
            out.append(item)
    return out


def normalize_type_url(value: str) -> str:
    cleaned = re.sub(r"[^\w/.-]+$", "", value)
    cleaned = re.sub(r"\.+$", "", cleaned)
    cleaned = re.sub(r"\.[0-9]+$", "", cleaned)
    return cleaned


def walk_json(obj: Any, path: str = "$") -> Iterable[Tuple[str, Any]]:
    yield path, obj
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield from walk_json(v, f"{path}.{k}")
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            yield from walk_json(v, f"{path}[{i}]")


def summarize_json(path: Path, obj: Any) -> Dict[str, Any]:
    type_urls: List[str] = []
    image_paths: List[str] = []
    uuids: List[str] = []
    colors: List[str] = []
    prices: List[str] = []
    discounts: List[str] = []
    product_candidates: List[Dict[str, Any]] = []

    for jpath, value in walk_json(obj):
        if isinstance(value, str):
            type_urls.extend(normalize_type_url(v) for v in TYPE_URL_RE.findall(value))
            image_paths.extend(IMAGE_RE.findall(value))
            uuids.extend(UUID_RE.findall(value))
            colors.extend(HEX_COLOR_RE.findall(value))
            prices.extend(MONEY_RE.findall(value))
            discounts.extend(PERCENT_RE.findall(value))

        if isinstance(value, dict):
            name = (
                value.get("title")
                or value.get("name")
                or value.get("displayName")
                or value.get("productName")
            )
            if isinstance(name, str):
                brand = value.get("brand") if isinstance(value.get("brand"), str) else None
                image = None
                for key in ("imageId", "selectedAssetId", "unselectedAssetId", "cloudinaryImageId"):
                    if isinstance(value.get(key), str):
                        image = value[key]
                        break
                product_candidates.append(
                    {"path": jpath, "name": name, "brand": brand, "image": image}
                )

    return {
        "source_file": str(path),
        "format": "json",
        "top_level_type": type(obj).__name__,
        "summary": {
            "type_url_count": len(set(type_urls)),
            "image_count": len(set(image_paths)),
            "uuid_count": len(set(uuids)),
            "color_count": len(set(colors)),
            "price_hits": len(prices),
            "discount_hits": len(discounts),
            "product_candidates": len(product_candidates),
        },
        "type_urls": sorted(set(type_urls)),
        "image_paths": sorted(set(image_paths)),
        "uuids": sorted(set(uuids)),
        "colors": sorted(set(colors)),
        "products": product_candidates[:500],
    }


def decode_escape_sequences(text: str) -> str:
    def repl(match: re.Match[str]) -> str:
        return chr(int(match.group(1), 16))

    return re.sub(r"\\x([0-9a-fA-F]{2})", repl, text)


def split_blocks_by_type_url(lines: List[str]) -> List[Dict[str, Any]]:
    blocks: List[Dict[str, Any]] = []
    current: Dict[str, Any] | None = None

    for line in lines:
        type_match = TYPE_URL_RE.search(line)
        if type_match:
            if current is not None:
                blocks.append(current)
            current = {
                "type_url": normalize_type_url(type_match.group(0)),
                "raw_lines": [line],
            }
            continue

        if current is not None:
            current["raw_lines"].append(line)

    if current is not None:
        blocks.append(current)
    return blocks


def summarize_dump(path: Path, text: str) -> Dict[str, Any]:
    decoded = decode_escape_sequences(text)
    lines = decoded.splitlines()
    blocks = split_blocks_by_type_url(lines)
    typed_blocks: List[Dict[str, Any]] = []

    global_type_urls: List[str] = []
    global_images: List[str] = []
    global_uuids: List[str] = []
    global_colors: List[str] = []
    global_prices: List[str] = []
    global_discounts: List[str] = []

    for idx, block in enumerate(blocks, start=1):
        joined = "\n".join(block["raw_lines"])
        type_urls = [normalize_type_url(v) for v in TYPE_URL_RE.findall(joined)]
        images = IMAGE_RE.findall(joined)
        uuids = UUID_RE.findall(joined)
        colors = HEX_COLOR_RE.findall(joined)
        prices = MONEY_RE.findall(joined)
        discounts = PERCENT_RE.findall(joined)

        global_type_urls.extend(type_urls)
        global_images.extend(images)
        global_uuids.extend(uuids)
        global_colors.extend(colors)
        global_prices.extend(prices)
        global_discounts.extend(discounts)

        text_chunks = []
        for raw_line in block["raw_lines"]:
            cleaned = re.sub(r"[^\w\s#/%\-.]", " ", raw_line)
            cleaned = re.sub(r"\s+", " ", cleaned).strip()
            if len(cleaned) >= 4:
                text_chunks.append(cleaned)

        typed_blocks.append(
            {
                "index": idx,
                "type_url": block["type_url"],
                "line_count": len(block["raw_lines"]),
                "type_urls_in_block": uniq(type_urls),
                "image_paths": uniq(images),
                "uuids": uniq(uuids),
                "colors": uniq(colors),
                "price_values": uniq(prices),
                "discount_values": uniq(discounts),
                "text_preview": uniq(text_chunks)[:20],
            }
        )

    return {
        "source_file": str(path),
        "format": "protobuf_text_dump",
        "summary": {
            "block_count": len(typed_blocks),
            "type_url_count": len(set(global_type_urls)),
            "image_count": len(set(global_images)),
            "uuid_count": len(set(global_uuids)),
            "color_count": len(set(global_colors)),
            "price_hits": len(global_prices),
            "discount_hits": len(global_discounts),
        },
        "type_urls": sorted(set(global_type_urls)),
        "image_paths": sorted(set(global_images)),
        "uuids": sorted(set(global_uuids)),
        "colors": sorted(set(global_colors)),
        "blocks": typed_blocks,
    }


def try_decode_raw_protobuf(raw_bytes: bytes) -> str | None:
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


def summarize_protoc_text(path: Path, decoded_text: str) -> Dict[str, Any]:
    type_urls = [normalize_type_url(v) for v in TYPE_URL_RE.findall(decoded_text)]
    images = IMAGE_RE.findall(decoded_text)
    uuids = UUID_RE.findall(decoded_text)
    colors = HEX_COLOR_RE.findall(decoded_text)

    price_blocks = extract_price_blocks_from_protoc(decoded_text)
    decoded_tree = parse_decode_raw_text(decoded_text)

    return {
        "source_file": str(path),
        "format": "protobuf_binary_decode_raw",
        "summary": {
            "line_count": decoded_text.count("\n") + 1,
            "type_url_count": len(set(type_urls)),
            "image_count": len(set(images)),
            "uuid_count": len(set(uuids)),
            "color_count": len(set(colors)),
            "price_block_count": len(price_blocks),
            "root_field_count": len(decoded_tree),
        },
        "type_urls": sorted(set(type_urls)),
        "image_paths": sorted(set(images)),
        "uuids": sorted(set(uuids)),
        "colors": sorted(set(colors)),
        "extracted_price_blocks": price_blocks,
        "decoded_raw_tree": decoded_tree,
        "decoded_raw_text": decoded_text,
    }


def add_field(node: Dict[str, Any], field_no: str, value: Any) -> None:
    if field_no not in node:
        node[field_no] = [value]
    else:
        node[field_no].append(value)


def parse_scalar_value(raw_value: str) -> Any:
    value = raw_value.strip()
    if value.startswith('"') and value.endswith('"'):
        # decode c-style escaped strings emitted by protoc
        return ast.literal_eval(value)

    if value.startswith("0x"):
        try:
            as_int = int(value, 16)
            as_float = struct.unpack("<f", as_int.to_bytes(4, byteorder="little", signed=False))[0]
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

        open_match = open_re.match(line)
        if open_match:
            field_no = open_match.group(1)
            child: Dict[str, Any] = {}
            add_field(stack[-1], field_no, child)
            stack.append(child)
            continue

        scalar_match = scalar_re.match(line)
        if scalar_match:
            field_no, raw_value = scalar_match.group(1), scalar_match.group(2)
            add_field(stack[-1], field_no, parse_scalar_value(raw_value))
            continue

    return root


def unescape_c_string(s: str) -> bytes:
    out = bytearray()
    i = 0
    while i < len(s):
        ch = s[i]
        if ch != "\\":
            out.extend(ch.encode("utf-8"))
            i += 1
            continue
        i += 1
        if i >= len(s):
            out.append(ord("\\"))
            break
        esc = s[i]
        i += 1
        if esc in {'\\', '"', "'"}:
            out.append(ord(esc))
        elif esc == "n":
            out.append(0x0A)
        elif esc == "r":
            out.append(0x0D)
        elif esc == "t":
            out.append(0x09)
        elif esc == "x" and i + 1 <= len(s):
            hx = s[i : i + 2]
            if len(hx) == 2 and all(c in "0123456789abcdefABCDEF" for c in hx):
                out.append(int(hx, 16))
                i += 2
            else:
                out.extend(b"\\x")
        elif esc in "01234567":
            oct_digits = esc
            for _ in range(2):
                if i < len(s) and s[i] in "01234567":
                    oct_digits += s[i]
                    i += 1
                else:
                    break
            out.append(int(oct_digits, 8))
        else:
            out.append(ord(esc))
    return bytes(out)


def read_varint(buf: bytes, i: int) -> Tuple[int, int] | Tuple[None, int]:
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
        # Common embedded pricing format: "INR" + field tag 0x10 + varint amount
        probe = idx + 3
        if probe < len(raw) and raw[probe] == 0x10:
            val, end = read_varint(raw, probe + 1)
            if val is not None and 0 < val < 1_000_000:
                values.append(val)
                i = end
                continue
        i = idx + 3
    return values


def infer_price(inr_values: List[int], discount_texts: List[str]) -> Dict[str, Any]:
    if not inr_values:
        return {}
    inferred: Dict[str, Any] = {"inr_values_ordered": inr_values}
    if len(inr_values) >= 2:
        inferred["mrp"] = max(inr_values)
        inferred["selling_price"] = min(inr_values[:2]) if len(inr_values) >= 2 else min(inr_values)
        if inferred["mrp"] >= inferred["selling_price"]:
            inferred["savings"] = inferred["mrp"] - inferred["selling_price"]
    if discount_texts:
        inferred["discount_text"] = discount_texts[0]
    return inferred


def extract_price_blocks_from_protoc(decoded_text: str) -> List[Dict[str, Any]]:
    quoted = re.findall(r'"((?:[^"\\]|\\.)*)"', decoded_text)
    blocks: List[Dict[str, Any]] = []

    for q in quoted:
        if "INR" not in q and "OFF" not in q and "/100" not in q:
            continue
        raw = unescape_c_string(q)
        text = raw.decode("utf-8", errors="ignore")
        inr_values = extract_inr_values(raw)
        discount_texts = uniq(m.group(0) for m in PERCENT_RE.finditer(text))
        unit_prices = uniq(m.group(0) for m in UNIT_PRICE_RE.finditer(text))
        if not inr_values and not discount_texts and not unit_prices:
            continue
        blocks.append(
            {
                "raw_preview": text[:220],
                "inr_values": inr_values,
                "discount_texts": discount_texts,
                "unit_prices": unit_prices,
                "inferred": infer_price(inr_values, discount_texts),
            }
        )
    return blocks


def decode_file(path: Path) -> Dict[str, Any]:
    raw_bytes = path.read_bytes()
    raw = raw_bytes.decode("utf-8", errors="replace")
    try:
        obj = json.loads(raw)
        return summarize_json(path, obj)
    except json.JSONDecodeError:
        pass

    decoded_text = try_decode_raw_protobuf(raw_bytes)
    if decoded_text:
        return summarize_protoc_text(path, decoded_text)

    if "type.googleapis.com/" in raw:
        return summarize_dump(path, raw)

    return {
        "source_file": str(path),
        "format": "plain_text",
        "summary": {"line_count": raw.count("\n") + 1, "char_count": len(raw)},
        "preview": raw[:1000],
    }


def collect_files(paths: List[str]) -> List[Path]:
    out: List[Path] = []
    for p in paths:
        path = Path(p)
        if path.is_file():
            out.append(path)
        elif path.is_dir():
            for child in sorted(path.rglob("*")):
                if child.is_file():
                    out.append(child)
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Decode Instamart payload files into structured JSON")
    parser.add_argument(
        "inputs",
        nargs="+",
        help="Input files or directories. Directories are scanned recursively.",
    )
    parser.add_argument(
        "--out-dir",
        default="Instamart/decoded",
        help="Output directory for structured JSON files",
    )
    args = parser.parse_args()

    files = collect_files(args.inputs)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    processed = 0
    for file_path in files:
        if file_path.name.startswith("."):
            continue
        decoded = decode_file(file_path)
        out_path = out_dir / f"{file_path.name}.structured.json"
        out_path.write_text(json.dumps(decoded, ensure_ascii=False, indent=2), encoding="utf-8")
        processed += 1
        fmt = decoded.get("format", "unknown")
        print(f"{file_path} -> {out_path} ({fmt})")

    print(f"Processed {processed} file(s)")


if __name__ == "__main__":
    main()
