import requests
import json
import base64

# ─── CONFIG ──────────────────────────────────────────────────────────────────
# PINCODE    = "700001"
# LAT        = "22.5726"
# LNG        = "88.3639"
# ADDRESS_ID = 700001 
# PRODUCT_ID = 214431 

# PINCODE    = "700001"
# LAT        = "22.5726"
# LNG        = "88.3639"
# ADDRESS_ID = 700001  
# PRODUCT_ID = 30002246

# PINCODE    = "700001"
# LAT        = "22.5726"
# LNG        = "88.3639"
# ADDRESS_ID = 700001  
# PRODUCT_ID = 10000025

# PINCODE    = "700001"
# LAT        = "22.5726"
# LNG        = "88.3639"
# ADDRESS_ID = 700001
# PRODUCT_ID = 294222

# PINCODE    = "700001"
# LAT        = "22.7835"
# LNG        = "86.1660"
# ADDRESS_ID = 700001
# PRODUCT_ID = 40242712

# PINCODE    = "700001"
# LAT        = "22.7835"
# LNG        = "86.1660"
# ADDRESS_ID = 700001
# PRODUCT_ID = 40179976

# PINCODE    = "700001"
# LAT        = "22.7835"
# LNG        = "86.1660"
# ADDRESS_ID = 700001
# PRODUCT_ID = 40108477

# PINCODE    = "700001"
# LAT        = "22.7835"
# LNG        = "86.1660"
# ADDRESS_ID = 700001
# PRODUCT_ID = 40108468

# Runtime defaults (used if no active config block is uncommented)
PINCODE = globals().get("PINCODE", "700001")
LAT = globals().get("LAT", "22.5726")
LNG = globals().get("LNG", "88.3639")
ADDRESS_ID = globals().get("ADDRESS_ID", int(PINCODE))
PRODUCT_ID = globals().get("PRODUCT_ID", 40108468)


# Cookies / tokens that must be provided (copy fresh values here)
BB_VID      = "MTE1MTczMjcyMjA3OTcwMTIzMA=="
BB_AID      = "Mjk2NTE4NTMwNA=="
# ─────────────────────────────────────────────────────────────────────────────

# PINCODE_COORDS = {
#     "700001": ("22.5726", "88.3639"),   # Kolkata (BBD Bagh)
#     "110001": ("28.6315", "77.2167"),   # New Delhi (Connaught Place)
#     "400001": ("18.9388", "72.8354"),   # Mumbai (Fort / GPO)
#     "600001": ("13.0837", "80.2707"),   # Chennai (George Town / GPO)
# }

GQL_QUERY = (
    "query ProductQuery($id: Int64!) { product(id: $id) { base_img_url ...productFields "
    "children { ...productFields } } } "
    "fragment productFields on Product { id desc pack_desc w absolute_url cart_count "
    "availability { avail_status short_eta medium_eta long_eta label display_mrp display_sp "
    "not_for_sale button show_express } images { s l ml m xl xxl } "
    "variable_weight { msg link } brand { name slug url } "
    "additional_attr { bby_lod info { id type image_url sub_type label position } } "
    "tabs { content title } tags { header values { display_name dest_type dest_slug url } } "
    "combo { destination { display_name dest_type dest_slug url dest_ids } total_saving_msg "
    "items { id brand sp mrp d_text d_avail link img_url qty wgt p_desc } total_sp total_mrp } "
    "category { tlc_name tlc_slug llc_slug llc_name llc_id } "
    "rating_info { avg_rating rating_count review_count sku_id } "
    "breadcrumb { id name slug type url } aplus_url "
    "pricing { discount { mrp d_text d_avail "
    "prim_price { sp icon { base_url image format } desc } "
    "sec_price { sp icon { base_url image format } desc background } "
    "offer_entry_text offer_available } "
    "promo { desc desc_label type label saving savings_display id url name } } }"
)


def log(msg: str) -> None:
    print(msg)


def normalize_location_config() -> None:
    """Keep PINCODE / LAT / LNG / ADDRESS_ID coherent to avoid false availability."""
    global LAT, LNG, ADDRESS_ID
    pincode = str(PINCODE)
    # expected = PINCODE_COORDS.get(pincode)
    # if expected:
    #     exp_lat, exp_lng = expected
    #     if LAT != exp_lat or LNG != exp_lng:
    #         log(f"[CONFIG FIX] PINCODE={pincode} expects lat/lng={exp_lat},{exp_lng}; "
    #             f"received {LAT},{LNG}. Auto-correcting.")
    #         LAT, LNG = exp_lat, exp_lng

    desired_address_id = int(pincode)
    if ADDRESS_ID != desired_address_id:
        log(f"[CONFIG FIX] ADDRESS_ID={ADDRESS_ID} does not match PINCODE={pincode}. "
            f"Auto-correcting to {desired_address_id}.")
        ADDRESS_ID = desired_address_id


def b64_lat_long(lat: str, lng: str) -> str:
    raw = f"{lat}|{lng}".encode("utf-8")
    return base64.b64encode(raw).decode("utf-8")


def decode_b64(value: str | None) -> str | None:
    if not value:
        return None
    try:
        return base64.b64decode(value).decode("utf-8")
    except Exception:
        return None


def get_cookie_value(session: requests.Session, name: str) -> str | None:
    # Requests cookie jar may contain same cookie name across domains/paths.
    # Prefer .bigbasket.com cookies and latest encountered value.
    preferred = None
    fallback = None
    for c in session.cookies:
        if c.name != name:
            continue
        fallback = c.value
        if "bigbasket.com" in (c.domain or ""):
            preferred = c.value
    return preferred if preferred is not None else fallback


def debug_request(tag: str, method: str, url: str, headers: dict, cookies: dict, body: dict | None) -> None:
    log(f"\n  [{tag}] request debug:")
    log(f"    method: {method}")
    log(f"    url   : {url}")
    log(f"    headers: {json.dumps(headers, indent=6)}")
    log(f"    cookies: {json.dumps(cookies, indent=6)}")
    if body is not None:
        log(f"    body: {json.dumps(body, indent=6)}")


def debug_cookie_jar(session: requests.Session, tag: str) -> None:
    keys = [
        "_bb_vid", "_bb_aid", "_bb_pin_code", "_bb_lat_long", "_bb_sa_ids",
        "_bb_cda_sa_info", "_bb_addressinfo", "csurftoken", "csrftoken",
    ]
    snap = {}
    for k in keys:
        v = get_cookie_value(session, k)
        if v is not None:
            snap[k] = v
    log(f"\n  [{tag}] session cookies snapshot:")
    if not snap:
        log("    (no relevant cookies in jar)")
        return
    log(f"    {json.dumps(snap, indent=4)}")
    lat_long_decoded = decode_b64(snap.get("_bb_lat_long"))
    if lat_long_decoded is not None:
        log(f"    decoded _bb_lat_long: {lat_long_decoded}")
    cda_decoded = decode_b64(snap.get("_bb_cda_sa_info"))
    if cda_decoded is not None:
        log(f"    decoded _bb_cda_sa_info: {cda_decoded}")


def run_set_address(session: requests.Session) -> tuple[str, str] | None:
    """POST set-current-address and return (bb_vid, bb_aid), or None if area not served."""
    print(f"BB_AID: {BB_AID}\n")
    print(f"BB_VID: {BB_VID}\n")

    url = "https://www.bigbasket.com/mapi/v4.2.0/set-current-address/"

    headers = {
        "User-Agent":    "BB Android/v6.3.1/os 13",
        "X-channel":     "BB-Android",
        "Content-Type":  "application/x-www-form-urlencoded",
    }

    lat_long_cookie = b64_lat_long(LAT, LNG)
    cookies = {
        "_bb_source":   "app",
        "_bb_vid":      BB_VID,
        "_bb_aid":      BB_AID,
        "_bb_cid":      "1",
        "_bb_pin_code": PINCODE,
        "_bb_lat_long": lat_long_cookie,
    }

    data = {
        "referrer":  "home",
        "id":        str(ADDRESS_ID),
        "lat":       LAT,
        "lng":       LNG,
        "transient": "1",
        "src":       "2",
    }

    log("\n══════════════════════════════════════════════════")
    log("[1/2] set-current-address")
    log("══════════════════════════════════════════════════")
    log(f"  URL        : {url}")
    log(f"  lat/lng    : {LAT}, {LNG}")
    log(f"  address_id : {ADDRESS_ID}")
    log(f"  _bb_lat_long (sent): {lat_long_cookie} -> {LAT}|{LNG}")
    log("  Sending request...")
    debug_request("set-current-address", "POST", url, headers, cookies, data)

    resp = session.post(url, headers=headers, cookies=cookies, data=data)

    log(f"  HTTP status: {resp.status_code}")
    set_cookie_header = resp.headers.get("set-cookie")
    if set_cookie_header:
        log(f"  Response set-cookie header:\n{set_cookie_header}")
    debug_cookie_jar(session, "after set-current-address")

    try:
        body = resp.json()
    except Exception:
        log(f"  [ERROR] Could not parse JSON. Raw response:\n{resp.text[:500]}")
        return None

    log(f"  Response body: {json.dumps(body, indent=4)}")
    title = ((body.get("response") or {}).get("title") or "").lower()
    if "changing area" in title:
        log("  Server asked for area confirmation. Sending commit call with transient=0 ...")
        data_commit = dict(data)
        data_commit["transient"] = "0"
        debug_request("set-current-address-confirm", "POST", url, headers, cookies, data_commit)
        resp2 = session.post(url, headers=headers, cookies=cookies, data=data_commit)
        log(f"  Confirm call HTTP status: {resp2.status_code}")
        try:
            body2 = resp2.json()
            log(f"  Confirm call response body: {json.dumps(body2, indent=4)}")
        except Exception:
            log(f"  Confirm call raw response: {resp2.text[:500]}")
        set_cookie_header_2 = resp2.headers.get("set-cookie")
        if set_cookie_header_2:
            log(f"  Confirm call set-cookie header:\n{set_cookie_header_2}")
        debug_cookie_jar(session, "after set-current-address confirm")

    # BigBasket returns status 185 when the pincode/area is not serviceable
    bb_status = body.get("status")
    if bb_status == 185:
        message = body.get("message", "Unknown error")
        log(f"\n  [BLOCKED] Area not serviceable (status {bb_status}): {message}")
        log("  Cannot proceed to product query. Update LAT/LNG/ADDRESS_ID and retry.")
        return None

    if bb_status not in (0, 200, None):
        log(f"\n  [WARNING] Unexpected BB status code: {bb_status}  — {body.get('message', '')}")

    # Extract bb_vid / bb_aid from updated session cookies (fall back to config values)
    bb_vid = (get_cookie_value(session, "_bb_vid") or BB_VID).strip('"')
    bb_aid = (get_cookie_value(session, "_bb_aid") or BB_AID).strip('"')
    log(f"\n  Extracted cookies:")
    log(f"    bb_vid = {bb_vid}")
    log(f"    bb_aid = {bb_aid}")
    return bb_vid, bb_aid


def run_product_query(session: requests.Session, bb_vid: str, bb_aid: str) -> dict | None:
    """POST GQL product query using bb_vid/bb_aid from previous step."""
    print(f"BB_AID: {BB_AID}\n")
    print(f"BB_VID: {BB_VID}\n")

    url = "https://www.bigbasket.com/pd-svc/v1/visitor/gql/"

    headers = {
        "User-Agent":      "BB Android/v6.1.0/os 13",
        "X-channel":       "BB-Android",
        "Content-Type":    "application/json",
        "Accept":          "*/*",
        "Accept-Encoding": "gzip",
        "X-NewRelic-ID":   "XAUAUlZXGwUGVVdRBwA=",
    }
    # If server issued a csurftoken, pass it as header like app/web clients do.
    csurftoken = get_cookie_value(session, "csurftoken")
    if csurftoken:
        headers["x-csurftoken"] = csurftoken

    # IMPORTANT:
    # Do not override service-area cookies in this request with hardcoded values.
    # Let the session cookie jar (updated by set-current-address) drive location context.
    session.cookies.set("_bb_source", "app")
    session.cookies.set("_bb_vid", bb_vid)
    session.cookies.set("_bb_aid", bb_aid)
    session.cookies.set("_bb_pin_code", PINCODE)
    session.cookies.set("_bb_lat_long", b64_lat_long(LAT, LNG))

    payload = {
        "query":     GQL_QUERY,
        "variables": {"id": PRODUCT_ID},
        "list_type": "pd",
    }

    log("\n══════════════════════════════════════════════════")
    log("[2/2] product GQL query")
    log("══════════════════════════════════════════════════")
    log(f"  URL        : {url}")
    log(f"  product_id : {PRODUCT_ID}")
    log(f"  bb_vid     : {bb_vid}")
    log(f"  bb_aid     : {bb_aid}")
    debug_cookie_jar(session, "before product query")
    log("  Sending request...")
    debug_request("product-gql", "POST", url, headers, {}, payload)
    resp = session.post(url, headers=headers, json=payload)

    log(f"  HTTP status: {resp.status_code}")
    set_cookie_header = resp.headers.get("set-cookie")
    if set_cookie_header:
        log(f"  Response set-cookie header:\n{set_cookie_header}")
    debug_cookie_jar(session, "after product query")

    try:
        data = resp.json()
        log(f"  Response body:\n{json.dumps(data, indent=4)}")
        product = (data or {}).get("data", {}).get("product", {})
        availability = (product or {}).get("availability", {})
        pricing = (product or {}).get("pricing", {}).get("discount", {})
        log("\n  Parsed summary:")
        log(f"    avail_status  : {availability.get('avail_status')}")
        log(f"    avail_label   : {availability.get('label')}")
        log(f"    display_sp    : {availability.get('display_sp')}")
        log(f"    display_mrp   : {availability.get('display_mrp')}")
        log(f"    sp            : {(pricing.get('prim_price') or {}).get('sp')}")
        log(f"    mrp           : {pricing.get('mrp')}")
        if availability.get("avail_status") == "010":
            log("    hint          : location/service-area cookies likely do not match target lat/lng/pincode")
        return data
    except Exception:
        log(f"  [ERROR] Could not parse JSON. Raw response:\n{resp.text[:1000]}")
        return None


def main():
    normalize_location_config()
    log("BigBasket product checker")
    log(f"Config → pincode={PINCODE}  lat={LAT}  lng={LNG}  product_id={PRODUCT_ID}")

    session = requests.Session()

    result = run_set_address(session)
    if result is None:
        log("\n[ABORT] Stopped after address step failed. Fix config and retry.")
        return

    bb_vid, bb_aid = result
    run_product_query(session, bb_vid, bb_aid)
    log("\n[DONE]")


if __name__ == "__main__":
    main()
