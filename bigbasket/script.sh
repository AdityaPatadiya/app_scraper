#!/bin/bash

# ==========================================
# USER CONFIGURATION (FILL THESE VALUES)
# ==========================================

# 1. Product Details
# Use the full URL with the slug (e.g., /pd/123/name/)
PRODUCT_URL="https://www.bigbasket.com/pd/126906/"

# 2. Location Details (The new location you want)
LAT="13.10197"
LONG="77.5863591"
ZIP="560064"

# 3. Security Token (x-csurftoken)
# Copy this from the "x-csurftoken" header in your browser's Network tab.
# It MUST match the 'csurftoken' inside the cookie string below.
CSRF_TOKEN="lXQKRg.MTE0NDA2MzI5ODY4ODg4ODU1OA==.1772263499763.Asz8FNI0a7m4o8Miv8/WVaXvEmjaCZJInRqhE3fRPxg="

# 4. Full Cookie String
# Copy the entire "cookie:" value from your browser request. 
# IMPORTANT: This allows the server to know WHO you are.
COOKIE_STRING="_bb_locSrc=default; x-channel=web; _bb_aid=MjkxMzA4NDUzMA==; _bb_cid=1; _bb_vid=MTE0NDA2MzI5ODY4ODg4ODU1OA==; _bb_nhid=7427; _bb_dsid=7427; _bb_dsevid=7427; _bb_bhid=; _bb_loid=; csrftoken=3JIKSiDQorCSePIC1zsMe0ebKA5ruqPnWeResZhz6LtNfFJdhMu6eZiWBPXRQ1Sl; isintegratedsa=true; jentrycontextid=10; xentrycontextid=10; xentrycontext=bbnow; _bb_bb2.0=1; is_global=1; _bb_addressinfo=; _bb_pin_code=; _bb_sa_ids=19224; _is_tobacco_enabled=1; _is_bb1.0_supported=0; _bb_cda_sa_info=djIuY2RhX3NhLjEwLjE5MjI0; is_integrated_sa=1; is_subscribe_sa=0; bb2_enabled=true; ufi=1; _gcl_au=1.1.1972760248.1772258689; jarvis-id=25a193fe-72d0-41fb-b924-098c5a6dbf70; adb=0; _gid=GA1.2.1235433838.1772258689; _fbp=fb.1.1772258689404.665925746554496713; bigbasket.com=47c28ce6-4332-480e-81ec-771270a35448; csurftoken=${CSRF_TOKEN}; ts=2026-02-28%2013:07:08.081"

# ==========================================
# SCRIPT LOGIC (DO NOT EDIT BELOW)
# ==========================================

echo "--- Step 1: Updating Location to $ZIP ---"

# We use a 'cookie jar' (-c) to save the NEW cookies the server sends back (like the new Address ID)
curl 'https://www.bigbasket.com/member-svc/v2/member/current-delivery-address/' \
  -X PUT \
  --http2 \
  -c bb_new_cookies.txt \
  -H 'authority: www.bigbasket.com' \
  -H 'accept: */*' \
  -H 'accept-language: en-US,en;q=0.9' \
  -H 'content-type: application/json' \
  -H 'origin: https://www.bigbasket.com' \
  -H 'referer: https://www.bigbasket.com/' \
  -H 'x-caller: UI-KIRK' \
  -H 'x-channel: BB-WEB' \
  -H 'x-entry-context: bbnow' \
  -H 'x-entry-context-id: 10' \
  -H 'x-csurftoken: '"$CSRF_TOKEN" \
  -H 'x-requested-with: XMLHttpRequest' \
  -H 'user-agent: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36' \
  -H "cookie: $COOKIE_STRING" \
  --data-raw '{"lat":'"$LAT"',"long":'"$LONG"',"return_hub_cookies":false,"contact_zipcode":"'"$ZIP"'"}' \
  --compressed \
  --output location_details.html

echo -e "\nLocation update response saved to 'location_details.html'."

echo -e "\n\n--- Step 2: Fetching Product Data ---"

# We use the 'cookie jar' (-b) to read the NEW location cookies we just saved.
# We ALSO send the original auth cookies (-H "Cookie...") so the server knows we are still logged in.
# The server will prioritize the specific cookies in the jar (the new location) over the old string.

curl "$PRODUCT_URL" \
  --http2 \
  -b bb_new_cookies.txt \
  -H 'upgrade-insecure-requests: 1' \
  -H 'user-agent: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36' \
  -H 'accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7' \
  -H 'x-entry-context: bbnow' \
  -H 'x-entry-context-id: 10' \
  -H "cookie: $COOKIE_STRING" \
  --compressed \
  --output product_details.html

echo -e "\n\nDone! Product details saved to 'product_details.html'."
