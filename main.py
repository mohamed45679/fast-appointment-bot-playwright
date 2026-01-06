# === FULL FINAL CODE (Async Playwright Version) ===

import asyncio
import pandas as pd
import random
import requests
import base64
from datetime import datetime, time as dt_time
from asyncio import Lock, Event

# === Configuration ===
BOT_TOKEN = "8127210037:AAFtiTU7tcEdbI-4Txiv2Ps6TdD8PRZnR1U"
CHAT_ID = "993152376"
EXCEL_FILE = "test13.xlsx"
SCREENSHOT_NAME = "category_found.png"
CAPTCHA_IMAGE_NAME = "captcha.png"
CAPTCHA_API_KEY = "a7708880ad3ee21096ebd3d2f54a71bd"
MAX_CONCURRENT = 2  # number of async tasks to run

CATEGORY_MAP = {
    "aufenthaltsbewilligung student (nur bachelor)": "44281520",
    "aufenthaltsbewilligung student (nur master, phd und stipendiate)": "44279679",
    "aufenthaltstitel rot-weiß-rot karte (nur für personen die selbst in österreich arbeiten wollen)": "26425165",
    "familienzusammenführung gem. nag (nur für ägypterinnen)": "32528820",
    "familienzusammenführung gem. nag (nur für sonstige staatsangehörige mit wohnsitz)": "32529659",
    "österreicher personenstandsangelegenheiten (eheschließung etc.)": "20950851",
    "österreicher (reisepässe, staatsbürgerschaft)": "134608"
}

claimed_slots = set()
lock = Lock()
stop_event = Event()


# === Telegram Helpers ===
async def send_telegram_alert_with_photo(bot_token, chat_id, message, photo_path):
    try:
        requests.post(f"https://api.telegram.org/bot{bot_token}/sendMessage",
                      data={"chat_id": chat_id, "text": message})
        with open(photo_path, 'rb') as photo:
            requests.post(f"https://api.telegram.org/bot{bot_token}/sendPhoto",
                          data={"chat_id": chat_id},
                          files={"photo": photo})
        print("✅ Alert and screenshot sent to Telegram.")
    except Exception as e:
        print(f"⚠️ Telegram error: {e}")


# === Time window check ===
def is_allowed_time():
    now = datetime.now().time()
    morning_start = dt_time(1, 10)
    morning_end = dt_time(23, 58)
    evening_start = dt_time(0, 10)
    evening_end = dt_time(0, 59)
    return (morning_start <= now <= morning_end) or (evening_start <= now <= evening_end)
import asyncio
import base64
import requests
from urllib.parse import urljoin
import os
import time

async def solve_image_captcha(page):
    try:
        await asyncio.sleep(0.2)

        # Wait for CAPTCHA image element
        captcha_img = await page.wait_for_selector(
            '#Captcha_CaptchaImage',
            timeout=10000
        )

        # Get CAPTCHA image src
        captcha_src = await captcha_img.get_attribute('src')
        if not captcha_src:
            print("❌ CAPTCHA src not found")
            return None

        # Build absolute CAPTCHA URL
        captcha_url = urljoin(page.url, captcha_src)

        # Download CAPTCHA image (raw bytes)
        response = await page.request.get(captcha_url)
        captcha_bytes = await response.body()

        # ---- SAVE CAPTCHA IMAGE LOCALLY ----
        timestamp = int(time.time())
        captcha_filename = f"captcha_{timestamp}.png"
        captcha_path = os.path.join(os.getcwd(), captcha_filename)

        with open(captcha_path, "wb") as f:
            f.write(captcha_bytes)

        print(f"📥 CAPTCHA image downloaded and saved: {captcha_filename}")

        # Encode saved image to base64
        with open(captcha_path, "rb") as f:
            b64_image = base64.b64encode(f.read()).decode("utf-8")

        print("📤 Sending CAPTCHA to 2Captcha...")

        upload_response = requests.post(
            "http://2captcha.com/in.php",
            data={
                "key": CAPTCHA_API_KEY,
                "method": "base64",
                "body": b64_image,
                "json": 1
            }
        )

        if upload_response.status_code != 200 or upload_response.json().get("status") != 1:
            print("❌ Failed to upload CAPTCHA:", upload_response.text)
            return None

        captcha_id = upload_response.json()["request"]
        print(f"🕒 Waiting for CAPTCHA solution (ID: {captcha_id})")

        for _ in range(30):
            await asyncio.sleep(3)
            result = requests.get(
                f"http://2captcha.com/res.php?key={CAPTCHA_API_KEY}&action=get&id={captcha_id}&json=1"
            )
            data = result.json()

            if data.get("status") == 1:
                print("✅ CAPTCHA solved -------------------", data["request"])
                return data["request"]

            if data.get("request") != "CAPTCHA_NOT_READY":
                print("❌ CAPTCHA error:", data)
                return None

        print("❌ CAPTCHA solve timeout.")
        return None

    except Exception as e:
        print(f"⚠️ CAPTCHA process error: {e}")
        return None




# === Bulk Form Fill (FAST) ===
def _safe_str(v):
    if v is None:
        return ""
    try:
        if str(v).lower() == "nan":
            return ""
    except Exception:
        pass
    return str(v).strip()

def _fmt_date_mmddyyyy(v):
    try:
        import pandas as _pd
        dt = _pd.to_datetime(v)
        return dt.strftime("%m/%d/%Y")
    except Exception:
        return _safe_str(v)

async def bulk_fill_form_once(page, df, i):
    """
    Fill the whole form in one injection (value set + input/change events),
    instead of field-by-field waits/fills.
    Keeps selectors identical to the original script.
    """
    # Prepare values
    payload = {
        "#Lastname": _safe_str(df.get("Last Name", [""])[i]),
        "#Firstname": _safe_str(df.get("First Name", [""])[i]),
        "#DateOfBirth": _fmt_date_mmddyyyy(df.get("Date of Birth", [""])[i]),
        "#TraveldocumentNumber": _safe_str(df.get("Passport Number", [""])[i]),
        "#Street": _safe_str(df.get("Street", [""])[i]),
        "#Postcode": _safe_str(int(df.get("Postcode", [0])[i])) if str(df.get("Postcode", [""])[i]).strip() not in ("", "nan") else "",
        "#City": _safe_str(df.get("City", [""])[i]),
        "#Telephone": _safe_str(_safe_str(df.get("Telephone", [""])[i]).split(".")[0]),
        "#Email": _safe_str(df.get("E-Mail", [""])[i]),
        "#LastnameAtBirth": _safe_str(df.get("Name at Birth", [""])[i]),
        "#PlaceOfBirth": _safe_str(df.get("Place of Birth", [""])[i]),
        "#TraveldocumentDateOfIssue": _fmt_date_mmddyyyy(df.get("Date of Issue", [""])[i]),
        "#TraveldocumentValidUntil": _fmt_date_mmddyyyy(df.get("Passport Valid Until", [""])[i]),
    }

    # Dropdown labels (match by visible text)
    gender_value = _safe_str(df.get("Sex", [""])[i]).lower()
    gender_map = {
        "male": "Male", "m": "Male", "1": "Male",
        "female": "Female", "f": "Female", "2": "Female",
        "unknown": "Unknown", "u": "Unknown", "3": "Unknown",
        "not applicable": "Not Applicable", "na": "Not Applicable", "4": "Not Applicable", "n/a": "Not Applicable"
    }
    selects = {
        "#Sex": gender_map.get(gender_value, gender_value.title() if gender_value else ""),
        "#Country": _safe_str(df.get("Country", [""])[i]),
        "#NationalityAtBirth": _safe_str(df.get("Nationality at Birth", [""])[i]),
        "#CountryOfBirth": _safe_str(df.get("Country of Birth", [""])[i]),
        "#NationalityForApplication": _safe_str(df.get("Actual Nationality", [""])[i]),
        "#TraveldocumentIssuingAuthority": _safe_str(df.get("Passport Issued By", [""])[i]),
    }

    # Wait once for the form to exist
    await page.wait_for_selector("#Lastname", timeout=30000)

    # Inject once: set values, select options by label, dispatch events
    # Wait briefly for dropdown options to be populated (single check)
    required_selectors = [k for k, v in selects.items() if v]
    try:
        if required_selectors:
            await page.wait_for_function(
                """(sels) => sels.every(s => {
                    const el = document.querySelector(s);
                    return el && el.options && el.options.length > 1;
                })""",
                required_selectors,
                timeout=5000
            )
    except Exception:
        pass

    missing_selects = await page.evaluate(
        """
        ({payload, selects}) => {
          const fire = (el) => {
            if (!el) return;
            el.dispatchEvent(new Event('input', { bubbles: true }));
            el.dispatchEvent(new Event('change', { bubbles: true }));
          };

          // text inputs
          for (const [sel, val] of Object.entries(payload)) {
            const el = document.querySelector(sel);
            if (!el) continue;
            el.value = val ?? "";
            fire(el);
          }

          // selects by label (robust: normalize + contains fallback)
          const norm = (s) => String(s ?? "").trim().toLowerCase().replace(/\s+/g, " ");
          const pickOptionByLabel = (el, wantedLabel) => {
            const w = norm(wantedLabel);
            if (!w) return false;
            const opts = Array.from(el.options || []);
            // exact match (normalized)
            let match = opts.find(o => norm(o.textContent || "") === w);
            // contains match
            if (!match) match = opts.find(o => norm(o.textContent || "").includes(w));
            // reverse contains (rare)
            if (!match) match = opts.find(o => w.includes(norm(o.textContent || "")) && norm(o.textContent || "").length > 2);
            if (match) {
              el.value = match.value;
              fire(el);
              // some forms require blur to trigger validation
              try { el.blur(); } catch (e) {}
              return true;
            }
            return false;
          };

          const missingSelects = [];
          for (const [sel, label] of Object.entries(selects)) {
            if (!label) continue;
            const el = document.querySelector(sel);
            if (!el) { missingSelects.push(`${sel}: element not found`); continue; }
            const ok = pickOptionByLabel(el, label);
            if (!ok) missingSelects.push(`${sel}: no option match for "${label}"`);
          }

          // accept terms checkbox if present
          const cb = document.querySelector('#DSGVOAccepted');
          if (cb && !cb.checked) {
            cb.checked = true;
            fire(cb);
          }

          return missingSelects;
        }
        """,
        {"payload": payload, "selects": selects}
    )

    # Log missing dropdown matches (if any)
    try:
        if missing_selects:
            print("⚠️ Bulk fill: some dropdowns did not match options:", missing_selects)
    except Exception:
        pass

    # Quick sanity check (best-effort)
    try:
        v = await page.input_value("#Lastname")
        if not v:
            await page.fill("#Lastname", payload["#Lastname"])
    except Exception:
        pass



# === CAPTCHA Prefetch (SAFE) ===
async def prefetch_captcha_image_b64(page):
    """
    SAFE prefetch: waits for the CAPTCHA image to appear and downloads it in-memory as base64.
    Tؤرؤ.
    """
    try:
        captcha_img = await page.wait_for_selector("#Captcha_CaptchaImage", timeout=15000)
        captcha_src = await captcha_img.get_attribute("src")
        if not captcha_src:
            return None
        captcha_url = urljoin(page.url, captcha_src)
        resp = await page.request.get(captcha_url)
        if not resp.ok:
            return None
        captcha_bytes = await resp.body()
        return base64.b64encode(captcha_bytes).decode("utf-8")
    except Exception:
        return None


# === Smart slot click ===
import asyncio
import random

async def smart_click_slot(page, browser_id, preferred_week, max_retries=50):
    """
    Smart slot selection and click for multi-browser booking.
    - Uses preferred_week from Excel:
        - "Next Week" → keep going to Next Week until button found and slots visible
        - "Current Week" → book slots in current week
    """
    try:
        retries = 0
        while retries < max_retries:

            if stop_event.is_set():
                print(f"[Browser {browser_id}] ⛔ Stopping due to stop_event.")
                await page.context.close()
                return False

            if not is_allowed_time():
                print(f"[Browser {browser_id}] ⛔ Outside allowed time window.")
                stop_event.set()
                await page.context.close()
                return False

            await asyncio.sleep(random.uniform(0.3, 0.8))  # random wait

            # ------------------- Handle Next Week preference -------------------
            if preferred_week.strip().lower() == "next week":
                next_week_found = False
                while not next_week_found:
                    try:
                        next_week_btn = await page.wait_for_selector('//input[@value="Next week"]', timeout=2000)
                        await next_week_btn.click()
                        print(f"[Browser {browser_id}] ⏭️ Excel says 'Next Week' → clicked Next Week")
                        await asyncio.sleep(1)
                        next_week_found = True
                    except Exception:
                        print(f"[Browser {browser_id}] ⚠️ Next Week button not found, refreshing...")
                        await page.reload()
                        await asyncio.sleep(random.uniform(1, 2))
                        continue
            # -------------------------------------------------------------------

            # ------------------- Get all available slots -------------------
            try:
                slot_buttons = await page.query_selector_all('//input[@name="Start"]')
                total = len(slot_buttons)
                if total == 0:
                    raise Exception("No slots found")
            except Exception:
                print(f"[Browser {browser_id}] ❌ No slots found, refreshing...")
                await page.reload()
                await asyncio.sleep(random.uniform(1, 2))
                retries += 1
                continue

            # ---------------- HANDLE SINGLE OR MULTIPLE SLOTS -----------------
            if total == 1:
                target_index = 0
                claim_key = f"index:{target_index}"
                async with lock:
                    if claim_key in claimed_slots:
                        print(f"[Browser {browser_id}] 😐 Only one slot available, but already claimed. Waiting...")
                        await asyncio.sleep(1)
                        continue
                    claimed_slots.add(claim_key)
                    print(f"[Browser {browser_id}] 🔹 Only one slot available. Claimed slot #1")
            else:
                available_indices = []
                for idx in range(total):
                    claim_key = f"index:{idx}"
                    async with lock:
                        if claim_key not in claimed_slots:
                            available_indices.append(idx)

                if not available_indices:
                    print(f"[Browser {browser_id}] 😐 All visible slots are claimed. Refreshing...")
                    await page.reload()
                    await asyncio.sleep(random.uniform(1.2, 2.5))
                    retries += 1
                    continue

                target_index = available_indices[-1]
                claim_key = f"index:{target_index}"
                async with lock:
                    if claim_key in claimed_slots:
                        continue  
                    claimed_slots.add(claim_key)
                    print(f"[Browser {browser_id}] 🔹 Claimed slot #{target_index + 1} for clicking")

            # ---------------- CLICK SLOT -----------------
            slot_btn = slot_buttons[target_index]
            try:
                await slot_btn.scroll_into_view_if_needed()
                await slot_btn.click()
                print(f"[Browser {browser_id}] 🎯 Slot #{target_index + 1} clicked successfully.")
                return True

            except Exception as e:
                print(f"[Browser {browser_id}] ⚠️ Failed to click slot #{target_index + 1}: {e}")
                async with lock:
                    claimed_slots.remove(claim_key)
                await page.reload()
                await asyncio.sleep(random.uniform(1.2, 2.5))
                retries += 1
                continue

        print(f"[Browser {browser_id}] ❌ Max retries reached. Could not claim a slot.")
        return False

    except Exception as e:
        print(f"[Browser {browser_id}] ❗ Unexpected error: {e}")
        return False


# === Book appointment ===
import asyncio
import pandas as pd

async def book_appointment(i, df, selected_value):
    from playwright.async_api import async_playwright
    import time

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()

        try:
            await page.goto("https://appointment.bmeia.gv.at/?Office=Kairo")
            await asyncio.sleep(0.3)

            # Accept cookies
            try:
                accept = await page.wait_for_selector('//button[contains(text(), "Accept")]', timeout=1000)
                await accept.click()
            except:
                pass

            category_map = {
                "aufenthaltsbewilligung student (nur bachelor)": "44281520",
                "aufenthaltsbewilligung student (nur master, phd und stipendiate)": "44279679",
                "aufenthaltstitel rot-weiß-rot karte (nur für personen die selbst in österreich arbeiten wollen)": "26425165",
                "familienzusammenführung gem. nag (nur für ägypterinnen)": "32528820",
                "familienzusammenführung gem. nag (nur für sonstige staatsangehörige mit wohnsitz)": "32529659",
                "österreicher personenstandsangelegenheiten (eheschließung etc.)": "20950851",
                "österreicher (reisepässe, staatsbürgerschaft)": "134608"
            }

            df = pd.read_excel(EXCEL_FILE)
            reservation = df["Reservation for"][i].strip().lower()
            selected_value = category_map.get(reservation)

            if not selected_value:
                print(f"❌ Unknown reservation category in Excel: '{reservation}'")
                await browser.close()
                return

            attempt = 0
            while True:
                attempt += 1
                print(f"🔄 Attempt {attempt}: Checking for category '{reservation}'...")

                try:
                    # ✅ Correct select
                    select_elem = await page.wait_for_selector(
                        'select#CalendarId',
                        timeout=10000
                    )

                    # Wait until the option exists (NOT visible)
                    await page.wait_for_selector(
                        f'select#CalendarId option[value="{selected_value}"]',
                        state="attached",
                        timeout=5000
                    )

                    # Debug: list categories
                    options = await select_elem.query_selector_all('option')
                    for opt in options:
                        val = await opt.get_attribute('value')
                        txt = await opt.inner_text()
                        print(f"Option found: value={val}, text={txt}")

                    # ✅ Correct selection
                    await select_elem.select_option(value=selected_value)

                    print(f"✅ Selected reservation category for: {reservation}")
                    break

                except Exception as e:
                    print(f"⚠️ Attempt {attempt} failed: {e}")

                    if not is_allowed_time():
                        print(f"⛔ Outside allowed time window. Stopping browser.")
                        stop_event.set()
                        await browser.close()
                        return

                    await asyncio.sleep(1)
                    await page.reload()

            # Click 'Next' up to 3 times
            for _ in range(3):
                start = time.time()
                while time.time() - start < 300:
                    old_source = await page.content()
                    try:
                        btn = await page.wait_for_selector('//input[@value="Next"]', timeout=5000)
                        await btn.click()
                        await asyncio.sleep(0.2)
                        new_source = await page.content()
                        if new_source != old_source:
                            print("✅ Click successful — page changed")
                            break
                        else:
                            print("⚠️ Click attempted but page did not change, retrying...")
                            continue
                    except Exception as e:
                        print(f"⚠️ Click failed, retrying... ({e})")
                        continue

            # Slot selection and personal info filling
            attempt = 0
            while True:
                attempt += 1
                print(f"🔁 Attempt {attempt}...")
                try:
                    preferred_week = str(df["PreferredWeek"][i]).strip()
                    await smart_click_slot(page, i + 1, preferred_week, max_retries=50)

                    await page.screenshot(path="slot_available.png")
                    next_btn = await page.wait_for_selector('//input[@value="Next"]')
                    await next_btn.click()
                    await asyncio.sleep(0.2)

                    
                    # Start CAPTCHA image prefetch in parallel while filling the form (safe: no solving)
                    captcha_prefetch_task = asyncio.create_task(prefetch_captcha_image_b64(page))
# ---- Personal Info ----
                    
                    # ---- Personal Info (FAST: bulk injection + events) ----
                    await bulk_fill_form_once(page, df, i)
                    print("✅ Bulk form fill completed")

                    # ✅ Validate bulk fill quickly; if OK, skip the slow field-by-field fills
                    bulk_ok = False
                    try:
                        ln = await page.input_value("#Lastname")
                        em = await page.input_value("#Email")
                        # validate key dropdowns to avoid skipping when selects didn't match
                        sex_val = await page.input_value("#Sex")
                        country_val = await page.input_value("#Country")
                        nat_birth_val = await page.input_value("#NationalityAtBirth")
                        nat_app_val = await page.input_value("#NationalityForApplication")
                        cob_val = await page.input_value("#CountryOfBirth")
                        issued_val = await page.input_value("#TraveldocumentIssuingAuthority")
                        bulk_ok = (
                            bool(ln.strip()) and bool(em.strip()) and
                            bool(sex_val.strip()) and bool(country_val.strip()) and
                            bool(nat_birth_val.strip()) and bool(nat_app_val.strip()) and
                            bool(cob_val.strip()) and bool(issued_val.strip())
                        )
                    except Exception:
                        bulk_ok = False

                    if bulk_ok:
                        print("⚡ Bulk fill validated — skipping field-by-field form steps.")
                    else:
                        print("⚠️ Bulk fill validation failed — falling back to field-by-field fill.")


                    if not bulk_ok:
                        # ---- Gender ----
                                            try:
                                                # Normalize Excel value and map to exact dropdown text
                                                gender_value = str(df["Sex"][i]).strip().lower()
                                                gender_map = {
                                                    "male": "Male",
                                                    "m": "Male",
                                                    "1": "Male",
                                                    "female": "Female",
                                                    "f": "Female",
                                                    "2": "Female",
                                                    "unknown": "Unknown",
                                                    "u": "Unknown",
                                                    "3": "Unknown",
                                                    "not applicable": "Not Applicable",
                                                    "na": "Not Applicable",
                                                    "4": "Not Applicable",
                                                    "n/a": "Not Applicable"
                                                }
                                                gender_value = gender_map.get(gender_value, gender_value)

                                                # Wait for dropdown (0.3s max)
                                                dropdown = await page.wait_for_selector('#Sex', timeout=200)

                                                # Select by exact label
                                                await dropdown.select_option(label=gender_value)
                                                print(f"✅ Gender Added --- {gender_value}")

                                            except Exception as e:
                                                print(f"⚠️ Gender not available: {e}")


                                            # ---- Street, ZIP, City ----
                                            try:
                                                await page.wait_for_selector('#Street', timeout=200)
                                                await page.fill('#Street', str(df["Street"][i]))
                                            
                                            except:
                                                print("Street not add")

                                            try:
                                                await page.wait_for_selector('#Postcode', timeout=200)
                                                await page.fill('#Postcode', str(int(df["Postcode"][i])))
                                            
                                            except:
                                                pass

                                            try:
                                                await page.wait_for_selector('#City', timeout=200)
                                                await page.fill('#City', str(df["City"][i]))
                                            
                                            except:
                                                print("City Not add")

                                            # ---- Country ----
                                            try:
                                                await page.wait_for_selector('#Country', timeout=200)
                                                await page.select_option('#Country', label=str(df["Country"][i]))
                                                print("✅ Country Added")
                                            
                                            except:
                                                print("Country not add")

                                            # ---- Phone, Email ----
                                            try:
                                                await page.wait_for_selector('#Telephone', timeout=200)
                                                await page.fill('#Telephone', str(df["Telephone"][i]).split('.')[0])
                                            
                                            except:
                                                print("Telephone not add")

                                            try:
                                                await page.wait_for_selector('#Email', timeout=200)
                                                await page.fill('#Email', str(df["E-Mail"][i]))
                                            
                                            except:
                                                print("Email Not add")

                                            # ---- Birth Details ----
                                            try:
                                                await page.wait_for_selector('#LastnameAtBirth', timeout=200)
                                                await page.fill('#LastnameAtBirth', str(df["Name at Birth"][i]))
                                            
                                            except:
                                                print("Name at Birth Not add")

                                            try:
                                                await page.wait_for_selector('#NationalityAtBirth', timeout=200)
                                                await page.select_option('#NationalityAtBirth', label=str(df["Nationality at Birth"][i]))
                                            
                                            except:
                                                print("Nationality at Birth not add")

                                            try:
                                                await page.wait_for_selector('#CountryOfBirth', timeout=200)
                                                await page.select_option('#CountryOfBirth', label=str(df["Country of Birth"][i]))
                                            
                                            except:
                                                print("Country of Birth not add")

                                            try:
                                                await page.wait_for_selector('#PlaceOfBirth', timeout=200)
                                                await page.fill('#PlaceOfBirth', str(df["Place of Birth"][i]))
                                            
                                            except:
                                                print("Place of Birth not add")

                                            # ---- Actual Nationality ----
                                            try:
                                                await page.wait_for_selector('#NationalityForApplication', timeout=200)
                                                await page.select_option('#NationalityForApplication', label=str(df["Actual Nationality"][i]))
                                            
                                            except:
                                                print("Actual Nationality not add")

                                            # ---- Passport Dates ----
                                            try:
                                                await page.wait_for_selector('#TraveldocumentDateOfIssue', timeout=200)
                                                date_of_issue = pd.to_datetime(df["Date of Issue"][i]).strftime("%m/%d/%Y")
                                                await page.fill('#TraveldocumentDateOfIssue', date_of_issue)

                                                valid_until = pd.to_datetime(df["Passport Valid Until"][i]).strftime("%m/%d/%Y")
                                                await page.fill('#TraveldocumentValidUntil', valid_until)

                                            
                                            except:
                                                print("Passport dates not add")

                                            try:
                                                await page.wait_for_selector('#TraveldocumentIssuingAuthority', timeout=200)
                                                await page.select_option('#TraveldocumentIssuingAuthority', label=str(df["Passport Issued By"][i]))
                                            
                                            except:
                                                print("Passport Issued By not add")

                                            # ---- Accept Terms ----
                                            try:
                                                # Wait max 3 seconds for checkbox
                                                await page.wait_for_selector('#DSGVOAccepted', timeout=200)
                                                await page.click('#DSGVOAccepted')
                                            
                                            except:
                                                print("⚠️ Terms checkbox not available, skipped")


                    

                    # CAPTCHA
                    # If CAPTCHA image was prefetched, log it (no change to solving pipeline)
                    try:
                        if 'captcha_prefetch_task' in locals():
                            if captcha_prefetch_task.done():
                                _b64 = captcha_prefetch_task.result()
                            else:
                                _b64 = None
                            if _b64:
                                print("📌 CAPTCHA image prefetched (base64 ready).")
                    except Exception:
                        pass

                    for retry_captcha in range(5):
                        captcha_text = await solve_image_captcha(page)

                        if captcha_text:
                            # Ensure CAPTCHA input is visible
                            await page.evaluate("""
                                () => {
                                    const el = document.querySelector('#CaptchaText');
                                    if (el) {
                                        el.scrollIntoView({ behavior: 'instant', block: 'center' });
                                        el.focus();
                                    }
                                }
                            """)
                            await asyncio.sleep(0.2)

                            # CAPTCHA input expects UPPERCASE
                            captcha_text = captcha_text.strip().upper()

                            # Clear then fill
                            await page.fill('#CaptchaText', '')
                            await page.type('#CaptchaText', captcha_text, delay=50)

                            print("🔐 CAPTCHA Text Entered:", captcha_text)
                            await asyncio.sleep(0.3)

                            # Click Next button
                            await page.click('#nextButton')
                            await asyncio.sleep(2)

                            # Detect CAPTCHA error
                            page_html = await page.content()
                            if "Captcha: The text from the picture does not match with your entry" in page_html:
                                print("❌ CAPTCHA incorrect. Retrying...")
                                continue

                            print("✅ CAPTCHA accepted")
                            break
                        else:
                            print("🔁 CAPTCHA solve failed, retrying...")



                    # Confirmation
                    try:
                        await page.wait_for_selector("//h2[contains(text(), 'Confirmation of reservation')]", timeout=5000)
                        print("✅ Reservation Booked Successfully")
                        df.at[i, 'Status'] = 'Success'
                        df.to_excel(EXCEL_FILE, index=False)
                        screenshot_name = f"Success_{df['First Name'][i]}_{df['Last Name'][i]}.png"
                        await page.screenshot(path=screenshot_name)
                        await send_telegram_alert_with_photo(BOT_TOKEN, CHAT_ID,f"✅ Reservation booked successfully for: {df['First Name'][i]} {df['Last Name'][i]}",screenshot_name)
                    except:  
                        # Nested error handling as before
                        try:
                            error_element = await page.wait_for_selector('//p[@class="message-error" and contains(text(), "The appointment was unfortunately allocated otherwise in the meantime.")]',timeout=500)
                            text = await error_element.inner_text()
                            print("Error Message:", text)
                        except:
                            try:
                                error_div = await page.wait_for_selector("//div[@class='validation-summary-errors']",timeout=500)
                                summary_span = await error_div.query_selector("span")
                                summary_text = await summary_span.inner_text()
                                print(f"⚠️ {summary_text}")
                            except:
                                try:
                                    error_massage1 = await page.wait_for_selector(
                                        '//p[@class="message-error" and contains(text(), "You have already reserved an appointment.")]',timeout=500)
                                    error_massage1_text = await error_massage1.inner_text()
                                    print("Error:", error_massage1_text)
                                    df.at[i, 'Status'] = 'Success'
                                    df.to_excel(EXCEL_FILE, index=False)
                                except:
                                    pass

                    break

                except Exception as e:
                    print("❌ No slots or error occurred. Refreshing...", e)
                    await page.reload()
                    await asyncio.sleep(0.5)

        except Exception as e:
            print(f"⚠️ Error occurred: {e}")
            async with lock:
                claimed_slots.clear()

        finally:
            await browser.close()
            async with lock:
                claimed_slots.clear()



# === MAIN EXECUTION (Async Playwright Version) ===
import datetime
import asyncio
import pandas as pd
from asyncio import Lock, Event

# === Configuration ===
# ------------------------------------------------------------------
MAX_CONCURRENT = 1  # corresponds to your MAX_THREADS
# ------------------------------------------------------------------
EXCEL_FILE = "test13.xlsx"

# === Globals ===
lock = Lock()  # asyncio lock for async context
stop_event = Event()
claimed_slots = set()

# === Telegram Notification Function ===
import requests
async def send_telegram_alert(bot_token, chat_id, message):
    try:
        await asyncio.to_thread(
            lambda: requests.get(
                f"https://api.telegram.org/bot{bot_token}/sendMessage",
                params={"chat_id": chat_id, "text": message}
            )
        )
    except Exception as e:
        print(f"⚠️ Telegram alert failed: {e}")


# === TIME WINDOW CHECK ===
def is_allowed_time():
    now = datetime.datetime.now().time()
    morning_start = datetime.time(1, 10)
    morning_end = datetime.time(23, 58)
    evening_start = datetime.time(0, 10)
    evening_end = datetime.time(0, 59)
    return (morning_start <= now <= morning_end) or (evening_start <= now <= evening_end)


# === PICK NEXT UNSUCCESSFUL USER ===
async def claim_next_pending_user():
    global claimed_slots
    async with lock:
        df = pd.read_excel(EXCEL_FILE)
        if "Status" not in df.columns:
            df["Status"] = ""
            df.to_excel(EXCEL_FILE, index=False)

        for i, row in df.iterrows():
            status = str(row.get("Status")).strip().lower()
            if status in ("success", "in progress"):
                continue
            if i in claimed_slots:
                continue
            if row.drop(labels=["Status"]).isnull().any():
                print(f"[Row {i}] ⚠️ Missing data, skipping.")
                continue
            claimed_slots.add(i)
            df.at[i, "Status"] = "In Progress"
            df.to_excel(EXCEL_FILE, index=False)
            print(f"[Row {i}] 🟡 Claimed for processing.")
            return i, row
        return None, None


# === PROCESS ONE USER ===
async def process_user():
    i, row = await claim_next_pending_user()
    if i is None:
        print("🎉 No pending users found.")
        return False

    try:
        reservation = row["Reservation for"].strip().lower()
        selected_value = CATEGORY_MAP.get(reservation)
        if not selected_value:
            print(f"❌ Unknown category in row {i + 1}: {reservation}")
            return True

        print(f"🚀 Starting booking for Row {i + 1}: {row['First Name']} {row['Last Name']}")
        # === Your existing async Playwright booking function ===
        await book_appointment(i, pd.read_excel(EXCEL_FILE), selected_value)

        # Reload Excel to verify result
        df = pd.read_excel(EXCEL_FILE)
        status = str(df.loc[i, "Status"]).strip().lower()
        if status == "success":
            print(f"✅ Row {i + 1} successfully booked.")
        else:
            print(f"❌ Row {i + 1} failed. It will be retried next run.")
            async with lock:
                df.at[i, "Status"] = ""
                df.to_excel(EXCEL_FILE, index=False)

    except Exception as e:
        print(f"⚠️ Error for Row {i + 1}: {e}")
        async with lock:
            df = pd.read_excel(EXCEL_FILE)
            df.at[i, "Status"] = ""
            df.to_excel(EXCEL_FILE, index=False)

    finally:
        async with lock:
            if i in claimed_slots:
                claimed_slots.remove(i)

    return True


# === CLEANUP IN-PROGRESS ENTRIES ===
async def cleanup_in_progress():
    try:
        df = pd.read_excel(EXCEL_FILE)
        if "Status" in df.columns:
            count = (df["Status"].astype(str).str.lower() == "in progress").sum()
            if count > 0:
                df.loc[df["Status"].astype(str).str.lower() == "in progress", "Status"] = ""
                df.to_excel(EXCEL_FILE, index=False)
                print(f"🧹 Reset {count} 'In Progress' entries.")
            else:
                print("✔ No 'In Progress' entries found.")
    except Exception as e:
        print(f"⚠️ Cleanup error: {e}")


# === MAIN SCHEDULER LOOP ===
async def scheduler_loop():
    consecutive_no_users = 0
    while True:
        if not is_allowed_time():
            print("\n⏸ Outside allowed time window. Cleaning up and sleeping 1 minute...")
            await cleanup_in_progress()
            stop_event.set()
            consecutive_no_users = 0
            await asyncio.sleep(60)
            continue

        print("\n🕔 Within allowed time window. Starting processing...")
        stop_event.clear()
        await cleanup_in_progress()

        tasks = set()
        for _ in range(MAX_CONCURRENT):
            tasks.add(asyncio.create_task(process_user()))

        while tasks:
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for finished in done:
                try:
                    result = finished.result()
                except Exception as e:
                    print(f"⚠️ Task crashed: {e}")
                    result = True

                if result is False:
                    consecutive_no_users += 1
                    print(f"🟡 No pending users found ({consecutive_no_users})")
                    if consecutive_no_users >= 3:
                        print("🎉 All users processed. Scheduler stopping.")
                        try:
                            await send_telegram_alert(BOT_TOKEN, CHAT_ID,
                                                      "🎉 All users processed. Scheduler stopped.")
                        except:
                            pass
                        return
                else:
                    consecutive_no_users = 0

                print("🚀 Launching new browser to replace finished one...")
                tasks.add(asyncio.create_task(process_user()))
                tasks.remove(finished)

        await asyncio.sleep(1)


# === ENTRY POINT ===
if __name__ == "__main__":
    print("🚀 Scheduler Started")
    asyncio.run(scheduler_loop())
    print("🛑 Scheduler Ended")
