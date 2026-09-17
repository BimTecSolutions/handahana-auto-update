"""
Handahana — incremental Firestore updater.

Same pattern as the Suba Dawasak incremental scraper:
  1. Ask Firestore what the latest draw number already stored is.
  2. Try draw numbers after that one, one by one, against the NLB site.
  3. Upload every new draw found, stopping after a run of consecutive
     misses (meaning we've caught up to "not published yet").

No input() prompts — everything is controlled by environment variables,
so this can run unattended on a schedule (e.g. GitHub Actions).

Environment variables:
  FIREBASE_CREDENTIALS        Path to the service account JSON file.
                               Default: serviceAccountKey.json
  START_DRAW_FALLBACK         Draw number to start from if Firestore has
                               no Handahana results yet at all. Default: 1
  MAX_CONSECUTIVE_MISSES      How many draw numbers in a row can fail to
                               resolve before we assume we've caught up
                               and stop. Default: 3
  MAX_NEW_DRAWS_PER_RUN       Safety cap on how many new draws a single
                               run will attempt. Default: 20
  DRAW_DELAY_SECONDS          Delay between draw attempts. Default: 1.0
"""

import os
import re
import time
from datetime import datetime
from typing import Optional, Dict

import requests
from bs4 import BeautifulSoup

import firebase_admin
from firebase_admin import credentials, firestore


class HandahanaScraper:
    BASE_URLS = [
        "https://www.nlb.lk",
        "https://r.nlb.lk",
    ]

    LOTTERY_SLUG = "handahana"
    LOTTERY_NAME = "Handahana"

    FIRESTORE_COLLECTION = "NLB_Results_All"
    FIRESTORE_DOCUMENT = "handahana"
    FIRESTORE_SUBCOLLECTION = "results"

    MAX_REQUEST_RETRIES = 3
    PAGE_WAIT = 4

    ZODIAC_SIGNS = {
        "ARIES", "TAURUS", "GEMINI", "CANCER", "LEO", "VIRGO",
        "LIBRA", "SCORPIO", "SAGITTARIUS", "CAPRICORN",
        "AQUARIUS", "PISCES",
    }

    def __init__(self, firebase_creds_path: str):
        self.db = None
        self.driver = None

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/139.0.0.0 Safari/537.36"
            ),
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;q=0.9,"
                "image/avif,image/webp,image/apng,*/*;q=0.8"
            ),
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.nlb.lk/",
            "Connection": "keep-alive",
        })

        self.init_firebase(firebase_creds_path)

    # ------------------------------------------------------------
    # FIREBASE
    # ------------------------------------------------------------
    def init_firebase(self, creds_path: str):
        if not firebase_admin._apps:
            cred = credentials.Certificate(creds_path)
            firebase_admin.initialize_app(cred)

        self.db = firestore.client()
        print("✅ Firebase initialized.")

    def get_results_collection(self):
        return (
            self.db
            .collection(self.FIRESTORE_COLLECTION)
            .document(self.FIRESTORE_DOCUMENT)
            .collection(self.FIRESTORE_SUBCOLLECTION)
        )

    def get_latest_draw_number(self) -> Optional[int]:
        """Return the highest draw_number currently stored, or None if empty."""
        query = (
            self.get_results_collection()
            .order_by("draw_number", direction=firestore.Query.DESCENDING)
            .limit(1)
        )

        docs = list(query.stream())

        if not docs:
            return None

        return docs[0].to_dict().get("draw_number")

    def upload_to_firestore(self, result: Dict) -> bool:
        draw_str = result["draw_number_str"]
        document_id = f"handahana_{draw_str}"

        try:
            self.get_results_collection().document(document_id).set(
                result, merge=True
            )
            print(f"   ☁️ Uploaded: {document_id}")
            return True
        except Exception as e:
            print(f"   ❌ Firestore upload failed: {e}")
            return False

    # ------------------------------------------------------------
    # SELENIUM
    # ------------------------------------------------------------
    def init_selenium(self):
        if self.driver:
            return self.driver

        try:
            from selenium import webdriver
            from selenium.webdriver.chrome.options import Options

            options = Options()
            options.add_argument("--headless=new")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-gpu")
            options.add_argument("--window-size=1920,1080")
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_experimental_option("excludeSwitches", ["enable-automation"])
            options.add_experimental_option("useAutomationExtension", False)
            options.add_argument("--disable-notifications")
            options.add_argument("--disable-popup-blocking")

            self.driver = webdriver.Chrome(options=options)

            try:
                self.driver.execute_script(
                    "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
                )
            except Exception:
                pass

            print("✅ Selenium Chrome initialized.")
            return self.driver

        except Exception as e:
            print(f"❌ Selenium initialization failed: {e}")
            return None

    def fetch_with_selenium(self, url: str) -> Optional[str]:
        driver = self.init_selenium()
        if not driver:
            return None

        for attempt in range(1, self.MAX_REQUEST_RETRIES + 1):
            try:
                driver.get(url)
                time.sleep(self.PAGE_WAIT)

                html = driver.page_source
                try:
                    body_text = driver.find_element("tag name", "body").text
                except Exception:
                    body_text = ""

                combined = (html + " " + body_text).lower()
                indicators = ["handahana", "draw", "results"]
                found = sum(1 for item in indicators if item in combined)

                if len(html) > 2000 and found >= 1:
                    return html

            except Exception as e:
                print(f"   ⚠️ Selenium error: {e}")

            if attempt < self.MAX_REQUEST_RETRIES:
                time.sleep(attempt * 2)

        return None

    def fetch_with_requests(self, url: str) -> Optional[str]:
        for attempt in range(1, self.MAX_REQUEST_RETRIES + 1):
            try:
                response = self.session.get(url, timeout=30, allow_redirects=True)
                if response.status_code == 200 and len(response.text) > 2000:
                    return response.text
            except Exception as e:
                print(f"   ⚠️ Request error: {e}")

            if attempt < self.MAX_REQUEST_RETRIES:
                time.sleep(attempt)

        return None

    def fetch_page(self, url: str) -> Optional[str]:
        html = self.fetch_with_selenium(url)
        if html:
            return html
        return self.fetch_with_requests(url)

    # ------------------------------------------------------------
    # TEXT HELPERS
    # ------------------------------------------------------------
    def normalize_text(self, text: str) -> str:
        text = text.replace("\xa0", " ")
        text = re.sub(r"\s+", " ", text)
        return text.strip()

    # ------------------------------------------------------------
    # DATE
    # ------------------------------------------------------------
    def extract_date(self, text: str) -> Optional[str]:
        patterns = [
            r"Draw\s+Date\s*:?\s*([A-Za-z]+\s+\d{1,2},?\s+\d{4})",
            r"Date\s*:?\s*([A-Za-z]+\s+\d{1,2},?\s+\d{4})",
            r"([A-Za-z]+\s+\d{1,2},?\s+\d{4})",
            r"(\d{1,2}[/-]\d{1,2}[/-]\d{4})",
            r"(\d{4}[/-]\d{1,2}[/-]\d{1,2})",
        ]
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1)
        return None

    def parse_date(self, date_string: Optional[str]) -> Optional[str]:
        if not date_string:
            return None

        formats = [
            "%B %d, %Y", "%B %d %Y", "%b %d, %Y", "%b %d %Y",
            "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d", "%Y-%m-%d",
        ]
        for fmt in formats:
            try:
                return datetime.strptime(date_string.strip(), fmt).isoformat()
            except ValueError:
                continue
        return date_string

    # ------------------------------------------------------------
    # RESULT PARSING
    # ------------------------------------------------------------
    def get_zodiac_pattern(self):
        return r"\b(" + "|".join(sorted(self.ZODIAC_SIGNS, key=len, reverse=True)) + r")\b"

    def format_number(self, number: int) -> str:
        # 0-9 -> 01-09, 10-99 unchanged, 100 -> 100
        if 0 <= number <= 9:
            return f"{number:02d}"
        return str(number)

    def extract_main_result(self, text: str) -> Optional[Dict]:
        zodiac_pattern = self.get_zodiac_pattern()

        pattern = (
            zodiac_pattern
            + r"\s+(\d{1,3})\s+(\d{1,3})\s+(\d{1,3})\s+(\d{1,3})"
        )

        matches = list(re.finditer(pattern, text, re.IGNORECASE))

        for match in matches:
            zodiac = match.group(1).upper()
            raw_numbers = [match.group(2), match.group(3), match.group(4), match.group(5)]

            valid = True
            numbers_int = []
            for value in raw_numbers:
                try:
                    number = int(value)
                except ValueError:
                    valid = False
                    break
                if not (0 <= number <= 100):
                    valid = False
                    break
                numbers_int.append(number)

            if not valid:
                continue

            numbers_formatted = [self.format_number(n) for n in numbers_int]

            return {
                "zodiac": zodiac,
                "numbers": numbers_formatted,
                "numbers_int": numbers_int,
            }

        # Fallback: inspect the text after each zodiac occurrence directly
        zodiac_matches = list(re.finditer(zodiac_pattern, text, re.IGNORECASE))

        for zodiac_match in zodiac_matches:
            zodiac = zodiac_match.group(1).upper()
            section = text[zodiac_match.end():zodiac_match.end() + 180]

            numbers = re.findall(r"(?<!\d)(\d{1,3})(?!\d)", section)
            valid_numbers = []
            for value in numbers:
                try:
                    number = int(value)
                    if 0 <= number <= 100:
                        valid_numbers.append(number)
                except ValueError:
                    continue

            if len(valid_numbers) >= 4:
                numbers_formatted = [self.format_number(n) for n in valid_numbers[:4]]
                return {
                    "zodiac": zodiac,
                    "numbers": numbers_formatted,
                    "numbers_int": [int(x) for x in numbers_formatted],
                }

        return None

    # ------------------------------------------------------------
    # SCRAPE ONE DRAW
    # ------------------------------------------------------------
    def scrape_draw(self, draw_number: int) -> Optional[Dict]:
        draw_padded = f"{draw_number:04d}"
        print(f"\n🔎 Checking draw {draw_padded}...")

        number_variations = [str(draw_number), draw_padded]
        urls = []
        for base_url in self.BASE_URLS:
            for number_value in number_variations:
                urls.append(f"{base_url}/results/{self.LOTTERY_SLUG}/{number_value}")
                urls.append(f"{base_url}/results/{self.LOTTERY_SLUG}/{number_value}/")
        urls = list(dict.fromkeys(urls))

        for url in urls:
            html = self.fetch_page(url)
            if not html:
                continue

            soup = BeautifulSoup(html, "html.parser")
            text = soup.get_text(separator=" ", strip=True)
            text = self.normalize_text(text)

            if len(text) < 300:
                continue

            date = self.extract_date(text)
            main_result = self.extract_main_result(text)
            if not main_result:
                continue

            result = {
                "lottery": self.LOTTERY_NAME,
                "lottery_slug": self.LOTTERY_SLUG,
                "draw_number": draw_number,
                "draw_number_str": draw_padded,
                "date": date,
                "date_iso": self.parse_date(date),
                "main_result": main_result,
                "url": url,
                "scraped_at": datetime.now().isoformat(),
                "source": "National Lotteries Board, Sri Lanka",
            }

            print(f"   ✅ Found draw {draw_padded}: {main_result['zodiac']} "
                  f"{'-'.join(main_result['numbers'])}")

            return result

        print(f"   ⛔ Draw {draw_padded} not available yet.")
        return None

    # ------------------------------------------------------------
    # INCREMENTAL UPDATE
    # ------------------------------------------------------------
    def find_and_upload_new_draws(
        self,
        start_draw_fallback: int,
        max_consecutive_misses: int,
        max_new_draws: int,
        delay: float,
    ) -> int:
        latest_known = self.get_latest_draw_number()

        if latest_known is None:
            print(f"📭 No existing results in Firestore. Starting from "
                  f"{start_draw_fallback:04d}.")
            next_draw = start_draw_fallback
        else:
            print(f"📌 Latest draw already in Firestore: {latest_known:04d}")
            next_draw = latest_known + 1

        new_uploaded = 0
        consecutive_misses = 0
        attempts = 0

        while (
            consecutive_misses < max_consecutive_misses
            and attempts < max_new_draws
        ):
            attempts += 1
            result = self.scrape_draw(next_draw)

            if result:
                if self.upload_to_firestore(result):
                    new_uploaded += 1
                consecutive_misses = 0
            else:
                consecutive_misses += 1

            next_draw += 1
            time.sleep(delay)

        if new_uploaded == 0:
            print("\n✅ No new draws found — already up to date.")
        else:
            print(f"\n🎉 Uploaded {new_uploaded} new draw(s).")

        return new_uploaded

    # ------------------------------------------------------------
    # CLOSE
    # ------------------------------------------------------------
    def close(self):
        if self.driver:
            try:
                self.driver.quit()
                print("🛑 Selenium Chrome closed.")
            except Exception:
                pass
            self.driver = None


def main():
    creds_path = os.environ.get("FIREBASE_CREDENTIALS", "serviceAccountKey.json")
    start_draw_fallback = int(os.environ.get("START_DRAW_FALLBACK", "1"))
    max_consecutive_misses = int(os.environ.get("MAX_CONSECUTIVE_MISSES", "3"))
    max_new_draws = int(os.environ.get("MAX_NEW_DRAWS_PER_RUN", "20"))
    delay = float(os.environ.get("DRAW_DELAY_SECONDS", "1.0"))

    if not os.path.exists(creds_path):
        print(f"❌ Credentials file not found: {creds_path}")
        raise SystemExit(1)

    scraper = None

    try:
        scraper = HandahanaScraper(creds_path)
        scraper.find_and_upload_new_draws(
            start_draw_fallback=start_draw_fallback,
            max_consecutive_misses=max_consecutive_misses,
            max_new_draws=max_new_draws,
            delay=delay,
        )
    finally:
        if scraper:
            scraper.close()


if __name__ == "__main__":
    main()
