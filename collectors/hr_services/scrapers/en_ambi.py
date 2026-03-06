"""アンビ スクレイパー - https://en-ambi.com/search/"""

import re
from bs4 import BeautifulSoup

from scrapers.base import BaseScraper


class Scraper(BaseScraper):
    service_name = "en_ambi"
    output_filename = "en_ambi.csv"           # 企業単位 dedup 済み
    output_filename_raw = "en_ambi_raw.csv"   # 求人単位 raw

    BASE_URL = "https://en-ambi.com/search/"

    def scrape(self) -> list[dict]:
        page = 1
        while True:
            if page == 1:
                url = f"{self.BASE_URL}?jobmerit=350&krt=top"
            else:
                url = f"{self.BASE_URL}?jobmerit=350&krt=top&per_page={page}"

            self.logger.info(f"Page {page}: {url}")
            html = self.client.fetch_requests(url)
            if html is None:
                self.logger.warning(f"Page {page}: HTML取得失敗")
                break

            companies = self._parse_page(html)
            if not companies:
                self.logger.info(f"Page {page}: 結果なし → 終了")
                break

            self.results.extend(companies)
            self.logger.info(
                f"Page {page}: {len(companies)}件取得（累計: {len(self.results)}件）"
            )
            page += 1
            self.client.sleep()

        return self.results

    def _parse_page(self, html: str) -> list[dict]:
        """HTML解析で求人情報を抽出"""
        soup = BeautifulSoup(html, "lxml")
        items = []

        # 求人カード: div.jobUnit
        job_units = soup.select("div.jobUnit")

        for unit in job_units:
            # 企業名: span.companyName
            company_el = unit.select_one("span.companyName")
            company_name = company_el.get_text(strip=True) if company_el else ""

            # 求人タイトル: a.catch
            title_el = unit.select_one("a.catch")
            title = title_el.get_text(strip=True) if title_el else ""

            # 掲載日: div.term span.data
            # "26/03/06～26/03/19" → "2026-03-06～2026-03-19"
            date_el = unit.select_one("div.term span.data")
            posting_date = ""
            if date_el:
                date_text = date_el.get_text(strip=True)
                dates = re.findall(r"(\d{2})/(\d{2})/(\d{2})", date_text)
                if dates:
                    # 終了日のみ取得（最後の日付）
                    y, m, d = dates[-1]
                    posting_date = f"20{y}-{m}-{d}"

            if company_name:
                items.append({
                    "企業名": company_name,
                    "タイトル": title,
                    "掲載日": posting_date,
                })

        return items


if __name__ == "__main__":
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

    import logging
    from config import LOG_DIR

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        handlers=[
            logging.FileHandler(LOG_DIR / "scraper.log", encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )
    scraper = Scraper()
    scraper.run()
