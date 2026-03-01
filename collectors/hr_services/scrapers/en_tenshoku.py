"""EN転職 スクレイパー - https://employment.en-japan.com/"""

import re
from bs4 import BeautifulSoup

from scrapers.base import BaseScraper


class Scraper(BaseScraper):
    service_name = "en_tenshoku"
    output_filename = "en_tenshoku.csv"           # 企業単位 dedup 済み
    output_filename_raw = "en_tenshoku_raw.csv"   # 求人単位 raw

    BASE_URL = "https://employment.en-japan.com/a/kanto/s_setsubikanri-unyu/"

    def scrape(self) -> list[dict]:
        page = 1
        while True:
            if page == 1:
                url = f"{self.BASE_URL}?caroute=1101&PK=F4DF97"
            else:
                url = f"{self.BASE_URL}{page}/?caroute=1101&PK=F4DF97"

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

        # 企業名要素: div.companyName 内の span.company
        company_divs = soup.select("div.companyName")

        for cd in company_divs:
            # 企業名
            company_el = cd.select_one("span.company")
            company_name = company_el.get_text(strip=True) if company_el else cd.get_text(strip=True)

            if not company_name or len(company_name) < 2:
                continue

            # 同じ親要素から求人タイトルを取得
            parent = cd.find_parent("div")
            title = ""
            if parent:
                job_name_el = parent.select_one("div.jobName")
                if job_name_el:
                    title = job_name_el.get_text(strip=True)

            items.append({
                "企業名": company_name,
                "タイトル": title,
                "掲載日": "",
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
