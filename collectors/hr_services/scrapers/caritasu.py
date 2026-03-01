"""キャリタス スクレイパー - https://job.career-tasu.jp/condition-search/result/
206社、40社/ページ。チェックポイント対応。
"""

import re
from bs4 import BeautifulSoup

from scrapers.base import BaseScraper


class Scraper(BaseScraper):
    service_name = "caritasu"
    output_filename = "caritasu.csv"

    BASE_URL = "https://job.career-tasu.jp/condition-search/result/"
    PARAMS = "corpOtherCheckCd=04-%E3%82%AD%E3%83%A3%E3%83%AA%E3%82%BF%E3%82%B9%E9%99%90%E5%AE%9A%E6%83%85%E5%A0%B1%E3%81%82%E3%82%8A"

    def scrape(self) -> list[dict]:
        page = 1
        while True:
            url = f"{self.BASE_URL}?{self.PARAMS}&p={page}"
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
                f"Page {page}: {len(companies)}社取得（累計: {len(self.results)}社）"
            )

            # チェックポイント
            if page % 5 == 0:
                self.save_checkpoint(page)
                self.save_csv()

            page += 1
            self.client.sleep()

        self.clear_checkpoint()
        return self.results

    def _parse_page(self, html: str) -> list[dict]:
        """HTML解析で企業情報を抽出"""
        soup = BeautifulSoup(html, "lxml")
        companies = []

        # 企業名: div.c_panelCompanyInfoMain__ttlBox
        ttl_boxes = soup.select("div.c_panelCompanyInfoMain__ttlBox")

        seen = set()
        for ttl in ttl_boxes:
            name = ttl.get_text(strip=True)
            if not name or len(name) < 2 or name in seen:
                continue
            seen.add(name)
            companies.append({
                "企業名": name,
                "タイトル": "",
                "掲載日": "",
            })

        return companies


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
