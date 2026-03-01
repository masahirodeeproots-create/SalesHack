"""type中途 スクレイパー - https://type.jp/job/search/
9つの職種カテゴリURL × offset型ページネーション。
2,724件、50件/ページ。
"""

import re
from bs4 import BeautifulSoup

from config import SERVICE_REGISTRY
from scrapers.base import BaseScraper


class Scraper(BaseScraper):
    service_name = "type_chuto"
    output_filename = "type_chuto.csv"           # 企業単位 dedup 済み
    output_filename_raw = "type_chuto_raw.csv"   # 求人単位 raw（カテゴリ間の同一求人は除去済み）

    ITEMS_PER_PAGE = 50

    def scrape(self) -> list[dict]:
        category_urls = SERVICE_REGISTRY["type_chuto"]["base_urls"]

        for cat_idx, base_url in enumerate(category_urls, 1):
            self.logger.info(f"--- カテゴリ {cat_idx}/{len(category_urls)} ---")
            offset = 0

            while True:
                # offsetパラメータを追加
                if "?" in base_url:
                    url = f"{base_url}&offset={offset}"
                else:
                    url = f"{base_url}?offset={offset}"

                self.logger.info(f"カテゴリ{cat_idx} offset={offset}: {url}")
                html = self.client.fetch_requests(url)
                if html is None:
                    self.logger.warning("HTML取得失敗")
                    break

                items = self._parse_page(html)
                if not items:
                    self.logger.info("結果なし → 次のカテゴリへ")
                    break

                self.results.extend(items)
                self.logger.info(
                    f"{len(items)}件取得（累計: {len(self.results)}件）"
                )

                offset += self.ITEMS_PER_PAGE
                self.client.sleep()

        # 企業名で重複排除（同じ企業が複数カテゴリに出る場合）
        seen = set()
        unique = []
        for r in self.results:
            key = f"{r['企業名']}|{r['タイトル']}"
            if key not in seen:
                seen.add(key)
                unique.append(r)
        self.logger.info(f"重複排除: {len(self.results)} → {len(unique)}件")
        self.results = unique
        return self.results

    def _parse_page(self, html: str) -> list[dict]:
        """HTML解析で求人情報を抽出"""
        soup = BeautifulSoup(html, "lxml")
        items = []

        # 企業名: p.company (class=['company', 'size-14px'])
        company_els = soup.select("p.company")

        for company_el in company_els:
            company_name = company_el.get_text(strip=True)
            if not company_name or len(company_name) < 2:
                continue

            # 同じ求人カード（article）からタイトルを取得
            card = company_el.find_parent("article")
            title = ""
            if card:
                title_el = card.select_one("h3.mod-job-info-text")
                if title_el:
                    title = title_el.get_text(strip=True)

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
