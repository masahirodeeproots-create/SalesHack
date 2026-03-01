"""ワンキャリア スクレイパー - https://www.onecareer.jp/companies/
サイトマップ(gzip)から全企業IDを取得し、各企業ページのog:titleから企業名を抽出。
企業リストページはNuxt SPAのためSSRでは取得不可。
9,960+企業 → チェックポイントで中断再開対応。
"""

import re
import gzip
from bs4 import BeautifulSoup

from scrapers.base import BaseScraper


class Scraper(BaseScraper):
    service_name = "onecareer"
    output_filename = "onecareer.csv"

    SITEMAP_URLS = [
        "https://www.onecareer.jp/sitemaps/sitemap_companies1.xml.gz",
        "https://www.onecareer.jp/sitemaps/sitemap_companies3.xml.gz",
    ]
    BASE_URL = "https://www.onecareer.jp/companies"
    CHECKPOINT_INTERVAL = 50

    def scrape(self) -> list[dict]:
        # 1. サイトマップからユニーク企業IDを取得
        company_ids = self._fetch_company_ids()
        self.logger.info(f"ユニーク企業ID数: {len(company_ids)}")

        # チェックポイントから再開
        start_idx = self.get_checkpoint()
        if start_idx > 0:
            self.results = self.load_existing_results()
            self.logger.info(
                f"チェックポイントから再開: idx={start_idx}（既存{len(self.results)}件）"
            )

        # 2. 各企業ページからog:titleで企業名を取得
        for i in range(start_idx, len(company_ids)):
            cid = company_ids[i]
            url = f"{self.BASE_URL}/{cid}"

            html = self.client.fetch_requests(url)
            if html is None:
                self.logger.warning(f"[{i+1}/{len(company_ids)}] 取得失敗: ID {cid}")
                self.client.sleep()
                continue

            company_name = self._extract_company_name(html)
            if company_name:
                self.results.append({
                    "企業名": company_name,
                    "タイトル": "",
                    "掲載日": "",
                })
            else:
                self.logger.debug(f"[{i+1}/{len(company_ids)}] 企業名抽出失敗: ID {cid}")

            if (i + 1) % 100 == 0:
                self.logger.info(
                    f"進捗: {i+1}/{len(company_ids)} ({len(self.results)}社取得済)"
                )

            # チェックポイント保存
            if (i + 1) % self.CHECKPOINT_INTERVAL == 0:
                self.save_checkpoint(i + 1)
                self.save_csv()

            self.client.sleep()

        self.clear_checkpoint()
        self.logger.info(f"合計: {len(self.results)}社")
        return self.results

    def _fetch_company_ids(self) -> list[str]:
        """サイトマップ(gzip)から全ユニーク企業IDを抽出"""
        company_ids = set()

        for sitemap_url in self.SITEMAP_URLS:
            self.logger.info(f"サイトマップ取得: {sitemap_url}")
            try:
                import requests
                resp = requests.get(sitemap_url, timeout=30)
                if resp.status_code != 200:
                    self.logger.warning(f"サイトマップ取得失敗: {resp.status_code}")
                    continue

                content = gzip.decompress(resp.content).decode("utf-8")
                ids = re.findall(r"/companies/(\d+)", content)
                company_ids.update(ids)
                self.logger.info(f"  抽出ID数: {len(ids)}（ユニーク累計: {len(company_ids)}）")
            except Exception as e:
                self.logger.error(f"サイトマップ処理エラー: {e}")

        return sorted(company_ids, key=int)

    def _extract_company_name(self, html: str) -> str:
        """og:title から企業名を抽出"""
        soup = BeautifulSoup(html, "lxml")

        # og:title: "COMPANY_NAMEの新卒採用・就職・会社概要とクチコミ｜就活サイト【ワンキャリア】"
        og_title = soup.find("meta", property="og:title")
        if og_title and og_title.get("content"):
            title = og_title["content"]
            m = re.match(r"^(.+?)の新卒採用", title)
            if m:
                return m.group(1).strip()

        # フォールバック: <title>
        title_tag = soup.find("title")
        if title_tag:
            title = title_tag.get_text(strip=True)
            m = re.match(r"^(.+?)の新卒採用", title)
            if m:
                return m.group(1).strip()

        return ""


if __name__ == "__main__":
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
