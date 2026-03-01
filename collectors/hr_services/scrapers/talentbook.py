"""タレントブック スクレイパー - https://www.talent-book.jp/companies
サイトマップから全企業スラグを取得し、各ページのog:titleから企業名を抽出。
/companies ページはクライアントサイドページネーションのため、SSRでは1ページ分しか取得できない。
"""

import re
from bs4 import BeautifulSoup

from scrapers.base import BaseScraper


class Scraper(BaseScraper):
    service_name = "talentbook"
    output_filename = "talentbook.csv"

    SITEMAP_URL = "https://www.talent-book.jp/sitemap.xml"
    BASE_URL = "https://www.talent-book.jp"

    # サイトマップ上の既知のナビゲーション/システムページ（企業ではない）
    NAV_SLUGS = {
        "stories", "knowhows", "companies", "feature", "categories",
        "recommended_stories", "privacy", "terms", "about", "contact",
        "law", "inquiry", "faq", "sitemap", "search",
    }

    def scrape(self) -> list[dict]:
        # 1. サイトマップから企業スラグを取得
        self.logger.info(f"サイトマップ取得: {self.SITEMAP_URL}")
        sitemap_xml = self.client.fetch_requests(self.SITEMAP_URL)
        if sitemap_xml is None:
            self.logger.error("サイトマップ取得失敗")
            return self.results

        slugs = self._extract_slugs(sitemap_xml)
        self.logger.info(f"企業スラグ数: {len(slugs)}")

        # 2. 各企業ページからog:titleで企業名を取得
        for i, slug in enumerate(slugs):
            url = f"{self.BASE_URL}/{slug}"
            self.logger.info(f"[{i+1}/{len(slugs)}] {url}")

            html = self.client.fetch_requests(url)
            if html is None:
                self.logger.warning(f"  取得失敗: {slug}")
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
                self.logger.warning(f"  企業名抽出失敗: {slug}")

            self.client.sleep()

        self.logger.info(f"合計: {len(self.results)}社")
        return self.results

    def _extract_slugs(self, sitemap_xml: str) -> list[str]:
        """サイトマップXMLから企業スラグを抽出"""
        soup = BeautifulSoup(sitemap_xml, "lxml-xml")
        slugs = set()

        for loc in soup.find_all("loc"):
            url = loc.text.strip()
            # トップレベルURL: https://www.talent-book.jp/{slug}
            m = re.match(r"https://www\.talent-book\.jp/([^/]+)$", url)
            if m:
                slug = m.group(1)
                if slug not in self.NAV_SLUGS and not slug.startswith("_"):
                    slugs.add(slug)

            # サブページURL: https://www.talent-book.jp/{slug}/stories/xxx
            m = re.match(r"https://www\.talent-book\.jp/([^/]+)/", url)
            if m:
                slug = m.group(1)
                if slug not in self.NAV_SLUGS and not slug.startswith("_"):
                    slugs.add(slug)

        return sorted(slugs)

    def _extract_company_name(self, html: str) -> str:
        """HTML の og:title から企業名を抽出"""
        soup = BeautifulSoup(html, "lxml")

        # og:title: "COMPANY_NAME | ロールモデル就活・転職サイト talentbook ..."
        og_title = soup.find("meta", property="og:title")
        if og_title and og_title.get("content"):
            title = og_title["content"]
            # " | " で分割して最初の部分が企業名
            parts = title.split(" | ")
            if parts:
                name = parts[0].strip()
                if name and len(name) >= 2:
                    return name

        # フォールバック: <title> タグ
        title_tag = soup.find("title")
        if title_tag:
            title = title_tag.get_text(strip=True)
            parts = title.split(" | ")
            if parts:
                name = parts[0].strip()
                if name and len(name) >= 2:
                    return name

        return ""


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
