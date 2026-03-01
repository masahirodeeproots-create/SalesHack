"""全サービスCSVを集約してマスターマッピングDBを構築する"""

import csv
import logging
from pathlib import Path

from config import OUTPUT_DIR, CSV_ENCODING, SERVICE_REGISTRY, SERVICE_NAMES
from company_cleaner import normalize_company_name, find_fuzzy_clusters, save_fuzzy_review

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/build_master.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


def build_master():
    """全サービスCSVからマスターマッピングCSVを生成する"""

    # 1. 各サービスCSVから正規化済み企業名セットを収集
    service_companies: dict[str, set[str]] = {}
    for key, config in SERVICE_REGISTRY.items():
        csv_path = OUTPUT_DIR / config["output_csv"]
        if not csv_path.exists():
            logger.warning(f"未取得: {config['name']} ({csv_path})")
            continue

        names = set()
        with open(csv_path, newline="", encoding=CSV_ENCODING) as f:
            for row in csv.DictReader(f):
                raw = row.get("企業名", "").strip()
                if raw:
                    normalized = normalize_company_name(raw)
                    if normalized:
                        names.add(normalized)

        service_companies[config["name"]] = names
        logger.info(f"{config['name']}: {len(names)}社（ユニーク）")

    if not service_companies:
        logger.error("読み込めるサービスCSVがありません")
        return

    # 2. 全企業名のユニバースを構築
    all_companies = set()
    for names in service_companies.values():
        all_companies.update(names)
    logger.info(f"全企業数（ユニーク）: {len(all_companies)}社")

    # 3. マスター行を構築
    master_rows = []
    for company in sorted(all_companies):
        row = {"企業名": company}
        for service_name in SERVICE_NAMES:
            row[service_name] = 1 if company in service_companies.get(service_name, set()) else 0
        master_rows.append(row)

    # 4. マスターCSV保存
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    master_path = OUTPUT_DIR / "master_mapping.csv"
    fieldnames = ["企業名"] + SERVICE_NAMES

    with open(master_path, "w", newline="", encoding=CSV_ENCODING) as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(master_rows)

    logger.info(f"マスターCSV保存: {master_path}（{len(master_rows)}社）")

    # 5. ファジーマッチ分析
    logger.info("ファジーマッチ分析中...")
    all_names = sorted(all_companies)
    clusters = find_fuzzy_clusters(all_names, threshold=0.85)

    if clusters:
        review_path = OUTPUT_DIR / "fuzzy_review.csv"
        save_fuzzy_review(clusters, review_path)
        logger.info(f"ファジーマッチ候補: {len(clusters)}クラスタ → {review_path}")
    else:
        logger.info("ファジーマッチ候補なし")

    # 6. サマリー出力
    print("\n" + "=" * 60)
    print("マスターDB構築完了")
    print(f"  総企業数: {len(master_rows)}社")
    print(f"  取得済みサービス: {len(service_companies)}/{len(SERVICE_REGISTRY)}")
    for name, companies in service_companies.items():
        print(f"    {name}: {len(companies)}社")
    if clusters:
        print(f"  ファジーマッチ候補: {len(clusters)}クラスタ（要レビュー）")
    print("=" * 60)


if __name__ == "__main__":
    build_master()
