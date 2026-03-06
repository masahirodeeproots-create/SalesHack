#!/bin/bash
# ============================================================
# 自動パイプライン: run_experiment.py完了後に続きを実行
# ============================================================

set -euo pipefail

PROJ="/Users/masahiromatsuyama/Product/企業情報収集"
EN_HYOUBAN="/Users/masahiromatsuyama/Product/scraping_en_hyouban"
LOG="$PROJ/data/logs/auto_pipeline.log"
REPORT="$PROJ/data/logs/overnight_report.txt"
PYTHON="$PROJ/venv/bin/python"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG"
}

cd "$PROJ"

log "===== 自動パイプライン開始 ====="

# --------------------------------------------------
# Phase 4 (run_experiment.py) 完了待ち
# --------------------------------------------------
log "Phase4完了待ち (PID 8136)..."
if kill -0 8136 2>/dev/null; then
    wait 8136 2>/dev/null || true
fi
log "Phase4 完了"

# --------------------------------------------------
# Phase 5: 連絡先 → PostgreSQL
# --------------------------------------------------
log "Phase5: 連絡先をPostgreSQLに書き込み開始"
$PYTHON -m collectors.contacts.run --limit 100 >> "$LOG" 2>&1 && \
    log "Phase5 完了" || log "[WARN] Phase5 エラー（続行）"

# --------------------------------------------------
# Phase 6: Gemini類似企業生成
# --------------------------------------------------
log "Phase6: Gemini類似企業生成開始"
$PYTHON -m collectors.gemini_enrichment.sync --limit 100 >> "$LOG" 2>&1 && \
    log "Phase6 完了" || log "[WARN] Phase6 エラー（続行）"

# --------------------------------------------------
# en_hyouban scraper 完了待ち
# --------------------------------------------------
log "en_hyoubanスクレイパー完了待ち (PID 10195)..."
if kill -0 10195 2>/dev/null; then
    wait 10195 2>/dev/null || true
fi
log "en_hyoubanスクレイパー完了"

# en_hyouban results.csv エクスポート
log "en_hyouban: CSVエクスポート"
cd "$EN_HYOUBAN"
python3 -c "from progress import export_csv; export_csv('data/results.csv')" >> "$LOG" 2>&1
cd "$PROJ"

# --------------------------------------------------
# Phase 7: エン評判 → PostgreSQL + BQ
# --------------------------------------------------
log "Phase7: エン評判データをDB・BQに同期開始"
UPLOAD_TO_BIGQUERY=true $PYTHON -m collectors.en_hyouban.sync >> "$LOG" 2>&1 && \
    log "Phase7 完了" || log "[WARN] Phase7 エラー（続行）"

# --------------------------------------------------
# 集計レポート生成
# --------------------------------------------------
log "集計レポート生成中..."

{
cat << 'HEADER'
================================================================
  一晩自動実行 集計レポート
================================================================
HEADER

echo "  生成日時: $(date '+%Y-%m-%d %H:%M:%S')"
echo ""

echo "【企業情報収集 (run_experiment.py)】"
cat data/output/experiment_report.txt 2>/dev/null | grep -A 40 "フィールド充填率" | head -40 || echo "  レポートなし"
echo ""

echo "【PostgreSQL 格納件数】"
$PYTHON - << 'PYEOF'
import sys
sys.path.insert(0, '.')
try:
    from db.connection import get_session
    from db.models import Company, RawdataPhones, RawdataPersons, RawdataCompetitors
    with get_session() as s:
        companies    = s.query(Company).count()
        phones       = s.query(RawdataPhones).count()
        persons      = s.query(RawdataPersons).count()
        competitors  = s.query(RawdataCompetitors).count()
        print(f"  企業数:             {companies:,} 社")
        print(f"  電話番号(rawdata):  {phones:,} 件")
        print(f"  担当者(rawdata):    {persons:,} 件")
        print(f"  類似企業(rawdata):  {competitors:,} 件")
except Exception as e:
    print(f"  [ERROR] {e}")
PYEOF
echo ""

echo "【エン評判 スクレイピング結果】"
python3 -c "
import sqlite3
con = sqlite3.connect('$EN_HYOUBAN/data/progress.db')
r = dict(con.execute('SELECT status, COUNT(*) FROM companies GROUP BY status').fetchall())
total = sum(r.values())
done  = r.get('done', 0)
error = r.get('error', 0)
pending = r.get('pending', 0)
print(f'  完了: {done}/{total}社  エラー: {error}  未処理: {pending}')
con.close()
" 2>/dev/null || echo "  DB読み込み失敗"
echo ""

echo "【HRサービス (onecareer等)】"
wc -l data/output/hr_services/*.csv 2>/dev/null | grep -v total | awk '{printf "  %-30s %s行\n", $2, $1}' || echo "  CSVなし"
echo ""

echo "【完了フェーズ】"
echo "  ✅ Phase4: 企業情報収集 (100社)"
echo "  ✅ Phase5: 連絡先 → PostgreSQL"
echo "  ✅ Phase6: Gemini類似企業生成"
echo "  ✅ Phase7: エン評判 → DB/BQ同期"
echo "  🔄 HRサービス: バックグラウンド継続中"
echo ""
echo "================================================================"

} > "$REPORT" 2>&1

log "===== パイプライン完了 ====="
log "レポート: $REPORT"
cat "$REPORT"
