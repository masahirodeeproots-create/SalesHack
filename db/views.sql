-- =============================================================
-- 分析用ビュー定義
-- =============================================================

-- -----------------------------------------------------------
-- 1. 企業サマリービュー (よく使うフィールドをピボット)
-- -----------------------------------------------------------
CREATE OR REPLACE VIEW v_company_summary AS
SELECT
    c.id,
    c.name_normalized AS 企業名,
    MAX(CASE WHEN fd.canonical_name = '業種' THEN v.value END) AS 業種,
    MAX(CASE WHEN fd.canonical_name = '本社所在地' THEN v.value END) AS 本社所在地,
    MAX(CASE WHEN fd.canonical_name = '設立' THEN v.value END) AS 設立,
    MAX(CASE WHEN fd.canonical_name = '代表者' THEN v.value END) AS 代表者,
    MAX(CASE WHEN fd.canonical_name = '資本金' THEN v.value END) AS 資本金,
    MAX(CASE WHEN fd.canonical_name = '従業員数' THEN v.value END) AS 従業員数,
    MAX(CASE WHEN fd.canonical_name = '売上高' THEN v.value END) AS 売上高,
    MAX(CASE WHEN fd.canonical_name = '上場区分' THEN v.value END) AS 上場区分,
    MAX(CASE WHEN fd.canonical_name = '事業内容' THEN v.value END) AS 事業内容,
    MAX(CASE WHEN fd.canonical_name = '企業URL' THEN v.value END) AS 企業URL,
    MAX(CASE WHEN fd.canonical_name = '電話番号' THEN v.value END) AS 電話番号,
    c.created_at,
    c.updated_at
FROM companies c
LEFT JOIN company_field_values v ON c.id = v.company_id
LEFT JOIN field_definitions fd ON v.field_id = fd.id
GROUP BY c.id, c.name_normalized, c.created_at, c.updated_at;

-- -----------------------------------------------------------
-- 2. HRサービス利用マトリクスビュー
-- -----------------------------------------------------------
CREATE OR REPLACE VIEW v_hr_service_matrix AS
SELECT
    c.name_normalized AS 企業名,
    MAX(CASE WHEN hs.key = 'labbase' THEN 1 ELSE 0 END) AS "Labbase",
    MAX(CASE WHEN hs.key = 'talentbook' THEN 1 ELSE 0 END) AS "タレントブック",
    MAX(CASE WHEN hs.key = 'type_shinsotsu' THEN 1 ELSE 0 END) AS "type就活",
    MAX(CASE WHEN hs.key = 'onecareer' THEN 1 ELSE 0 END) AS "ワンキャリア",
    MAX(CASE WHEN hs.key = 'levtech_rookie' THEN 1 ELSE 0 END) AS "レバテックルーキー",
    MAX(CASE WHEN hs.key = 'bizreach_campus' THEN 1 ELSE 0 END) AS "ビズリーチキャンパス",
    MAX(CASE WHEN hs.key = 'offerbox' THEN 1 ELSE 0 END) AS "オファーボックス",
    MAX(CASE WHEN hs.key = 'en_tenshoku' THEN 1 ELSE 0 END) AS "EN転職",
    MAX(CASE WHEN hs.key = 'kimisuka' THEN 1 ELSE 0 END) AS "キミスカ",
    MAX(CASE WHEN hs.key = 'caritasu' THEN 1 ELSE 0 END) AS "キャリタス",
    MAX(CASE WHEN hs.key = 'career_ticket' THEN 1 ELSE 0 END) AS "キャリアチケット",
    MAX(CASE WHEN hs.key = 'bizreach' THEN 1 ELSE 0 END) AS "ビズリーチ",
    MAX(CASE WHEN hs.key = 'en_ambi' THEN 1 ELSE 0 END) AS "アンビ",
    MAX(CASE WHEN hs.key = 'type_chuto' THEN 1 ELSE 0 END) AS "type中途",
    COUNT(DISTINCT hs.id) AS 利用サービス数
FROM companies c
LEFT JOIN company_service_usage csu ON c.id = csu.company_id
LEFT JOIN hr_services hs ON csu.service_id = hs.id
GROUP BY c.id, c.name_normalized;

-- -----------------------------------------------------------
-- 3. 電話番号一覧ビュー (担当者紐付き)
-- -----------------------------------------------------------
CREATE OR REPLACE VIEW v_phone_numbers AS
SELECT
    c.name_normalized AS 企業名,
    pn.number AS 電話番号,
    pn.label AS ラベル,
    pn.status AS ステータス,
    pn.status_detail AS ステータス詳細,
    pn.source AS 入手元,
    STRING_AGG(DISTINCT cp.name, ', ') AS 紐付き担当者,
    pn.updated_at
FROM companies c
JOIN phone_numbers pn ON c.id = pn.company_id
LEFT JOIN person_phone_numbers ppn ON pn.id = ppn.phone_number_id
LEFT JOIN company_persons cp ON ppn.person_id = cp.id
GROUP BY c.name_normalized, pn.id, pn.number, pn.label,
         pn.status, pn.status_detail, pn.source, pn.updated_at
ORDER BY c.name_normalized, pn.number;

-- -----------------------------------------------------------
-- 4. 担当者一覧ビュー
-- -----------------------------------------------------------
CREATE OR REPLACE VIEW v_company_persons AS
SELECT
    c.name_normalized AS 企業名,
    cp.name AS 担当者名,
    cp.department AS 部署,
    cp.role AS 役割,
    cp.is_decision_maker AS 決裁者,
    cp.email AS メール,
    STRING_AGG(DISTINCT pn.number, ', ') AS 電話番号,
    cp.source AS 情報源,
    cp.updated_at
FROM companies c
JOIN company_persons cp ON c.id = cp.company_id
LEFT JOIN person_phone_numbers ppn ON cp.id = ppn.person_id
LEFT JOIN phone_numbers pn ON ppn.phone_number_id = pn.id
GROUP BY c.name_normalized, cp.id, cp.name, cp.department, cp.role,
         cp.is_decision_maker, cp.email, cp.source, cp.updated_at
ORDER BY c.name_normalized, cp.name;

-- -----------------------------------------------------------
-- 5. 架電ダッシュボードビュー
-- -----------------------------------------------------------
CREATE OR REPLACE VIEW v_call_dashboard AS
SELECT
    c.name_normalized AS 企業名,
    d.status AS 商談ステータス,
    d.priority AS 優先度,
    p.name AS 商品名,
    sr.name AS 担当営業,
    COUNT(cl.id) AS 総架電数,
    MAX(cl.called_at) AS 最終架電日,
    SUM(CASE WHEN cl.phone_status = '該当' THEN 1 ELSE 0 END) AS 該当番号数,
    SUM(CASE WHEN cl.call_result = 'アポ' THEN 1 ELSE 0 END) AS アポ数,
    SUM(CASE WHEN cl.call_result = '不在' THEN 1 ELSE 0 END) AS 不在数
FROM companies c
LEFT JOIN deals d ON c.id = d.company_id
LEFT JOIN products p ON d.product_id = p.id
LEFT JOIN sales_reps sr ON d.assigned_rep_id = sr.id
LEFT JOIN call_logs cl ON c.id = cl.company_id
GROUP BY c.id, c.name_normalized, d.status, d.priority, p.name, sr.name;

-- -----------------------------------------------------------
-- 6. 営業マン別の架電実績ビュー
-- -----------------------------------------------------------
CREATE OR REPLACE VIEW v_sales_rep_stats AS
SELECT
    sr.name AS 営業担当者,
    COUNT(cl.id) AS 総架電数,
    COUNT(DISTINCT cl.company_id) AS 架電企業数,
    SUM(CASE WHEN cl.phone_status = '該当' THEN 1 ELSE 0 END) AS 該当件数,
    SUM(CASE WHEN cl.call_result = 'アポ' THEN 1 ELSE 0 END) AS アポ獲得数,
    SUM(CASE WHEN cl.call_result = '資料請求' THEN 1 ELSE 0 END) AS 資料請求数,
    SUM(CASE WHEN cl.call_result = '獲得見込み' THEN 1 ELSE 0 END) AS 獲得見込み数,
    ROUND(
        SUM(CASE WHEN cl.call_result = 'アポ' THEN 1 ELSE 0 END)::numeric
        / NULLIF(SUM(CASE WHEN cl.phone_status = '該当' THEN 1 ELSE 0 END), 0) * 100, 1
    ) AS アポ率,
    MIN(cl.called_at) AS 初回架電日,
    MAX(cl.called_at) AS 最終架電日
FROM sales_reps sr
LEFT JOIN call_logs cl ON sr.id = cl.sales_rep_id
GROUP BY sr.id, sr.name;

-- -----------------------------------------------------------
-- 7. フィールド充填率ビュー
-- -----------------------------------------------------------
CREATE OR REPLACE VIEW v_field_fill_rate AS
SELECT
    fd.canonical_name AS フィールド名,
    fd.category AS カテゴリ,
    COUNT(v.id) AS 入力済み企業数,
    (SELECT COUNT(*) FROM companies) AS 全企業数,
    ROUND(
        COUNT(v.id)::numeric / NULLIF((SELECT COUNT(*) FROM companies), 0) * 100, 1
    ) AS 充填率
FROM field_definitions fd
LEFT JOIN company_field_values v ON fd.id = v.field_id
GROUP BY fd.id, fd.canonical_name, fd.category
ORDER BY fd.display_order;

-- -----------------------------------------------------------
-- 8. 収集進捗ダッシュボードビュー（リアルタイム観測用）
-- -----------------------------------------------------------
CREATE OR REPLACE VIEW v_collection_progress AS
WITH total AS (SELECT COUNT(*) AS n FROM companies)
SELECT
    fd.category                                                      AS カテゴリ,
    fd.canonical_name                                                AS フィールド名,
    COUNT(v.id)                                                      AS 取得済み社数,
    total.n                                                          AS 全社数,
    ROUND(COUNT(v.id)::numeric / NULLIF(total.n, 0) * 100, 1)       AS 充填率,
    MAX(v.scraped_at)                                                AS 最終取得日時,
    COUNT(DISTINCT v.source)                                         AS ソース数
FROM field_definitions fd
CROSS JOIN total
LEFT JOIN company_field_values v ON fd.id = v.field_id
GROUP BY fd.id, fd.canonical_name, fd.category, fd.display_order, total.n
ORDER BY fd.display_order;

-- -----------------------------------------------------------
-- 9. 企業ごとの収集完了率ビュー
-- -----------------------------------------------------------
CREATE OR REPLACE VIEW v_company_coverage AS
WITH total_fields AS (SELECT COUNT(*) AS n FROM field_definitions),
     per_company AS (
         SELECT company_id, COUNT(*) AS filled_fields
         FROM company_field_values
         GROUP BY company_id
     )
SELECT
    c.name_normalized                                                AS 企業名,
    c.stock_code                                                     AS 証券コード,
    COALESCE(pc.filled_fields, 0)                                    AS 取得済みフィールド数,
    tf.n                                                             AS 全フィールド数,
    ROUND(COALESCE(pc.filled_fields, 0)::numeric / tf.n * 100, 1)   AS 充填率,
    EXISTS(SELECT 1 FROM phone_numbers pn WHERE pn.company_id = c.id) AS 電話あり,
    EXISTS(SELECT 1 FROM company_persons cp WHERE cp.company_id = c.id) AS 担当者あり,
    EXISTS(SELECT 1 FROM company_service_usage csu WHERE csu.company_id = c.id) AS HRサービスあり,
    c.updated_at                                                     AS 最終更新
FROM companies c
CROSS JOIN total_fields tf
LEFT JOIN per_company pc ON c.id = pc.company_id
ORDER BY pc.filled_fields DESC NULLS LAST;
