"""
SQLAlchemy モデル定義
====================
EAV (Entity-Attribute-Value) ハイブリッド設計:
- companies テーブルは最小限の固定カラムのみ
- 企業の属性は field_definitions + company_field_values で柔軟に管理
- 新しい収集項目は field_definitions に1行追加するだけ
- HRサービス追加は hr_services に1行追加するだけ

営業管理:
- phone_numbers: 企業の電話番号候補 (1企業:N番号)
- company_persons: 企業の担当者 (1企業:N人)
- person_phone_numbers: 担当者×電話番号 (多対多, メモ付き)
- call_logs: 架電ログ (中心テーブル, 営業マンの日々の記録)
- deals: 商談管理 (1企業×1商品で1商談)
- deal_activities: 商談の活動履歴
"""

import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, relationship


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _new_uuid() -> uuid.UUID:
    return uuid.uuid4()


class Base(DeclarativeBase):
    pass


# ===========================================================================
# CORE: 企業情報
# ===========================================================================


class Company(Base):
    """企業マスタ - 最小限の固定カラムのみ"""

    __tablename__ = "companies"

    id = Column(UUID(as_uuid=True), primary_key=True, default=_new_uuid)
    name_raw = Column(Text, nullable=False, comment="元の企業名")
    name_normalized = Column(
        Text, unique=True, nullable=False, comment="名寄せ後の企業名"
    )
    stock_code = Column(Text, nullable=True, comment="証券コード（上場企業のみ）例: 8031")
    created_at = Column(DateTime(timezone=True), default=_utcnow, nullable=False)
    updated_at = Column(
        DateTime(timezone=True), default=_utcnow, onupdate=_utcnow, nullable=False
    )

    # relationships
    field_values = relationship(
        "CompanyFieldValue", back_populates="company", cascade="all, delete-orphan"
    )
    field_values_history = relationship(
        "CompanyFieldValueHistory",
        back_populates="company",
        cascade="all, delete-orphan",
    )
    service_usages = relationship(
        "CompanyServiceUsage", back_populates="company", cascade="all, delete-orphan"
    )
    phone_numbers = relationship(
        "PhoneNumber", back_populates="company", cascade="all, delete-orphan"
    )
    persons = relationship(
        "CompanyPerson", back_populates="company", cascade="all, delete-orphan"
    )
    call_logs = relationship(
        "CallLog", back_populates="company", cascade="all, delete-orphan"
    )
    deals = relationship(
        "Deal", back_populates="company", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"<Company {self.name_normalized}>"


class FieldDefinition(Base):
    """
    収集フィールドの定義マスタ (EAVの"Attribute")。
    master_fields.json の内容がここに入る。

    新しい項目の追加:
        INSERT INTO field_definitions (canonical_name, category, aliases, source_priority)
        VALUES ('新項目名', 'カテゴリ', '["alias1"]', '["媒体A"]');
    """

    __tablename__ = "field_definitions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    canonical_name = Column(
        Text, unique=True, nullable=False, comment="正規フィールド名 (例: 業種)"
    )
    category = Column(Text, nullable=False, comment="カテゴリ (例: 基本企業情報)")
    data_type = Column(
        Text, nullable=False, default="text", comment="値の型 (text/number/json/date)"
    )
    aliases = Column(
        JSONB, nullable=False, default=list, comment='別名リスト (例: ["業種","業界分類"])'
    )
    media_presence = Column(
        JSONB,
        nullable=False,
        default=dict,
        comment='媒体ごとの出現頻度 (例: {"リクナビ":"common"})',
    )
    source_priority = Column(
        JSONB,
        nullable=False,
        default=list,
        comment='値が競合時の優先媒体順 (例: ["PR TIMES","リクナビ"])',
    )
    note = Column(Text, nullable=True, comment="備考")
    display_order = Column(
        Integer, nullable=False, default=0, comment="CSV/UI上の表示順"
    )
    created_at = Column(DateTime(timezone=True), default=_utcnow, nullable=False)

    # relationships
    values = relationship("CompanyFieldValue", back_populates="field_definition")
    values_history = relationship(
        "CompanyFieldValueHistory", back_populates="field_definition"
    )

    def __repr__(self) -> str:
        return f"<FieldDefinition {self.canonical_name}>"


class CompanyFieldValue(Base):
    """企業ごとの最新フィールド値 (source_priorityで最良を1つ保持)"""

    __tablename__ = "company_field_values"

    id = Column(UUID(as_uuid=True), primary_key=True, default=_new_uuid)
    company_id = Column(
        UUID(as_uuid=True), ForeignKey("companies.id", ondelete="CASCADE"), nullable=False
    )
    field_id = Column(
        Integer, ForeignKey("field_definitions.id", ondelete="CASCADE"), nullable=False
    )
    value = Column(Text, nullable=False, comment="値")
    source = Column(Text, nullable=True, comment="取得元媒体名")
    scraped_at = Column(DateTime(timezone=True), default=_utcnow, comment="取得日時")

    __table_args__ = (
        UniqueConstraint("company_id", "field_id", name="uq_company_field"),
        Index("ix_cfv_company", "company_id"),
        Index("ix_cfv_field", "field_id"),
    )

    company = relationship("Company", back_populates="field_values")
    field_definition = relationship("FieldDefinition", back_populates="values")


class CompanyFieldValueHistory(Base):
    """全ソースの生データを履歴として保持 (再統合可能)"""

    __tablename__ = "company_field_values_history"

    id = Column(UUID(as_uuid=True), primary_key=True, default=_new_uuid)
    company_id = Column(
        UUID(as_uuid=True), ForeignKey("companies.id", ondelete="CASCADE"), nullable=False
    )
    field_id = Column(
        Integer, ForeignKey("field_definitions.id", ondelete="CASCADE"), nullable=False
    )
    value = Column(Text, nullable=False)
    source = Column(Text, nullable=False, comment="取得元媒体名")
    scraped_at = Column(DateTime(timezone=True), default=_utcnow, nullable=False)

    __table_args__ = (
        Index("ix_cfvh_company_field", "company_id", "field_id"),
    )

    company = relationship("Company", back_populates="field_values_history")
    field_definition = relationship("FieldDefinition", back_populates="values_history")


# ===========================================================================
# HR SERVICES: サービス利用状況
# ===========================================================================


class HrService(Base):
    """
    HRサービスマスタ。
    新サービス追加:
        INSERT INTO hr_services (key, display_name, category, base_url, scrape_method)
        VALUES ('doda', 'doda', '中途', 'https://doda.jp', 'requests');
    + scrapers/hr_services/doda.py を1ファイル追加するだけ。
    """

    __tablename__ = "hr_services"

    id = Column(Integer, primary_key=True, autoincrement=True)
    key = Column(Text, unique=True, nullable=False, comment="内部キー (例: labbase)")
    display_name = Column(Text, nullable=False, comment="表示名 (例: Labbase)")
    category = Column(Text, nullable=False, comment="新卒/中途/両方")
    base_url = Column(Text, nullable=True, comment="ベースURL")
    scrape_method = Column(
        Text, nullable=False, default="requests", comment="requests/scrapingdog/playwright"
    )
    active = Column(Boolean, nullable=False, default=True, comment="有効フラグ")

    usages = relationship("CompanyServiceUsage", back_populates="service")

    def __repr__(self) -> str:
        return f"<HrService {self.display_name}>"


class CompanyServiceUsage(Base):
    """企業×HRサービスの利用状況"""

    __tablename__ = "company_service_usage"

    id = Column(UUID(as_uuid=True), primary_key=True, default=_new_uuid)
    company_id = Column(
        UUID(as_uuid=True), ForeignKey("companies.id", ondelete="CASCADE"), nullable=False
    )
    service_id = Column(
        Integer, ForeignKey("hr_services.id", ondelete="CASCADE"), nullable=False
    )
    listing_title = Column(Text, nullable=True, comment="掲載タイトル")
    listing_url = Column(Text, nullable=True, comment="掲載URL")
    posted_at = Column(Date, nullable=True, comment="掲載日")
    scraped_at = Column(DateTime(timezone=True), default=_utcnow, nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "company_id", "service_id", "listing_url", name="uq_company_service_url"
        ),
        Index("ix_csu_company", "company_id"),
        Index("ix_csu_service", "service_id"),
    )

    company = relationship("Company", back_populates="service_usages")
    service = relationship("HrService", back_populates="usages")


# ===========================================================================
# CONTACTS: 電話番号 & 担当者
# ===========================================================================


class PhoneNumber(Base):
    """
    企業の電話番号候補。
    1企業に対して複数の電話番号を管理する。
    ステータスは架電ログ (call_logs) の結果で更新されていく。

    status の値:
        "未確認" / "該当" / "使われてない" / "AI対応" / "別会社・別拠点・別事業部"
    """

    __tablename__ = "phone_numbers"

    id = Column(UUID(as_uuid=True), primary_key=True, default=_new_uuid)
    company_id = Column(
        UUID(as_uuid=True), ForeignKey("companies.id", ondelete="CASCADE"), nullable=False
    )
    number = Column(Text, nullable=False, comment="電話番号")
    label = Column(
        Text, nullable=True, comment="ラベル (代表/人事部直通/大阪拠点 等)"
    )
    status = Column(
        Text, nullable=False, default="未確認",
        comment="ステータス (未確認/該当/使われてない/AI対応/別会社・別拠点・別事業部)",
    )
    status_detail = Column(
        Text, nullable=True, comment="別会社等の場合の詳細メモ"
    )
    source = Column(
        Text, nullable=True, comment="入手元 (Web収集/架電判明 等)"
    )
    created_at = Column(DateTime(timezone=True), default=_utcnow, nullable=False)
    updated_at = Column(
        DateTime(timezone=True), default=_utcnow, onupdate=_utcnow, nullable=False
    )

    __table_args__ = (
        UniqueConstraint("company_id", "number", name="uq_company_phone"),
        Index("ix_phone_company", "company_id"),
    )

    company = relationship("Company", back_populates="phone_numbers")
    person_links = relationship(
        "PersonPhoneNumber", back_populates="phone_number", cascade="all, delete-orphan"
    )
    call_logs = relationship("CallLog", back_populates="phone_number")

    def __repr__(self) -> str:
        return f"<PhoneNumber {self.number} [{self.status}]>"


class CompanyPerson(Base):
    """
    企業の担当者。
    架電で判明した担当者名をここに記録する。
    異動情報は person_transfers で履歴管理。

    role の例: "新卒担当" / "中途担当" / "人事部長" / "受付" 等
    """

    __tablename__ = "company_persons"

    id = Column(UUID(as_uuid=True), primary_key=True, default=_new_uuid)
    company_id = Column(
        UUID(as_uuid=True), ForeignKey("companies.id", ondelete="CASCADE"), nullable=False
    )
    name = Column(Text, nullable=False, comment="担当者名")
    department = Column(Text, nullable=True, comment="部署名")
    role = Column(
        Text, nullable=True, comment="役割 (新卒担当/中途担当/人事部長 等)"
    )
    is_decision_maker = Column(
        Boolean, nullable=False, default=False, comment="決裁者かどうか"
    )
    email = Column(Text, nullable=True, comment="メールアドレス")
    notes = Column(Text, nullable=True, comment="備考")
    source = Column(
        Text, nullable=True, comment="情報源 (架電判明/Web 等)"
    )
    created_at = Column(DateTime(timezone=True), default=_utcnow, nullable=False)
    updated_at = Column(
        DateTime(timezone=True), default=_utcnow, onupdate=_utcnow, nullable=False
    )

    __table_args__ = (Index("ix_person_company", "company_id"),)

    company = relationship("Company", back_populates="persons")
    phone_links = relationship(
        "PersonPhoneNumber", back_populates="person", cascade="all, delete-orphan"
    )
    transfers = relationship(
        "PersonTransfer", back_populates="person", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"<CompanyPerson {self.name} ({self.role})>"


class PersonPhoneNumber(Base):
    """
    担当者×電話番号の多対多中間テーブル (メモ付き)。
    「この番号にかけると田中さんにつながる」を表現する。

    memo の例: "内線1234で取り次ぎ" / "午前中のみ" 等
    """

    __tablename__ = "person_phone_numbers"

    id = Column(UUID(as_uuid=True), primary_key=True, default=_new_uuid)
    person_id = Column(
        UUID(as_uuid=True),
        ForeignKey("company_persons.id", ondelete="CASCADE"),
        nullable=False,
    )
    phone_number_id = Column(
        UUID(as_uuid=True),
        ForeignKey("phone_numbers.id", ondelete="CASCADE"),
        nullable=False,
    )
    memo = Column(
        Text, nullable=True, comment="補足 (内線番号/時間帯 等)"
    )
    created_at = Column(DateTime(timezone=True), default=_utcnow, nullable=False)

    __table_args__ = (
        UniqueConstraint("person_id", "phone_number_id", name="uq_person_phone"),
    )

    person = relationship("CompanyPerson", back_populates="phone_links")
    phone_number = relationship("PhoneNumber", back_populates="person_links")


class PersonTransfer(Base):
    """担当者の異動履歴"""

    __tablename__ = "person_transfers"

    id = Column(UUID(as_uuid=True), primary_key=True, default=_new_uuid)
    person_id = Column(
        UUID(as_uuid=True),
        ForeignKey("company_persons.id", ondelete="CASCADE"),
        nullable=False,
    )
    transferred_at = Column(Date, nullable=True, comment="異動日")
    old_department = Column(Text, nullable=True, comment="旧部署")
    new_department = Column(Text, nullable=True, comment="新部署")
    old_role = Column(Text, nullable=True, comment="旧役職")
    new_role = Column(Text, nullable=True, comment="新役職")
    notes = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), default=_utcnow, nullable=False)

    __table_args__ = (Index("ix_transfer_person", "person_id"),)

    person = relationship("CompanyPerson", back_populates="transfers")


# ===========================================================================
# SALES: 営業マスタ & 商品
# ===========================================================================


class SalesRep(Base):
    """営業担当者マスタ"""

    __tablename__ = "sales_reps"

    id = Column(UUID(as_uuid=True), primary_key=True, default=_new_uuid)
    name = Column(Text, nullable=False, comment="営業担当者名")
    email = Column(Text, nullable=True)
    active = Column(Boolean, nullable=False, default=True)

    call_logs = relationship("CallLog", back_populates="sales_rep")
    deals = relationship("Deal", back_populates="assigned_rep")

    def __repr__(self) -> str:
        return f"<SalesRep {self.name}>"


class Product(Base):
    """
    営業商品/サービスマスタ。
    新商品追加:
        INSERT INTO products (name) VALUES ('プロダクトB');
    """

    __tablename__ = "products"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(Text, unique=True, nullable=False, comment="商品/サービス名")
    description = Column(Text, nullable=True, comment="説明")
    active = Column(Boolean, nullable=False, default=True, comment="有効フラグ")
    created_at = Column(DateTime(timezone=True), default=_utcnow, nullable=False)

    call_logs = relationship("CallLog", back_populates="product")
    deals = relationship("Deal", back_populates="product")

    def __repr__(self) -> str:
        return f"<Product {self.name}>"


# ===========================================================================
# CALL LOGS: 架電管理 (中心テーブル)
# ===========================================================================


class CallLog(Base):
    """
    架電ログ - 営業活動の中心テーブル。
    営業マンが1件架電するごとに1行記録する。

    記録フロー:
    1. 第1段階: 番号ステータス判定 (phone_status)
       → phone_numbers テーブルのステータスを更新
    2. 第2段階: 通話結果 (call_result, phone_status="該当"の場合のみ)
    3. 判明した情報を各テーブルに反映
       → 新番号 → phone_numbers に INSERT
       → 担当者名 → company_persons に INSERT/UPDATE
    """

    __tablename__ = "call_logs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=_new_uuid)

    # --- 誰が / 何を / どこに / どの番号に ---
    company_id = Column(
        UUID(as_uuid=True), ForeignKey("companies.id", ondelete="CASCADE"), nullable=False
    )
    phone_number_id = Column(
        UUID(as_uuid=True),
        ForeignKey("phone_numbers.id", ondelete="SET NULL"),
        nullable=True,
        comment="かけた電話番号",
    )
    sales_rep_id = Column(
        UUID(as_uuid=True),
        ForeignKey("sales_reps.id", ondelete="SET NULL"),
        nullable=False,
        comment="架電した営業担当者",
    )
    product_id = Column(
        Integer,
        ForeignKey("products.id", ondelete="SET NULL"),
        nullable=True,
        comment="営業商品/サービス",
    )
    called_at = Column(DateTime(timezone=True), nullable=False, comment="架電日時")

    # --- 第1段階: 番号ステータス ---
    phone_status = Column(
        Text,
        nullable=False,
        comment="番号ステータス (該当/使われてない/AI対応/別会社・別拠点・別事業部)",
    )
    phone_status_memo = Column(
        Text, nullable=True, comment="別会社・別拠点等の場合の詳細"
    )

    # --- 新番号発見 ---
    discovered_number = Column(
        Text, nullable=True, comment="架電中に新しく判明した電話番号"
    )
    discovered_number_memo = Column(
        Text, nullable=True, comment="新番号の所在等のメモ"
    )

    # --- 第2段階: 通話結果 (phone_status=該当 の場合のみ) ---
    call_result = Column(
        Text,
        nullable=True,
        comment="通話結果 (不在/受付ブロック/着電NG/獲得見込み/資料請求/アポ/架電NG)",
    )

    # --- 通話相手 vs 判明した担当者 ---
    spoke_with = Column(
        Text, nullable=True, comment="実際に話した相手の名前"
    )
    discovered_person_chuto = Column(
        Text, nullable=True, comment="判明した中途採用担当者名"
    )
    discovered_person_shinsotsu = Column(
        Text, nullable=True, comment="判明した新卒採用担当者名"
    )

    # --- メモ ---
    notes = Column(Text, nullable=True, comment="その他架電メモ")

    created_at = Column(DateTime(timezone=True), default=_utcnow, nullable=False)
    updated_at = Column(
        DateTime(timezone=True), default=_utcnow, onupdate=_utcnow, nullable=False
    )

    __table_args__ = (
        Index("ix_call_company", "company_id"),
        Index("ix_call_rep", "sales_rep_id"),
        Index("ix_call_called_at", "called_at"),
        Index("ix_call_phone", "phone_number_id"),
    )

    company = relationship("Company", back_populates="call_logs")
    phone_number = relationship("PhoneNumber", back_populates="call_logs")
    sales_rep = relationship("SalesRep", back_populates="call_logs")
    product = relationship("Product", back_populates="call_logs")


# ===========================================================================
# DEALS: 商談管理
# ===========================================================================


class Deal(Base):
    """
    商談管理。1企業×1商品で1商談 (UNIQUE制約)。

    status の値:
        "未着手" / "架電中" / "アポ獲得" / "商談中" / "受注" / "失注" / "保留"
    """

    __tablename__ = "deals"

    id = Column(UUID(as_uuid=True), primary_key=True, default=_new_uuid)
    company_id = Column(
        UUID(as_uuid=True), ForeignKey("companies.id", ondelete="CASCADE"), nullable=False
    )
    product_id = Column(
        Integer, ForeignKey("products.id", ondelete="SET NULL"), nullable=True
    )
    assigned_rep_id = Column(
        UUID(as_uuid=True),
        ForeignKey("sales_reps.id", ondelete="SET NULL"),
        nullable=True,
    )
    status = Column(
        Text,
        nullable=False,
        default="未着手",
        comment="ステータス (未着手/架電中/アポ獲得/商談中/受注/失注/保留)",
    )
    priority = Column(Integer, nullable=True, comment="優先度 (1-5)")
    expected_revenue = Column(Integer, nullable=True, comment="見込み売上")
    notes = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), default=_utcnow, nullable=False)
    updated_at = Column(
        DateTime(timezone=True), default=_utcnow, onupdate=_utcnow, nullable=False
    )

    __table_args__ = (
        UniqueConstraint("company_id", "product_id", name="uq_company_product_deal"),
        Index("ix_deal_company", "company_id"),
        Index("ix_deal_status", "status"),
    )

    company = relationship("Company", back_populates="deals")
    product = relationship("Product", back_populates="deals")
    assigned_rep = relationship("SalesRep", back_populates="deals")
    activities = relationship(
        "DealActivity", back_populates="deal", cascade="all, delete-orphan"
    )


class DealActivity(Base):
    """
    商談の活動履歴。
    ステータス変更・アポ・提案など商談に関する活動を時系列で記録。
    """

    __tablename__ = "deal_activities"

    id = Column(UUID(as_uuid=True), primary_key=True, default=_new_uuid)
    deal_id = Column(
        UUID(as_uuid=True), ForeignKey("deals.id", ondelete="CASCADE"), nullable=False
    )
    activity_type = Column(
        Text, nullable=False, comment="活動種別 (ステータス変更/メモ/アポ/提案/受注 等)"
    )
    old_status = Column(Text, nullable=True, comment="変更前ステータス")
    new_status = Column(Text, nullable=True, comment="変更後ステータス")
    description = Column(Text, nullable=True, comment="活動内容")
    acted_at = Column(
        DateTime(timezone=True), default=_utcnow, nullable=False, comment="活動日時"
    )
    created_at = Column(DateTime(timezone=True), default=_utcnow, nullable=False)

    __table_args__ = (Index("ix_activity_deal", "deal_id"),)

    deal = relationship("Deal", back_populates="activities")
