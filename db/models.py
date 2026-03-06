"""
SQLAlchemy モデル定義
====================
rawdata設計（ソース別テーブル）:
- rawdata_{source}: 各媒体の生データ（クレンジング前）
- 企業マスター（companies）は1回生成・以降変更なし
- EAV設計（field_definitions / company_field_values）は廃止

営業管理（設計対象外・変更禁止）:
- phone_numbers, company_persons, person_phone_numbers
- call_logs, sales_reps, products, deals, deal_activities
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
# CORE: 企業マスター
# ===========================================================================


class Company(Base):
    """企業マスター - 1回生成・以降変更なし"""

    __tablename__ = "companies"

    id = Column(UUID(as_uuid=True), primary_key=True, default=_new_uuid)
    name = Column(Text, nullable=False, comment="元の企業名")
    name_normalized = Column(
        Text, unique=True, nullable=False, comment="名寄せ後の企業名"
    )
    stock_code = Column(Text, nullable=True, comment="証券コード（上場企業のみ）例: 8031")
    created_at = Column(DateTime(timezone=True), default=_utcnow, nullable=False)
    updated_at = Column(
        DateTime(timezone=True), default=_utcnow, onupdate=_utcnow, nullable=False
    )

    # 営業管理テーブルへの relationship（設計対象外・変更禁止）
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


# ===========================================================================
# RAWDATA: 各媒体の生データテーブル（29本）
# ===========================================================================


class RawdataCompanyInfo(Base):
    """① 登録データ（CSVアップロード・13万件）"""

    __tablename__ = "rawdata_company_info"

    id = Column(UUID(as_uuid=True), primary_key=True, default=_new_uuid)
    original_id = Column(Text, nullable=False, comment="企業マスターのUUID")
    企業名 = Column(Text, nullable=True)
    本社都道府県 = Column(Text, nullable=True)
    代表者名 = Column(Text, nullable=True)
    従業員数 = Column(Text, nullable=True)
    企業規模 = Column(Text, nullable=True)
    業種 = Column(Text, nullable=True)
    業種詳細 = Column(Text, nullable=True)
    代表電話番号 = Column(Text, nullable=True)
    scraped_at = Column(DateTime(timezone=True), default=_utcnow, nullable=False)

    __table_args__ = (Index("ix_rawdata_company_info_oid", "original_id"),)


class RawdataRaCompany(Base):
    """② リクルートエージェント（企業詳細ページ）"""

    __tablename__ = "rawdata_ra_company"

    id = Column(UUID(as_uuid=True), primary_key=True, default=_new_uuid)
    original_id = Column(Text, nullable=False)
    source_url = Column(Text, nullable=True)
    本社所在地 = Column(Text, nullable=True)
    設立 = Column(Text, nullable=True)
    資本金 = Column(Text, nullable=True)
    事業所 = Column(Text, nullable=True)
    関連会社 = Column(Text, nullable=True)
    株主 = Column(Text, nullable=True)
    株主公開 = Column(Text, nullable=True)
    決算情報 = Column(Text, nullable=True)
    備考 = Column(Text, nullable=True)
    公開求人数 = Column(Text, nullable=True)
    scraped_at = Column(DateTime(timezone=True), default=_utcnow, nullable=False)

    __table_args__ = (Index("ix_rawdata_ra_company_oid", "original_id"),)


class RawdataRaKyujin(Base):
    """⑭ リクルートエージェント（求人ページ /viewjob/）"""

    __tablename__ = "rawdata_ra_kyujin"

    id = Column(UUID(as_uuid=True), primary_key=True, default=_new_uuid)
    original_id = Column(Text, nullable=False)
    source_url = Column(Text, nullable=True)
    想定年収 = Column(Text, nullable=True, comment="例: 690万円～1,235万円")
    仕事の特徴 = Column(Text, nullable=True, comment="カンマ区切りタグ 例: 業界未経験歓迎, 年間休日120日以上")
    scraped_at = Column(DateTime(timezone=True), default=_utcnow, nullable=False)

    __table_args__ = (Index("ix_rawdata_ra_kyujin_oid", "original_id"),)


class RawdataMynavi(Base):
    """③ マイナビ"""

    __tablename__ = "rawdata_mynavi"

    id = Column(UUID(as_uuid=True), primary_key=True, default=_new_uuid)
    original_id = Column(Text, nullable=False)
    source_url = Column(Text, nullable=True)
    本社郵便番号 = Column(Text, nullable=True)
    採用実績校 = Column(Text, nullable=True)
    採用実績学部学科 = Column(Text, nullable=True)
    scraped_at = Column(DateTime(timezone=True), default=_utcnow, nullable=False)

    __table_args__ = (Index("ix_rawdata_mynavi_oid", "original_id"),)


class RawdataRikunabiCompany(Base):
    """④ リクナビ（企業情報）"""

    __tablename__ = "rawdata_rikunabi_company"

    id = Column(UUID(as_uuid=True), primary_key=True, default=_new_uuid)
    original_id = Column(Text, nullable=False)
    source_url = Column(Text, nullable=True)
    業種 = Column(Text, nullable=True)
    設立 = Column(Text, nullable=True)
    代表者 = Column(Text, nullable=True)
    資本金 = Column(Text, nullable=True)
    従業員数 = Column(Text, nullable=True)
    売上高 = Column(Text, nullable=True)
    純利益 = Column(Text, nullable=True)
    地域別売上高 = Column(Text, nullable=True)
    連結子会社数 = Column(Text, nullable=True)
    連結研究開発費 = Column(Text, nullable=True)
    事業内容 = Column(Text, nullable=True)
    事業所 = Column(Text, nullable=True)
    企業理念 = Column(Text, nullable=True)
    関連会社 = Column(Text, nullable=True)
    リクナビ限定情報 = Column(Text, nullable=True)
    プレエントリー候補リスト登録人数 = Column(Text, nullable=True)
    scraped_at = Column(DateTime(timezone=True), default=_utcnow, nullable=False)

    __table_args__ = (Index("ix_rawdata_rikunabi_company_oid", "original_id"),)


class RawdataRikunabiEmploy(Base):
    """15 リクナビ（採用情報）"""

    __tablename__ = "rawdata_rikunabi_employ"

    id = Column(UUID(as_uuid=True), primary_key=True, default=_new_uuid)
    original_id = Column(Text, nullable=False)
    source_url = Column(Text, nullable=True)
    主な募集職種 = Column(Text, nullable=True)
    主な勤務地 = Column(Text, nullable=True)
    応募資格 = Column(Text, nullable=True)
    積極採用対象 = Column(Text, nullable=True)
    採用予定学科 = Column(Text, nullable=True)
    採用人数 = Column(Text, nullable=True)
    新卒採用人数 = Column(Text, nullable=True)
    初年度月収例 = Column(Text, nullable=True)
    選考フロー = Column(Text, nullable=True)
    給与 = Column(Text, nullable=True)
    手当 = Column(Text, nullable=True)
    昇給 = Column(Text, nullable=True)
    賞与 = Column(Text, nullable=True)
    勤務時間 = Column(Text, nullable=True)
    休日休暇 = Column(Text, nullable=True)
    福利厚生 = Column(Text, nullable=True)
    試用期間 = Column(Text, nullable=True)
    研修制度 = Column(Text, nullable=True)
    自己啓発支援 = Column(Text, nullable=True)
    メンター制度 = Column(Text, nullable=True)
    キャリアコンサルティング制度 = Column(Text, nullable=True)
    社内検定制度 = Column(Text, nullable=True)
    月平均残業時間 = Column(Text, nullable=True)
    有給休暇取得日数 = Column(Text, nullable=True)
    育児休業取得者数 = Column(Text, nullable=True)
    女性管理職比率 = Column(Text, nullable=True)
    海外赴任者数 = Column(Text, nullable=True)
    受動喫煙対策 = Column(Text, nullable=True)
    過去3年間採用実績 = Column(Text, nullable=True)
    平均勤続年数 = Column(Text, nullable=True)
    平均年齢 = Column(Text, nullable=True)
    提出書類 = Column(Text, nullable=True)
    採用活動開始時期 = Column(Text, nullable=True)
    scraped_at = Column(DateTime(timezone=True), default=_utcnow, nullable=False)

    __table_args__ = (Index("ix_rawdata_rikunabi_employ_oid", "original_id"),)


class RawdataCaritasu(Base):
    """⑤ キャリタス（企業詳細）"""

    __tablename__ = "rawdata_caritasu"

    id = Column(UUID(as_uuid=True), primary_key=True, default=_new_uuid)
    original_id = Column(Text, nullable=False)
    source_url = Column(Text, nullable=True)
    企業名 = Column(Text, nullable=True)
    上場区分 = Column(Text, nullable=True)
    沿革 = Column(Text, nullable=True)
    scraped_at = Column(DateTime(timezone=True), default=_utcnow, nullable=False)

    __table_args__ = (Index("ix_rawdata_caritasu_oid", "original_id"),)


class RawdataPrtimes(Base):
    """⑥ PR TIMES"""

    __tablename__ = "rawdata_prtimes"

    id = Column(UUID(as_uuid=True), primary_key=True, default=_new_uuid)
    original_id = Column(Text, nullable=False)
    source_url = Column(Text, nullable=True)
    本社所在地 = Column(Text, nullable=True)
    電話番号 = Column(Text, nullable=True)
    企業URL = Column(Text, nullable=True)
    プレスリリース = Column(Text, nullable=True, comment='[{"title": "...", "date": "..."}] 直近3件')
    SNS_X = Column(Text, nullable=True)
    SNS_Facebook = Column(Text, nullable=True)
    SNS_YouTube = Column(Text, nullable=True)
    scraped_at = Column(DateTime(timezone=True), default=_utcnow, nullable=False)

    __table_args__ = (Index("ix_rawdata_prtimes_oid", "original_id"),)


class RawdataMinkabu(Base):
    """16 みんかぶ（stock_codeがある企業のみ）
    列名が期名を含み動的（売上高_2024年6月期 等）のため JSONB で保持。
    """

    __tablename__ = "rawdata_minkabu"

    id = Column(UUID(as_uuid=True), primary_key=True, default=_new_uuid)
    original_id = Column(Text, nullable=False)
    source_url = Column(Text, nullable=True)
    financial_data = Column(
        JSONB,
        nullable=True,
        comment='{"売上高_2024年6月期": "100億円", "営業利益_2024年6月期": "10億円", ...} 7指標×3期分',
    )
    scraped_at = Column(DateTime(timezone=True), default=_utcnow, nullable=False)

    __table_args__ = (Index("ix_rawdata_minkabu_oid", "original_id"),)


class RawdataEnHyouban(Base):
    """⑦ エン評判サイト"""

    __tablename__ = "rawdata_en_hyouban"

    id = Column(UUID(as_uuid=True), primary_key=True, default=_new_uuid)
    original_id = Column(Text, nullable=False)
    source_url = Column(Text, nullable=True)
    company_name = Column(Text, nullable=True)
    total_score = Column(Text, nullable=True)
    review_count = Column(Text, nullable=True)
    founded_year = Column(Text, nullable=True)
    employees = Column(Text, nullable=True)
    capital = Column(Text, nullable=True)
    listed_year = Column(Text, nullable=True)
    avg_salary = Column(Text, nullable=True)
    avg_age = Column(Text, nullable=True)
    score_growth = Column(Text, nullable=True)
    score_advantage = Column(Text, nullable=True)
    score_meritocracy = Column(Text, nullable=True)
    score_culture = Column(Text, nullable=True)
    score_youth = Column(Text, nullable=True)
    score_contribution = Column(Text, nullable=True)
    score_innovation = Column(Text, nullable=True)
    score_leadership = Column(Text, nullable=True)
    reviews_text = Column(Text, nullable=True)
    scraped_at = Column(DateTime(timezone=True), default=_utcnow, nullable=False)

    __table_args__ = (Index("ix_rawdata_en_hyouban_oid", "original_id"),)


class RawdataPhones(Base):
    """⑧ 電話番号 & 付随データ（Google検索スニペット由来）"""

    __tablename__ = "rawdata_phones"

    id = Column(UUID(as_uuid=True), primary_key=True, default=_new_uuid)
    original_id = Column(Text, nullable=False)
    source_url = Column(Text, nullable=True, comment="出典スニペットURL")
    拠点 = Column(Text, nullable=True, comment="東京本社、大阪支社 等")
    事業部 = Column(Text, nullable=True, comment="人事部、採用担当窓口 等")
    ラベル = Column(Text, nullable=True, comment="新卒 / 中途 / 不明")
    電話番号 = Column(Text, nullable=True)
    担当者名リレーションキー = Column(Text, nullable=True, comment="FK to rawdata_persons.id（初期値）")
    scraped_at = Column(DateTime(timezone=True), default=_utcnow, nullable=False)

    __table_args__ = (Index("ix_rawdata_phones_oid", "original_id"),)


class RawdataPersons(Base):
    """⑨ 担当者名 & 付随データ（Google検索スニペット由来）"""

    __tablename__ = "rawdata_persons"

    id = Column(UUID(as_uuid=True), primary_key=True, default=_new_uuid)
    original_id = Column(Text, nullable=False)
    source_url = Column(Text, nullable=True, comment="出典スニペットURL")
    拠点 = Column(Text, nullable=True)
    事業部 = Column(Text, nullable=True)
    ラベル = Column(Text, nullable=True, comment="新卒 / 中途 / 不明")
    担当者名 = Column(Text, nullable=True)
    電話番号リレーションキー = Column(Text, nullable=True, comment="FK to rawdata_phones.id（初期値）")
    scraped_at = Column(DateTime(timezone=True), default=_utcnow, nullable=False)

    __table_args__ = (Index("ix_rawdata_persons_oid", "original_id"),)


class RawdataEmails(Base):
    """⑩ メールアドレス"""

    __tablename__ = "rawdata_emails"

    id = Column(UUID(as_uuid=True), primary_key=True, default=_new_uuid)
    original_id = Column(Text, nullable=False)
    事業部 = Column(Text, nullable=True)
    メールアドレス = Column(Text, nullable=True)
    scraped_at = Column(DateTime(timezone=True), default=_utcnow, nullable=False)

    __table_args__ = (Index("ix_rawdata_emails_oid", "original_id"),)


class RawdataCompetitors(Base):
    """Ⅺ 競合・類似企業（Gemini API生成）"""

    __tablename__ = "rawdata_competitors"

    id = Column(UUID(as_uuid=True), primary_key=True, default=_new_uuid)
    original_id = Column(Text, nullable=False)
    類似企業1 = Column(Text, nullable=True)
    類似企業2 = Column(Text, nullable=True)
    類似企業3 = Column(Text, nullable=True)
    競合企業1 = Column(Text, nullable=True)
    競合企業2 = Column(Text, nullable=True)
    競合企業3 = Column(Text, nullable=True)
    scraped_at = Column(DateTime(timezone=True), default=_utcnow, nullable=False)

    __table_args__ = (Index("ix_rawdata_competitors_oid", "original_id"),)


class RawdataCallLogs(Base):
    """13 架電ログ（CSVインポート由来）
    source_url / scraped_at なし。called_at を使用。
    """

    __tablename__ = "rawdata_call_logs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=_new_uuid)
    original_id = Column(Text, nullable=True, comment="中間1で company_name → original_id 変換後に付与")
    company_name = Column(Text, nullable=False)
    sales_rep_name = Column(Text, nullable=True)
    called_at = Column(DateTime(timezone=True), nullable=True)
    phone_number = Column(Text, nullable=True)
    phone_status = Column(Text, nullable=True)
    product_name = Column(Text, nullable=True)
    phone_status_memo = Column(Text, nullable=True)
    discovered_number = Column(Text, nullable=True)
    discovered_number_memo = Column(Text, nullable=True)
    call_result = Column(Text, nullable=True)
    spoke_with = Column(Text, nullable=True)
    discovered_person_chuto = Column(Text, nullable=True)
    discovered_person_shinsotsu = Column(Text, nullable=True)
    notes = Column(Text, nullable=True)

    __table_args__ = (Index("ix_rawdata_call_logs_oid", "original_id"),)


# ---------------------------------------------------------------------------
# Ⅻ HRサービス（14テーブル）
# 共通カラムを Mixin で定義
# ---------------------------------------------------------------------------


class _HrRawdataMixin:
    """HRサービス rawdata テーブルの共通カラム"""

    id = Column(UUID(as_uuid=True), primary_key=True, default=_new_uuid)
    original_id = Column(Text, nullable=True, comment="中間1で企業名 → original_id 変換後に付与")
    source_url = Column(Text, nullable=True)
    企業名_掲載名 = Column(Text, nullable=True)
    掲載日 = Column(Text, nullable=True)
    scraped_at = Column(DateTime(timezone=True), default=_utcnow, nullable=False)


class RawdataHrLabbase(_HrRawdataMixin, Base):
    __tablename__ = "rawdata_hr_labbase"


class RawdataHrTalentbook(_HrRawdataMixin, Base):
    __tablename__ = "rawdata_hr_talentbook"


class RawdataHrTypeShinsotsu(_HrRawdataMixin, Base):
    __tablename__ = "rawdata_hr_type_shinsotsu"


class RawdataHrOnecareer(_HrRawdataMixin, Base):
    __tablename__ = "rawdata_hr_onecareer"


class RawdataHrLevtechRookie(_HrRawdataMixin, Base):
    __tablename__ = "rawdata_hr_levtech_rookie"


class RawdataHrBizreachCampus(_HrRawdataMixin, Base):
    __tablename__ = "rawdata_hr_bizreach_campus"


class RawdataHrOfferbox(_HrRawdataMixin, Base):
    __tablename__ = "rawdata_hr_offerbox"


class RawdataHrEnTenshoku(_HrRawdataMixin, Base):
    __tablename__ = "rawdata_hr_en_tenshoku"


class RawdataHrKimisuka(_HrRawdataMixin, Base):
    __tablename__ = "rawdata_hr_kimisuka"


class RawdataHrCaritasu(_HrRawdataMixin, Base):
    """新卒一覧への掲載確認用（⑤rawdata_caritasu は中途詳細用・別テーブル）"""

    __tablename__ = "rawdata_hr_caritasu"


class RawdataHrCareerTicket(_HrRawdataMixin, Base):
    __tablename__ = "rawdata_hr_career_ticket"


class RawdataHrBizreach(_HrRawdataMixin, Base):
    __tablename__ = "rawdata_hr_bizreach"


class RawdataHrEnAmbi(_HrRawdataMixin, Base):
    __tablename__ = "rawdata_hr_en_ambi"


class RawdataHrTypeChuto(_HrRawdataMixin, Base):
    __tablename__ = "rawdata_hr_type_chuto"


class RawdataHrHitotore(_HrRawdataMixin, Base):
    __tablename__ = "rawdata_hr_hitotore"


class RawdataHrAcaric(_HrRawdataMixin, Base):
    __tablename__ = "rawdata_hr_acaric"


class RawdataHrSupporters(_HrRawdataMixin, Base):
    __tablename__ = "rawdata_hr_supporters"


# ===========================================================================
# 営業管理テーブル（設計対象外・変更・削除禁止）
# ===========================================================================


class PhoneNumber(Base):
    """
    企業の電話番号候補（営業管理用）。
    call_data から更新される。rawdata_phones とは別物。

    status の値:
        "未確認" / "該当" / "使われてない" / "AI対応" / "別会社・別拠点・別事業部"
    """

    __tablename__ = "phone_numbers"

    id = Column(UUID(as_uuid=True), primary_key=True, default=_new_uuid)
    company_id = Column(
        UUID(as_uuid=True), ForeignKey("companies.id", ondelete="CASCADE"), nullable=False
    )
    number = Column(Text, nullable=False, comment="電話番号")
    label = Column(Text, nullable=True, comment="ラベル (代表/人事部直通/大阪拠点 等)")
    status = Column(
        Text,
        nullable=False,
        default="未確認",
        comment="ステータス (未確認/該当/使われてない/AI対応/別会社・別拠点・別事業部)",
    )
    status_detail = Column(Text, nullable=True, comment="別会社等の場合の詳細メモ")
    source = Column(Text, nullable=True, comment="入手元 (Web収集/架電判明 等)")
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
    企業の担当者（営業管理用）。
    架電で判明した担当者名を記録する。
    """

    __tablename__ = "company_persons"

    id = Column(UUID(as_uuid=True), primary_key=True, default=_new_uuid)
    company_id = Column(
        UUID(as_uuid=True), ForeignKey("companies.id", ondelete="CASCADE"), nullable=False
    )
    name = Column(Text, nullable=False, comment="担当者名")
    department = Column(Text, nullable=True, comment="部署名")
    role = Column(Text, nullable=True, comment="役割 (新卒担当/中途担当/人事部長 等)")
    is_decision_maker = Column(Boolean, nullable=False, default=False, comment="決裁者かどうか")
    email = Column(Text, nullable=True, comment="メールアドレス")
    notes = Column(Text, nullable=True, comment="備考")
    source = Column(Text, nullable=True, comment="情報源 (架電判明/Web 等)")
    created_at = Column(DateTime(timezone=True), default=_utcnow, nullable=False)
    updated_at = Column(
        DateTime(timezone=True), default=_utcnow, onupdate=_utcnow, nullable=False
    )

    __table_args__ = (Index("ix_person_company", "company_id"),)

    company = relationship("Company", back_populates="persons")
    phone_links = relationship(
        "PersonPhoneNumber", back_populates="person", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"<CompanyPerson {self.name} ({self.role})>"


class PersonPhoneNumber(Base):
    """担当者×電話番号の多対多中間テーブル（営業管理用）"""

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
    memo = Column(Text, nullable=True, comment="補足 (内線番号/時間帯 等)")
    created_at = Column(DateTime(timezone=True), default=_utcnow, nullable=False)

    __table_args__ = (
        UniqueConstraint("person_id", "phone_number_id", name="uq_person_phone"),
    )

    person = relationship("CompanyPerson", back_populates="phone_links")
    phone_number = relationship("PhoneNumber", back_populates="person_links")


class SalesRep(Base):
    """営業担当者マスタ（営業管理用）"""

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
    """商品・サービスマスタ（営業管理用）"""

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


class CallLog(Base):
    """
    架電ログ（営業管理用）。
    rawdata_call_logs（BQパイプライン用フラットテーブル）とは別物。
    """

    __tablename__ = "call_logs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=_new_uuid)
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
    phone_status = Column(
        Text,
        nullable=False,
        comment="番号ステータス (該当/使われてない/AI対応/別会社・別拠点・別事業部)",
    )
    phone_status_memo = Column(Text, nullable=True, comment="別会社・別拠点等の場合の詳細")
    discovered_number = Column(Text, nullable=True, comment="架電中に新しく判明した電話番号")
    discovered_number_memo = Column(Text, nullable=True, comment="新番号の所在等のメモ")
    call_result = Column(
        Text,
        nullable=True,
        comment="通話結果 (不在/受付ブロック/着電NG/獲得見込み/資料請求/アポ/架電NG)",
    )
    spoke_with = Column(Text, nullable=True, comment="実際に話した相手の名前")
    discovered_person_chuto = Column(Text, nullable=True, comment="判明した中途採用担当者名")
    discovered_person_shinsotsu = Column(Text, nullable=True, comment="判明した新卒採用担当者名")
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


class Deal(Base):
    """商談管理（営業管理用）"""

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
    """商談活動履歴（営業管理用）"""

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
