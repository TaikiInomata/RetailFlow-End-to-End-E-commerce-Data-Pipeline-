"""
debezium_schemas.py
───────────────────
Định nghĩa schema Debezium envelope tường minh cho từng bảng CDC.

Lý do cần file này:
  - spark.readStream.json() KHÔNG thể tự suy luận schema khi dùng glob path
    (ngay cả khi spark.sql.streaming.schemaInference = true).
  - Schema phải được cung cấp tường minh → dùng .schema(debezium_schema).

Cấu trúc Debezium Envelope (Postgres Source Connector):
  {
    "before": { ...fields... } | null,
    "after":  { ...fields... } | null,
    "op":     "r" | "c" | "u" | "d",
    "ts_ms":  <epoch_ms>,
    "source": { ...metadata... }
  }
"""

# pyrefly: ignore [missing-import]
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, IntegerType,
    DecimalType, BooleanType,
)

# ---------------------------------------------------------------------------
# Helper: Wrapper Debezium envelope xung quanh payload schema của 1 bảng
# ---------------------------------------------------------------------------
def _debezium_envelope(payload_schema: StructType) -> StructType:
    """
    Bọc payload_schema vào cấu trúc envelope Debezium chuẩn.
    Cả 'before' và 'after' đều dùng cùng payload_schema.
    """
    return StructType([
        StructField("before",  payload_schema, nullable=True),
        StructField("after",   payload_schema, nullable=True),
        StructField("op",      StringType(),   nullable=False),
        StructField("ts_ms",   LongType(),     nullable=True),
        StructField("source",  StructType([
            StructField("version",   StringType(), nullable=True),
            StructField("connector", StringType(), nullable=True),
            StructField("name",      StringType(), nullable=True),
            StructField("ts_ms",     LongType(),   nullable=True),
            StructField("db",        StringType(), nullable=True),
            StructField("schema",    StringType(), nullable=True),
            StructField("table",     StringType(), nullable=True),
            StructField("txId",      LongType(),   nullable=True),
            StructField("lsn",       LongType(),   nullable=True),
            StructField("xmin",      LongType(),   nullable=True),
        ]), nullable=True),
    ])


# ---------------------------------------------------------------------------
# Payload schemas cho từng bảng
# ---------------------------------------------------------------------------

_PRODUCTS_PAYLOAD = StructType([
    StructField("product_id",   IntegerType(),      nullable=True),
    StructField("name",         StringType(),       nullable=True),
    StructField("description",  StringType(),       nullable=True),
    StructField("price",        DecimalType(10, 2), nullable=True),
    StructField("stock",        IntegerType(),      nullable=True),
    StructField("category_id",  IntegerType(),      nullable=True),
    StructField("created_at",   LongType(),         nullable=True),  # micro-epoch từ Debezium
    StructField("updated_at",   LongType(),         nullable=True),
])

_ORDERS_PAYLOAD = StructType([
    StructField("order_id",     IntegerType(),      nullable=True),
    StructField("user_id",      IntegerType(),      nullable=True),
    StructField("status",       StringType(),       nullable=True),
    StructField("total_amount", DecimalType(12, 2), nullable=True),
    StructField("created_at",   LongType(),         nullable=True),
    StructField("updated_at",   LongType(),         nullable=True),
])

_USERS_PAYLOAD = StructType([
    StructField("user_id",      IntegerType(),      nullable=True),
    StructField("username",     StringType(),       nullable=True),
    StructField("email",        StringType(),       nullable=True),
    StructField("full_name",    StringType(),       nullable=True),
    StructField("is_active",    BooleanType(),      nullable=True),
    StructField("created_at",   LongType(),         nullable=True),
    StructField("updated_at",   LongType(),         nullable=True),
])

_ORDER_ITEMS_PAYLOAD = StructType([
    StructField("order_item_id", IntegerType(),      nullable=True),
    StructField("order_id",      IntegerType(),      nullable=True),
    StructField("product_id",    IntegerType(),      nullable=True),
    StructField("quantity",      IntegerType(),      nullable=True),
    StructField("unit_price",    DecimalType(10, 2), nullable=True),
])

_CATEGORIES_PAYLOAD = StructType([
    StructField("category_id",   IntegerType(), nullable=True),
    StructField("name",          StringType(),  nullable=True),
    StructField("parent_id",     IntegerType(), nullable=True),
])

_REVIEWS_PAYLOAD = StructType([
    StructField("review_id",   IntegerType(), nullable=True),
    StructField("user_id",     IntegerType(), nullable=True),
    StructField("product_id",  IntegerType(), nullable=True),
    StructField("rating",      IntegerType(), nullable=True),
    StructField("comment",     StringType(),  nullable=True),
    StructField("created_at",  LongType(),    nullable=True),
])

_COUPONS_PAYLOAD = StructType([
    StructField("coupon_id",    IntegerType(),      nullable=True),
    StructField("code",         StringType(),       nullable=True),
    StructField("discount_pct", DecimalType(5, 2),  nullable=True),
    StructField("valid_from",   LongType(),         nullable=True),
    StructField("valid_to",     LongType(),         nullable=True),
    StructField("is_active",    BooleanType(),      nullable=True),
])

# ---------------------------------------------------------------------------
# Registry: table_name → Debezium envelope schema
# ---------------------------------------------------------------------------
_SCHEMA_REGISTRY: dict = {
    "products":    _debezium_envelope(_PRODUCTS_PAYLOAD),
    "orders":      _debezium_envelope(_ORDERS_PAYLOAD),
    "users":       _debezium_envelope(_USERS_PAYLOAD),
    "order_items": _debezium_envelope(_ORDER_ITEMS_PAYLOAD),
    "categories":  _debezium_envelope(_CATEGORIES_PAYLOAD),
    "reviews":     _debezium_envelope(_REVIEWS_PAYLOAD),
    "coupons":     _debezium_envelope(_COUPONS_PAYLOAD),
}


def get_debezium_schema(table_name: str) -> StructType:
    """
    Trả về Debezium envelope schema cho bảng được chỉ định.

    Args:
        table_name: Tên bảng (VD: 'products', 'orders').

    Returns:
        StructType schema tương ứng.

    Raises:
        ValueError: Nếu bảng chưa có schema định nghĩa trong registry.
    """
    schema = _SCHEMA_REGISTRY.get(table_name)
    if schema is None:
        available = ", ".join(sorted(_SCHEMA_REGISTRY.keys()))
        raise ValueError(
            f"Chưa có schema cho bảng '{table_name}'. "
            f"Các bảng hiện được hỗ trợ: [{available}]. "
            f"Vui lòng thêm payload schema vào debezium_schemas.py."
        )
    return schema
