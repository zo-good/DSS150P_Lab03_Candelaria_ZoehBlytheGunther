import psycopg
from src.config import DB

_UPSERT_SQL = """
INSERT INTO curated.sales_order_lines (
    order_id, customer_id, product_id, order_timestamp,
    customer_city, customer_tier, product_name, category, brand,
    quantity, unit_price, discount_pct,
    gross_amount, discount_amount, net_amount, status,
    source_updated_at, pipeline_run_id, processed_at_utc, record_hash
) VALUES (
    %(order_id)s, %(customer_id)s, %(product_id)s, %(order_timestamp)s,
    %(customer_city)s, %(customer_tier)s, %(product_name)s, %(category)s, %(brand)s,
    %(quantity)s, %(unit_price)s, %(discount_pct)s,
    %(gross_amount)s, %(discount_amount)s, %(net_amount)s, %(status)s,
    %(source_updated_at)s, %(pipeline_run_id)s, %(processed_at_utc)s, %(record_hash)s
)
ON CONFLICT (order_id) DO UPDATE SET
    customer_id = EXCLUDED.customer_id,
    product_id = EXCLUDED.product_id,
    order_timestamp = EXCLUDED.order_timestamp,
    customer_city = EXCLUDED.customer_city,
    customer_tier = EXCLUDED.customer_tier,
    product_name = EXCLUDED.product_name,
    category = EXCLUDED.category,
    brand = EXCLUDED.brand,
    quantity = EXCLUDED.quantity,
    unit_price = EXCLUDED.unit_price,
    discount_pct = EXCLUDED.discount_pct,
    gross_amount = EXCLUDED.gross_amount,
    discount_amount = EXCLUDED.discount_amount,
    net_amount = EXCLUDED.net_amount,
    status = EXCLUDED.status,
    source_updated_at = EXCLUDED.source_updated_at,
    pipeline_run_id = EXCLUDED.pipeline_run_id,
    processed_at_utc = EXCLUDED.processed_at_utc,
    record_hash = EXCLUDED.record_hash
WHERE curated.sales_order_lines.record_hash <> EXCLUDED.record_hash
"""


def upsert_curated(df, run_id: str) -> int:
    """Load curated.sales_order_lines using rerun-safe UPSERT semantics."""
    records = df.to_dict(orient='records')
    affected = 0
    with psycopg.connect(
        host=DB['host'], port=DB['port'], dbname=DB['dbname'],
        user=DB['user'], password=DB['password'],
    ) as conn:
        with conn.cursor() as cur:
            for record in records:
                cur.execute(_UPSERT_SQL, record)
                affected += cur.rowcount
        conn.commit()
    return affected


def load_partition(df, year: int, month: int, run_id: str) -> int:
    """Load only a selected year/month partition and record audit.partition_loads."""
    raise NotImplementedError('Implement Goal 3 selected-partition load')
