import pandas as pd

from src.benchmark.partitions import add_partition_columns, partition_key
from src.common.db import connect

TABLE = 'curated.sales_order_lines'

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
  AND EXCLUDED.source_updated_at >= curated.sales_order_lines.source_updated_at
"""
# Two guards in the WHERE clause:
#   * record_hash differs  -> unchanged rows are not rewritten (second load = 0 rows);
#   * incoming source_updated_at is not older than the stored one -> a delayed or
#     out-of-order run holding an OLDER snapshot cannot overwrite newer data.

_ADVISORY_LOCK_SQL = "SELECT pg_advisory_xact_lock(hashtext('curated.sales_order_lines'))"


def _records(df: pd.DataFrame) -> list[dict]:
    """DataFrame -> list of dicts with NaN/NaT turned into SQL NULL.

    Sorted by order_id so every concurrent loader takes row locks in the same
    order (different orders are how deadlocks happen).
    """
    ordered = df.sort_values('order_id').reset_index(drop=True)
    cleaned = ordered.astype(object).where(ordered.notna(), None)
    return cleaned.to_dict(orient='records')


def _upsert(conn, df: pd.DataFrame) -> int:
    records = _records(df)
    if not records:
        return 0
    with conn.cursor() as cur:
        # Loads of this table are serialised for the length of the transaction.
        # Row-level ON CONFLICT is already safe; this additionally rules out
        # interleaved batches and lock-order deadlocks between concurrent runs.
        cur.execute(_ADVISORY_LOCK_SQL)
        cur.executemany(_UPSERT_SQL, records)
        # psycopg 3 reports the total affected rows across the whole batch here.
        return cur.rowcount


def upsert_curated(df, run_id: str, conn=None) -> int:
    """Load curated.sales_order_lines using rerun-safe UPSERT semantics.

    The whole batch is ONE transaction: if any row fails, nothing from this
    call is stored. Returns the number of rows inserted or changed.
    """
    if conn is not None:
        return _upsert(conn, df)
    with connect() as c:  # commits on success, rolls back on error, then closes
        return _upsert(c, df)


def _record_partition_audit(conn, key: str, row_count: int, run_id: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO audit.partition_loads (partition_key, loaded_at_utc, row_count, pipeline_run_id)
            VALUES (%s, now(), %s, %s)
            ON CONFLICT (partition_key) DO UPDATE SET
                loaded_at_utc = EXCLUDED.loaded_at_utc,
                row_count = EXCLUDED.row_count,
                pipeline_run_id = EXCLUDED.pipeline_run_id
            """,
            (key, row_count, run_id),
        )


def load_partition(df, year: int, month: int, run_id: str) -> int:
    """Load only one year/month and record audit.partition_loads.

    * Refuses a frame containing rows from any other month (so the audit row
      count can never claim more or less than what was loaded).
    * The UPSERT and the audit row are written in the SAME transaction.
    """
    key = partition_key(year, month)
    if len(df):
        parts = add_partition_columns(df)
        outside = (parts['order_year'] != int(year)) | (parts['order_month'] != int(month))
        if outside.any():
            raise ValueError(
                f'{int(outside.sum())} row(s) do not belong to partition {key}; nothing was loaded'
            )
    with connect() as conn:
        affected = _upsert(conn, df)
        _record_partition_audit(conn, key, len(df), run_id)
    return affected


def count_partition_rows(year: int, month: int) -> int:
    """Rows stored in PostgreSQL for one UTC year/month (verification query)."""
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*) FROM curated.sales_order_lines
            WHERE EXTRACT(YEAR  FROM order_timestamp AT TIME ZONE 'UTC') = %s
              AND EXTRACT(MONTH FROM order_timestamp AT TIME ZONE 'UTC') = %s
            """,
            (int(year), int(month)),
        )
        return cur.fetchone()[0]


def count_rows() -> int:
    with connect() as conn, conn.cursor() as cur:
        cur.execute('SELECT count(*) FROM curated.sales_order_lines')
        return cur.fetchone()[0]


def verify_loaded(df) -> list[str]:
    """Check that every order_id in ``df`` is present in PostgreSQL.

    Returns human-readable errors (empty list = OK), like validate_curated.
    """
    ids = df['order_id'].tolist()
    if not ids:
        return []
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            'SELECT count(*) FROM curated.sales_order_lines WHERE order_id = ANY(%s)',
            (ids,),
        )
        found = cur.fetchone()[0]
    expected = len(set(ids))
    if found != expected:
        return [f'{expected - found} of {expected} curated order_id(s) are missing from PostgreSQL']
    return []
