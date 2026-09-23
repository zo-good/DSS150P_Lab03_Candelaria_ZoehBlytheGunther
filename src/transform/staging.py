import json
from pathlib import Path
import pandas as pd
from src.common.audit import utc_now_iso
from src.config import path_for, SETTINGS


def _dedupe_latest(df: pd.DataFrame, key: str, updated_col: str = 'updated_at') -> pd.DataFrame:
    """Keep only the row with the greatest updated_at for each business key."""
    return (
        df.sort_values(updated_col, na_position='first')
          .drop_duplicates(subset=[key], keep='last')
          .reset_index(drop=True)
    )


def _add_audit_columns(df: pd.DataFrame, run_id: str) -> pd.DataFrame:
    df = df.copy()
    df['pipeline_run_id'] = run_id
    df['staged_at_utc'] = utc_now_iso()
    return df


def _quarantine_row(row: dict, source: str, reason: str) -> dict:
    return {
        'source': source,
        'reason': reason,
        'quarantined_at_utc': utc_now_iso(),
        'record': json.dumps(row, default=str),
    }


def _clean_customers(raw_dir: Path):
    df = pd.read_csv(raw_dir / 'customers.csv')

    df['updated_at'] = pd.to_datetime(df['updated_at'], utc=True, errors='coerce')
    df['created_at'] = pd.to_datetime(df['created_at'], utc=True, errors='coerce')

    df = _dedupe_latest(df, 'customer_id')

    df['email'] = df['email'].astype('string').str.strip().str.lower()
    df['email_missing'] = df['email'].isna() | (df['email'] == '')

    df['city'] = df['city'].astype('string').str.strip().str.title()

    return df, []


def _clean_products(raw_dir: Path):
    df = pd.read_json(raw_dir / 'products.json')

    df['updated_at'] = pd.to_datetime(df['updated_at'], utc=True, errors='coerce')
    df = _dedupe_latest(df, 'product_id')

    df['category_name'] = df['category'].apply(lambda c: c.get('name') if isinstance(c, dict) else None)
    df['category_department'] = df['category'].apply(lambda c: c.get('department') if isinstance(c, dict) else None)
    df = df.drop(columns=['category'])

    df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce')

    invalid_mask = df['unit_price'].isna() | (df['unit_price'] < 0)
    invalid_rows = df[invalid_mask]
    valid_df = df[~invalid_mask].reset_index(drop=True)

    quarantine = [
        _quarantine_row(row.to_dict(), 'products', 'invalid or negative unit_price')
        for _, row in invalid_rows.iterrows()
    ]
    return valid_df, quarantine


def _clean_orders(raw_dir: Path):
    df = pd.read_csv(raw_dir / 'orders.csv')

    df['order_timestamp'] = pd.to_datetime(df['order_timestamp'], utc=True, errors='coerce')
    df['updated_at'] = pd.to_datetime(df['updated_at'], utc=True, errors='coerce')
    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')

    df = _dedupe_latest(df, 'order_id')

    allowed_statuses = set(SETTINGS['quality']['allowed_order_statuses'])
    min_qty = SETTINGS['quality']['min_quantity']
    max_qty = SETTINGS['quality']['max_quantity']

    reason = pd.Series([None] * len(df), index=df.index, dtype=object)
    reason[df['order_timestamp'].isna()] = 'unparseable order_timestamp'
    still_ok = reason.isna()
    reason[still_ok & df['updated_at'].isna()] = 'unparseable updated_at'
    still_ok = reason.isna()
    reason[still_ok & df['quantity'].isna()] = 'quantity is not numeric'
    still_ok = reason.isna()
    bad_range = df['quantity'].notna() & ((df['quantity'] < min_qty) | (df['quantity'] > max_qty))
    reason[still_ok & bad_range] = f'quantity outside {min_qty}..{max_qty}'
    still_ok = reason.isna()
    reason[still_ok & ~df['status'].isin(allowed_statuses)] = 'status not in allowed set'

    invalid_mask = reason.notna()
    invalid_rows = df[invalid_mask]
    invalid_reasons = reason[invalid_mask]
    valid_df = df[~invalid_mask].reset_index(drop=True)

    quarantine = [
        _quarantine_row(row.to_dict(), 'orders', r)
        for (_, row), r in zip(invalid_rows.iterrows(), invalid_reasons)
    ]
    return valid_df, quarantine


def build_staging(raw_dir, run_id: str):
    """Create cleaned, typed staging datasets."""
    raw_dir = Path(raw_dir)

    customers_df, customers_q = _clean_customers(raw_dir)
    products_df, products_q = _clean_products(raw_dir)
    orders_df, orders_q = _clean_orders(raw_dir)

    customers_df = _add_audit_columns(customers_df, run_id)
    products_df = _add_audit_columns(products_df, run_id)
    orders_df = _add_audit_columns(orders_df, run_id)

    staging_dir = path_for('staging_dir')
    staging_dir.mkdir(parents=True, exist_ok=True)
    customers_df.to_parquet(staging_dir / 'customers.parquet', index=False)
    products_df.to_parquet(staging_dir / 'products.parquet', index=False)
    orders_df.to_parquet(staging_dir / 'orders.parquet', index=False)

    quarantine_df = pd.DataFrame(customers_q + products_q + orders_q)
    if not quarantine_df.empty:
        quarantine_dir = path_for('quarantine_dir')
        quarantine_dir.mkdir(parents=True, exist_ok=True)
        quarantine_df.to_parquet(quarantine_dir / f'staging_run_id={run_id}.parquet', index=False)

    staging = {'customers': customers_df, 'products': products_df, 'orders': orders_df}
    return staging, quarantine_df