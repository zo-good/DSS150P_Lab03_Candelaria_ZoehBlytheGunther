import pandas as pd
from pathlib import Path
from src.common.audit import utc_now_iso, record_hash
from src.config import path_for

HASH_KEYS = [
    'order_id', 'customer_id', 'product_id', 'order_timestamp', 'source_updated_at',
    'customer_city', 'customer_tier', 'product_name', 'category', 'brand',
    'quantity', 'unit_price', 'discount_pct', 'status',
]

KEEP_COLUMNS = [
    'order_id', 'customer_id', 'product_id', 'order_timestamp',
    'customer_city', 'customer_tier', 'product_name', 'category', 'brand',
    'quantity', 'unit_price', 'discount_pct',
    'gross_amount', 'discount_amount', 'net_amount', 'status',
    'source_updated_at', 'pipeline_run_id', 'processed_at_utc', 'record_hash',
]


def build_curated(staging: dict, run_id: str):
    """Join staging orders/customers/products and create analysis-ready sales rows."""
    orders = staging['orders']
    customers = staging['customers']
    products = staging['products']

    merged = orders.merge(
        customers[['customer_id', 'city', 'customer_tier']],
        on='customer_id', how='left', indicator='_customer_match'
    )
    merged = merged.merge(
        products[['product_id', 'name', 'brand', 'category_name']],
        on='product_id', how='left', indicator='_product_match'
    )

    orphan_customer = merged['_customer_match'] == 'left_only'
    orphan_product = merged['_product_match'] == 'left_only'
    orphan_mask = orphan_customer | orphan_product

    quarantine_rows = []
    for idx in merged.index[orphan_mask]:
        row = merged.loc[idx]
        reasons = []
        if orphan_customer.loc[idx]:
            reasons.append(f"customer_id {row['customer_id']} not found in customers")
        if orphan_product.loc[idx]:
            reasons.append(f"product_id {row['product_id']} not found in products")
        quarantine_rows.append({
            'source': 'curated_join',
            'reason': '; '.join(reasons),
            'quarantined_at_utc': utc_now_iso(),
            'record': str(row.drop(labels=['_customer_match', '_product_match']).to_dict()),
        })

    curated = merged[~orphan_mask].drop(columns=['_customer_match', '_product_match']).reset_index(drop=True)
    curated = curated.rename(columns={'city': 'customer_city', 'name': 'product_name', 'category_name': 'category'})

    curated['quantity'] = curated['quantity'].astype(int)
    curated['gross_amount'] = curated['quantity'] * curated['unit_price']
    curated['discount_amount'] = curated['gross_amount'] * curated['discount_pct']
    curated['net_amount'] = curated['gross_amount'] - curated['discount_amount']

    curated['source_updated_at'] = curated['updated_at']
    curated['pipeline_run_id'] = run_id
    curated['processed_at_utc'] = utc_now_iso()
    curated['record_hash'] = curated.apply(lambda r: record_hash(r.to_dict(), HASH_KEYS), axis=1)

    curated = curated[KEEP_COLUMNS]

    curated_dir = path_for('curated_dir')
    curated_dir.mkdir(parents=True, exist_ok=True)
    curated.to_parquet(curated_dir / f'sales_order_lines_run_id={run_id}.parquet', index=False)

    quarantine_df = pd.DataFrame(quarantine_rows)
    if not quarantine_df.empty:
        quarantine_dir = path_for('quarantine_dir')
        quarantine_dir.mkdir(parents=True, exist_ok=True)
        quarantine_df.to_parquet(quarantine_dir / f'curated_run_id={run_id}.parquet', index=False)

    return curated, quarantine_df