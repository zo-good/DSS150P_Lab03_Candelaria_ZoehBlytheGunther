from src.config import SETTINGS

REQUIRED_AUDIT_FIELDS = ['source_updated_at', 'pipeline_run_id', 'processed_at_utc', 'record_hash']


def validate_curated(df) -> list[str]:
    """Return a list of human-readable validation errors."""
    errors = []

    null_ids = df['order_id'].isna().sum()
    if null_ids > 0:
        errors.append(f'{null_ids} row(s) have a null order_id')

    dupe_ids = df['order_id'].duplicated().sum()
    if dupe_ids > 0:
        errors.append(f'{dupe_ids} duplicate order_id value(s) found in curated data')

    min_qty = SETTINGS['quality']['min_quantity']
    max_qty = SETTINGS['quality']['max_quantity']
    bad_qty = df[df['quantity'].isna() | (df['quantity'] < min_qty) | (df['quantity'] > max_qty)]
    if len(bad_qty) > 0:
        errors.append(f'{len(bad_qty)} row(s) have quantity outside {min_qty}..{max_qty} or null')

    for col in ['gross_amount', 'discount_amount', 'net_amount']:
        negative = df[df[col] < 0]
        if len(negative) > 0:
            errors.append(f'{len(negative)} row(s) have negative {col}')

    allowed_statuses = set(SETTINGS['quality']['allowed_order_statuses'])
    bad_status = df[~df['status'].isin(allowed_statuses)]
    if len(bad_status) > 0:
        errors.append(f'{len(bad_status)} row(s) have a status outside the allowed set')

    for field in REQUIRED_AUDIT_FIELDS:
        missing = df[field].isna().sum()
        if missing > 0:
            errors.append(f'{missing} row(s) missing required audit field: {field}')

    return errors