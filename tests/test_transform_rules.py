"""Transformation rules: staging cleaning, quarantine, curated joins, amounts, hashing."""
import pandas as pd
import pytest

from factories import cust, order, prod, write_source
from src import pipeline
from src.common.runtime import curated_path_for, quarantine_path_for, staging_dir_for
from src.transform.curated import HASH_KEYS
from src.validate.quality import validate_curated


def run(project, run_id, customers=None, products=None, orders=None):
    write_source(project,
                 customers if customers is not None else [cust()],
                 products if products is not None else [prod()],
                 orders if orders is not None else [order()])
    pipeline.do_extract(run_id)
    return pipeline.do_transform(run_id)


# ---------------------------------------------------------------- dedupe
def test_latest_version_wins_regardless_of_file_order(project):
    result = run(
        project, 'r1',
        customers=[cust(customer_tier='Gold', updated_at='2025-06-01T00:00:00+00:00'),
                   cust(customer_tier='Bronze', updated_at='2024-01-01T00:00:00+00:00')],
        products=[prod(brand='New', updated_at='2025-06-01T00:00:00+00:00'),
                  prod(brand='Old', updated_at='2024-01-01T00:00:00+00:00')],
        orders=[order(status='SHIPPED', updated_at='2026-05-09T00:00:00+00:00'),
                order(status='PAID', updated_at='2026-05-07T04:22:00+00:00')],
    )
    staging = result['staging']
    assert len(staging['customers']) == len(staging['products']) == len(staging['orders']) == 1
    assert staging['customers'].iloc[0]['customer_tier'] == 'Gold'
    assert staging['products'].iloc[0]['brand'] == 'New'
    assert staging['orders'].iloc[0]['status'] == 'SHIPPED'
    assert len(result['staging_quarantine']) == 0  # superseded versions are dropped, not quarantined


# ------------------------------------------------------- customer cleaning
def test_email_and_city_are_normalised_and_missing_email_flagged(project):
    result = run(project, 'r1', customers=[
        cust(customer_id='C1', email='  Ana.Reyes@EXAMPLE.com ', city=' quezon CITY '),
        cust(customer_id='C2', email='', city='cebu'),
    ])
    customers = result['staging']['customers'].set_index('customer_id')
    assert customers.loc['C1', 'email'] == 'ana.reyes@example.com'
    assert customers.loc['C1', 'city'] == 'Quezon City'
    assert not customers.loc['C1', 'email_missing']
    assert customers.loc['C2', 'city'] == 'Cebu'
    assert customers.loc['C2', 'email_missing']


# --------------------------------------------------------- product quarantine
def test_invalid_product_prices_are_quarantined_and_zero_is_allowed(project):
    result = run(project, 'r1', products=[
        prod(product_id='P1', unit_price=100.0),
        prod(product_id='P2', unit_price=0.0),        # boundary: free item is valid
        prod(product_id='P3', unit_price=-5.0),
        prod(product_id='P4', unit_price='abc'),
        prod(product_id='P5', unit_price=None),
    ])
    assert sorted(result['staging']['products']['product_id']) == ['P1', 'P2']
    quarantine = result['staging_quarantine']
    assert len(quarantine) == 3
    assert set(quarantine['source']) == {'products'}
    assert set(quarantine['reason']) == {'invalid or negative unit_price'}


def test_nested_category_is_flattened(project):
    result = run(project, 'r1')
    row = result['staging']['products'].iloc[0]
    assert row['category_name'] == 'Tools' and row['category_department'] == 'Hardware'
    assert 'category' not in result['staging']['products'].columns


# ----------------------------------------------------------- order quarantine
@pytest.mark.parametrize('overrides, reason', [
    (dict(quantity=0), 'quantity outside 1..20'),
    (dict(quantity=21), 'quantity outside 1..20'),
    (dict(quantity='abc'), 'quantity is not numeric'),
    (dict(status='LOST'), 'status not in allowed set'),
    (dict(order_timestamp='not-a-date'), 'unparseable order_timestamp'),
    (dict(updated_at='garbage'), 'unparseable updated_at'),
])
def test_bad_order_is_quarantined_with_reason_and_good_order_survives(project, overrides, reason):
    result = run(project, 'r1', orders=[order(order_id='GOOD'), order(order_id='BAD', **overrides)])
    assert list(result['staging']['orders']['order_id']) == ['GOOD']
    quarantine = result['staging_quarantine']
    assert len(quarantine) == 1
    assert quarantine.iloc[0]['reason'] == reason
    assert quarantine.iloc[0]['source'] == 'orders'
    assert '"BAD"' in quarantine.iloc[0]['record']  # the rejected record is kept for review
    assert quarantine_path_for('staging', 'r1').is_file()


def test_quantity_boundaries_1_and_20_are_accepted(project):
    result = run(project, 'r1', orders=[order(order_id='LOW', quantity=1), order(order_id='HIGH', quantity=20)])
    assert sorted(result['staging']['orders']['order_id']) == ['HIGH', 'LOW']


def test_many_defects_are_quarantined_not_raised(project):
    bad = [order(order_id=f'B{i}', quantity=0) for i in range(5)]
    result = run(project, 'r1', orders=[order(order_id='G1'), order(order_id='G2')] + bad)
    assert len(result['staging']['orders']) == 2
    assert len(result['staging_quarantine']) == 5


# ------------------------------------------------------------ curated join
def test_orphan_orders_are_quarantined_with_both_reasons(project):
    result = run(project, 'r1', orders=[
        order(order_id='OK'),
        order(order_id='NOCUST', customer_id='C404'),
        order(order_id='NOPROD', product_id='P404'),
        order(order_id='NOBOTH', customer_id='C405', product_id='P405'),
    ])
    assert list(result['curated']['order_id']) == ['OK']
    quarantine = result['curated_quarantine']
    assert len(quarantine) == 3 and set(quarantine['source']) == {'curated_join'}
    reasons = ' | '.join(quarantine['reason'])
    assert 'customer_id C404 not found in customers' in reasons
    assert 'product_id P404 not found in products' in reasons
    assert any('C405' in r and 'P405' in r and '; ' in r for r in quarantine['reason'])
    assert quarantine_path_for('curated', 'r1').is_file()


def test_curated_carries_customer_and_product_attributes(project):
    result = run(project, 'r1')
    row = result['curated'].iloc[0]
    assert (row['customer_city'], row['customer_tier']) == ('Manila', 'Gold')
    assert (row['product_name'], row['brand'], row['category']) == ('Widget', 'Acme', 'Tools')


# ------------------------------------------------------------------ amounts
def test_amounts_are_computed_from_quantity_price_and_discount(project):
    result = run(project, 'r1', orders=[
        order(order_id='A', quantity=4, unit_price=250.0, discount_pct=0.1),
        order(order_id='B', quantity=3, unit_price=10.0, discount_pct=0),
    ])
    rows = result['curated'].set_index('order_id')
    assert rows.loc['A', 'gross_amount'] == pytest.approx(1000.0)
    assert rows.loc['A', 'discount_amount'] == pytest.approx(100.0)
    assert rows.loc['A', 'net_amount'] == pytest.approx(900.0)
    assert rows.loc['B', 'net_amount'] == pytest.approx(rows.loc['B', 'gross_amount']) == pytest.approx(30.0)
    assert (rows['net_amount'] == rows['gross_amount'] - rows['discount_amount']).all()


# ------------------------------------------------------------- record_hash
def test_hash_is_stable_across_runs_but_audit_columns_differ(project):
    first = run(project, 'run_one')['curated'].sort_values('order_id').reset_index(drop=True)
    pipeline.do_extract('run_two')
    second = pipeline.do_transform('run_two')['curated'].sort_values('order_id').reset_index(drop=True)
    assert list(first['record_hash']) == list(second['record_hash'])
    assert set(first['pipeline_run_id']) == {'run_one'} and set(second['pipeline_run_id']) == {'run_two'}


def test_hash_ignores_pipeline_columns():
    assert set(HASH_KEYS).isdisjoint({'pipeline_run_id', 'processed_at_utc', 'staged_at_utc', 'record_hash'})


@pytest.mark.parametrize('order_change, customer_change', [
    (dict(quantity=3), {}),
    (dict(status='SHIPPED'), {}),
    ({}, dict(city='Cebu')),
])
def test_hash_changes_when_business_data_changes(project, order_change, customer_change):
    base = run(project, 'base')['curated'].iloc[0]['record_hash']
    changed = run(project, 'changed', customers=[cust(**customer_change)],
                  orders=[order(**order_change)])['curated'].iloc[0]['record_hash']
    assert base != changed


# ---------------------------------------------------------- per-run outputs
def test_runs_write_to_their_own_files_and_do_not_overwrite_each_other(project):
    run(project, 'run_a', orders=[order(order_id='A1')])
    before = curated_path_for('run_a').read_bytes()
    run(project, 'run_b', orders=[order(order_id='B1'), order(order_id='B2')])
    assert curated_path_for('run_a').read_bytes() == before
    assert len(pd.read_parquet(curated_path_for('run_a'))) == 1
    assert len(pd.read_parquet(curated_path_for('run_b'))) == 2
    assert staging_dir_for('run_a') != staging_dir_for('run_b')
    assert len(pd.read_parquet(staging_dir_for('run_a') / 'orders.parquet')) == 1
    assert len(pd.read_parquet(staging_dir_for('run_b') / 'orders.parquet')) == 2


# --------------------------------------------------------- validate_curated
def test_validate_curated_accepts_clean_data_and_flags_each_defect(project):
    clean = run(project, 'r1', orders=[order(order_id='A'), order(order_id='B')])['curated']
    assert validate_curated(clean) == []

    def errors_for(mutate):
        frame = clean.copy()
        mutate(frame)
        return ' | '.join(validate_curated(frame))

    assert 'duplicate order_id' in errors_for(lambda f: f.__setitem__('order_id', 'A'))
    assert 'null order_id' in errors_for(lambda f: f.loc.__setitem__((0, 'order_id'), None))
    assert 'quantity outside' in errors_for(lambda f: f.loc.__setitem__((0, 'quantity'), 99))
    assert 'negative net_amount' in errors_for(lambda f: f.loc.__setitem__((0, 'net_amount'), -1.0))
    assert 'status outside the allowed set' in errors_for(lambda f: f.loc.__setitem__((0, 'status'), 'LOST'))
    assert 'missing required audit field: record_hash' in errors_for(
        lambda f: f.loc.__setitem__((0, 'record_hash'), None))
