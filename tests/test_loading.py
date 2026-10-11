"""Loading into PostgreSQL: idempotency, change detection, stale-data guard,
transactions, concurrency and selected-partition loads. Uses a throwaway database
(skipped automatically when no PostgreSQL server is reachable)."""
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import pytest

from factories import cust, default_dataset, order, prod
from src import pipeline
from src.common.db import connect
from src.common.runtime import read_manifest
from src.load.postgres import (
    count_partition_rows, count_rows, load_partition, upsert_curated, verify_loaded,
)

pytestmark = pytest.mark.postgres


def fetch(sql, params=None):
    with connect() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def test_first_load_inserts_everything_and_a_rerun_changes_nothing(pg, make_curated):
    df = make_curated('r1')
    assert upsert_curated(df, 'r1') == 5
    assert upsert_curated(df, 'r1') == 0          # idempotent: nothing rewritten
    assert count_rows() == 5
    assert fetch('SELECT count(DISTINCT order_id) FROM curated.sales_order_lines') == [(5,)]


def test_reload_from_a_new_run_with_identical_data_still_changes_nothing(pg, make_curated):
    upsert_curated(make_curated('r1'), 'r1')
    again = make_curated('r2')                     # new run id, same source data
    assert upsert_curated(again, 'r2') == 0
    assert set(r[0] for r in fetch('SELECT pipeline_run_id FROM curated.sales_order_lines')) == {'r1'}


def test_a_changed_source_row_updates_only_that_row(pg, make_curated):
    upsert_curated(make_curated('r1'), 'r1')
    customers, products, orders = default_dataset()
    orders[1] = order(order_id='O2', customer_id='C2', product_id='P2', quantity=9,
                      order_timestamp='2026-05-20T10:00:00+00:00', updated_at='2026-06-01T00:00:00+00:00')
    changed = make_curated('r2', orders=orders)
    assert upsert_curated(changed, 'r2') == 1
    rows = {r[0]: r[1:] for r in fetch('SELECT order_id, quantity, pipeline_run_id FROM curated.sales_order_lines')}
    assert rows['O2'] == (9, 'r2')
    assert all(rows[k][1] == 'r1' for k in ('O1', 'O3', 'O4', 'O5'))


def test_an_older_snapshot_cannot_overwrite_newer_data(pg, make_curated):
    customers, products, orders = default_dataset()
    newer = [dict(o) for o in orders]
    newer[0] = order(order_id='O1', quantity=9, updated_at='2026-06-10T00:00:00+00:00')
    older = [dict(o) for o in orders]
    older[0] = order(order_id='O1', quantity=3, updated_at='2026-05-08T00:00:00+00:00')
    assert upsert_curated(make_curated('newer', orders=newer), 'newer') == 5
    assert upsert_curated(make_curated('older', orders=older), 'older') == 0   # delayed run loses
    assert fetch("SELECT quantity FROM curated.sales_order_lines WHERE order_id='O1'") == [(9,)]


def test_a_correction_with_the_same_source_timestamp_is_applied(pg, make_curated):
    customers, products, orders = default_dataset()
    fixed = [dict(o) for o in orders]
    fixed[0] = order(order_id='O1', quantity=7)       # same updated_at as the original O1
    upsert_curated(make_curated('r1'), 'r1')
    assert upsert_curated(make_curated('r2', orders=fixed), 'r2') == 1
    assert fetch("SELECT quantity FROM curated.sales_order_lines WHERE order_id='O1'") == [(7,)]


def test_missing_values_are_stored_as_sql_null(pg, make_curated):
    customers, products, orders = default_dataset()
    products[0] = prod(product_id='P1', brand=None)
    upsert_curated(make_curated('r1', products=products), 'r1')
    assert fetch("SELECT brand FROM curated.sales_order_lines WHERE order_id='O1'") == [(None,)]


def test_a_failing_row_rolls_back_the_whole_batch(pg, make_curated):
    df = make_curated('r1').copy()
    df['quantity'] = df['quantity'].astype(object)
    df.loc[df['order_id'] == 'O3', 'quantity'] = None       # violates NOT NULL, mid-batch
    with pytest.raises(Exception):
        upsert_curated(df, 'r1')
    assert count_rows() == 0                                  # nothing from the batch survived


def test_concurrent_loads_of_the_same_data_insert_each_row_exactly_once(pg, make_curated):
    df = make_curated('r1')
    with ThreadPoolExecutor(max_workers=4) as pool:
        affected = list(pool.map(lambda _: upsert_curated(df, 'r1'), range(4)))
    assert sum(affected) == 5 and sorted(affected) == [0, 0, 0, 5]
    assert count_rows() == 5


def test_concurrent_loads_from_different_runs_do_not_deadlock_or_duplicate(pg, make_curated):
    frames = [make_curated(f'r{i}') for i in range(4)]
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(lambda args: upsert_curated(args[1], f'r{args[0]}'), enumerate(frames)))
    assert count_rows() == 5


# --------------------------------------------------------- partition loads
def test_partition_load_stores_only_the_selected_month_and_audits_it(pg, make_curated):
    from src.benchmark.partitions import select_partition
    df = make_curated('r1')
    may = select_partition(df, 2026, 5)
    assert load_partition(may, 2026, 5, 'r1') == 2
    assert count_partition_rows(2026, 5) == 2 and count_partition_rows(2026, 6) == 0
    assert count_rows() == 2
    assert fetch('SELECT partition_key, row_count, pipeline_run_id FROM audit.partition_loads') == [('2026-05', 2, 'r1')]
    assert load_partition(may, 2026, 5, 'r2') == 0            # reload: no changes, still ONE audit row
    assert fetch('SELECT partition_key, row_count, pipeline_run_id FROM audit.partition_loads') == [('2026-05', 2, 'r2')]


def test_partition_load_refuses_rows_from_other_months_and_writes_nothing(pg, make_curated):
    df = make_curated('r1')
    with pytest.raises(ValueError, match='do not belong to partition 2026-05'):
        load_partition(df, 2026, 5, 'r1')
    assert count_rows() == 0 and fetch('SELECT count(*) FROM audit.partition_loads') == [(0,)]


def test_partition_counts_follow_utc_even_when_the_database_timezone_differs(pg, pg_database, make_curated):
    with connect(dbname='postgres') as admin:
        admin.autocommit = True
        admin.execute(f"ALTER DATABASE {pg_database} SET timezone TO 'Asia/Manila'")
    upsert_curated(make_curated('r1'), 'r1')
    # O4 is 2026-06-30T23:59:59Z = 2026-07-01 07:59:59 in Manila; UTC says June.
    assert count_partition_rows(2026, 6) == 2 and count_partition_rows(2026, 7) == 1


# ----------------------------------------------------------- verification
def test_verify_loaded_detects_missing_rows(pg, make_curated):
    df = make_curated('r1')
    assert verify_loaded(df) == [f'5 of 5 curated order_id(s) are missing from PostgreSQL']
    upsert_curated(df, 'r1')
    assert verify_loaded(df) == []
    with connect() as conn:
        conn.execute("DELETE FROM curated.sales_order_lines WHERE order_id = 'O2'")
    assert verify_loaded(df) == ['1 of 5 curated order_id(s) are missing from PostgreSQL']


def test_validate_stage_checks_only_what_was_loaded(pg, make_curated):
    make_curated('r1')
    result = pipeline.do_validate('r1', check_db=True)
    assert 'no successful load is recorded' in result['errors'][0]   # never loaded -> not "validated"

    pipeline.do_load_partition('r1', 2026, 5)
    result = pipeline.do_validate('r1', check_db=True)               # scope = the loaded month only
    assert result['errors'] == [] and result['database_scope'] == '2026-05'
    assert result['rows'] == 5 and result['database_rows_checked'] == 2   # 5 validated, 2 confirmed in the DB
    assert read_manifest('r1')['stages']['load']['scope'] == '2026-05'

    pipeline.do_load('r1')                                            # now the full file
    result = pipeline.do_validate('r1', check_db=True)
    assert result['errors'] == [] and result['database_scope'] == 'full' and result['database_rows_checked'] == 5
