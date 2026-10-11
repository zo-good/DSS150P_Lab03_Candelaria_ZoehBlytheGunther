"""Tiny builders for synthetic source data used across the tests."""
import json
from pathlib import Path

import pandas as pd

CUSTOMER_COLUMNS = ['customer_id', 'first_name', 'last_name', 'email', 'city',
                    'customer_tier', 'created_at', 'updated_at']
ORDER_COLUMNS = ['order_id', 'customer_id', 'product_id', 'order_timestamp', 'quantity',
                 'unit_price', 'discount_pct', 'status', 'updated_at']


def cust(**kw):
    row = dict(customer_id='C1', first_name='Ana', last_name='Reyes', email='ana@example.com',
               city='Manila', customer_tier='Gold', created_at='2024-01-01T00:00:00+00:00',
               updated_at='2025-01-01T00:00:00+00:00')
    row.update(kw)
    return row


def prod(**kw):
    row = dict(product_id='P1', name='Widget', category={'name': 'Tools', 'department': 'Hardware'},
               brand='Acme', unit_price=100.0, active=True, updated_at='2025-01-01T00:00:00+00:00')
    row.update(kw)
    return row


def order(**kw):
    row = dict(order_id='O1', customer_id='C1', product_id='P1',
               order_timestamp='2026-05-07T01:22:00+00:00', quantity=2, unit_price=100.0,
               discount_pct=0.1, status='PAID', updated_at='2026-05-07T04:22:00+00:00')
    row.update(kw)
    return row


def write_source(root, customers, products, orders):
    """Write the three source files into <root>/data/source."""
    source = Path(root) / 'data' / 'source'
    source.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(customers, columns=CUSTOMER_COLUMNS).to_csv(source / 'customers.csv', index=False)
    (source / 'products.json').write_text(json.dumps(products), encoding='utf-8')
    pd.DataFrame(orders, columns=ORDER_COLUMNS).to_csv(source / 'orders.csv', index=False)


# Five orders across three UTC months (May x2, June x2, July x1). The June/July
# pair sits exactly on the month boundary (23:59:59Z vs 00:00:00Z).
def default_dataset():
    customers = [cust(customer_id='C1'), cust(customer_id='C2', email='ben@example.com', city='Cebu')]
    products = [prod(product_id='P1'), prod(product_id='P2', name='Gadget', brand='Zenith')]
    orders = [
        order(order_id='O1', order_timestamp='2026-05-07T01:22:00+00:00'),
        order(order_id='O2', customer_id='C2', product_id='P2', order_timestamp='2026-05-20T10:00:00+00:00'),
        order(order_id='O3', order_timestamp='2026-06-01T00:00:00+00:00'),
        order(order_id='O4', customer_id='C2', order_timestamp='2026-06-30T23:59:59+00:00'),
        order(order_id='O5', product_id='P2', order_timestamp='2026-07-01T00:00:00+00:00'),
    ]
    return customers, products, orders
