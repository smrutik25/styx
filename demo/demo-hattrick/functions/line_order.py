from styx.common.operator import Operator
from styx.common.stateful_function import StatefulFunction


line_order_operator = Operator('line_order')
"""
Attributes
----------
ORDERKEY <- ctx.key
LINENUMBER <- ctx.key
ctx.get()
CUSTKEY
PARTKEY
SUPKEY
ORDERDATE
ORDPRIORITY
SHIPPRIORITY
QUANTITY
EXTENDEDPRICE
ORDTOTALPRICE
DISCOUNT
REVENUE
SUPPLYCOST
TAX
COMMITDATE
SHIPMODE
"""


@line_order_operator.register
async def receive_total(ctx: StatefulFunction, total_price: int):
    value = ctx.get()
    value["ORDTOTALPRICE"] = total_price
    ctx.put(value)


@line_order_operator.register
async def receive_part(ctx: StatefulFunction, entrypoint_key, part_price: int):
    value = ctx.get()
    value["EXTENDEDPRICE"] = value["QUANTITY"] * part_price
    value["REVENUE"] = (value["EXTENDEDPRICE"] * (100 - value["DISCOUNT"])) / 100
    ctx.call_remote_async(
        'new_order_txn',
        'receive_part_extended_price',
        entrypoint_key,
        # needed to get back the reply to the entrypoint
        (value["EXTENDEDPRICE"], ctx.key)
    )
    ctx.put(value)


@line_order_operator.register
async def set_customer_id(ctx: StatefulFunction, cust_key):
    value = ctx.get()
    value["CUSTKEY"] = cust_key
    ctx.put(value)


@line_order_operator.register
async def set_supplier_id(ctx: StatefulFunction, sup_key):
    value = ctx.get()
    value["SUPKEY"] = sup_key
    ctx.put(value)


@line_order_operator.register
async def set_date(ctx: StatefulFunction, odate):
    value = ctx.get()
    value["ORDERDATE"] = odate
    value["COMMITDATE"] = odate  # TODO: Increment this by days
    ctx.put(value)


@line_order_operator.register
async def new_order_txn(ctx: StatefulFunction, entrypoint_key, params: dict):
    entry = {
        "PARTKEY": params['PK'],
        "ORDPRIORITY": params['OP'],
        "SHIPPRIORITY": params['SP'],
        "QUANTITY": params['Q'],
        "EXTENDEDPRICE": 0,  # For this specific part of the order
        "ORDTOTALPRICE": 0,  # The total price after every part is accounted for
        "DISCOUNT": params['D'],
        "REVENUE": 0,  # extended-price with the discount applied
        "SUPPLYCOST": params['SC'],
        "TAX": params['T'],
        "SHIPMODE": params['SM'],
    }
    ctx.call_remote_async(
        'customer_idx',
        'get_customer_id',
        params['CN'],
        (ctx.key, )
    )
    ctx.call_remote_async(
        'date',
        'get_date',
        params['OD'],
        (ctx.key,)
    )
    ctx.call_remote_async(
        'supplier_idx',
        'get_supplier_id',
        params['SN']
        (ctx.key, )
    )
    ctx.call_remote_async(
        'part',
        'get_part_price',
        params['PK'],
        # needed to get back the reply to the entrypoint
        (entrypoint_key, ctx.key)
    )
    ctx.put(entry)
