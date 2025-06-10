from styx.common.operator import Operator
from styx.common.stateful_function import StatefulFunction


customer_operator = Operator('customer')
"""
Attributes
----------
CUSTKEY -> ctx.key
ctx.value()
NAME
ADDRESS
CITY
NATION
REGION
PHONE
MKTSEGMENT
PAYMENTCNT
"""
customer_operator.set_analytical_schema([
    {
        "column_name": "CUSTKEY",
        "data_type": "BIGINT",
        "primary_key": True
    },
    {
        "column_name": "NAME",
        "data_type": "VARCHAR"
    },
    {
        "column_name": "ADDRESS",
        "data_type": "VARCHAR"
    },
    {
        "column_name": "CITY",
        "data_type": "VARCHAR"
    },
    {
        "column_name": "NATION",
        "data_type": "VARCHAR"
    },
    {
        "column_name": "REGION",
        "data_type": "VARCHAR"
    },
    {
        "column_name": "PHONE",
        "data_type": "VARCHAR"
    },
    {
        "column_name": "MKTSEGMENT",
        "data_type": "VARCHAR"
    },
    {
        "column_name": "PAYMENTCNT",
        "data_type": "INT"
    }
])


@customer_operator.register
async def register_payment(ctx: StatefulFunction, order_key, amount):
    customer_data = ctx.get()
    customer_data["PAYMENTCNT"] += 1
    history_key = f"{order_key}:{ctx.key}"
    ctx.call_remote_async(
        'history',
        'register_payment',
        history_key,
        (amount,)
    )
    ctx.put(customer_data)
