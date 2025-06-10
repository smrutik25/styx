from styx.common.operator import Operator
from styx.common.stateful_function import StatefulFunction


history_operator = Operator('history')
history_operator.set_analytical_schema([
    {
        "column_name": "ORDERKEY",
        "data_type": "BIGINT",
        "primary_key": True
    },
    {
        "column_name": "CUSTKEY",
        "data_type": "BIGINT",
        "primary_key": True
    },
    {
        "column_name": "AMOUNT",
        "data_type": "FLOAT"
    }
])

@history_operator.register
async def register_payment(ctx: StatefulFunction, amount):
    ctx.put({"AMOUNT": amount})
    return ctx.key, amount
