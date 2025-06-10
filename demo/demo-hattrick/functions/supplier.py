from styx.common.operator import Operator
from styx.common.stateful_function import StatefulFunction


supplier_operator = Operator('supplier')
supplier_operator.set_analytical_schema([
    {
        "column_name": "SUPKEY",
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
        "column_name": "YTD",
        "data_type": "FLOAT"
    },
])


@supplier_operator.register
async def register_payment(ctx: StatefulFunction, amount):
    value = ctx.get()
    value["YTD"] += amount
    ctx.put(value)
