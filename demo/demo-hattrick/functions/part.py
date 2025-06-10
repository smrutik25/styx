from styx.common.operator import Operator
from styx.common.stateful_function import StatefulFunction


part_operator = Operator('part')

part_operator.set_analytical_schema([
    {
        "column_name": "PARTKEY",
        "data_type": "BIGINT",
        "primary_key": True
    },
    {
        "column_name": "NAME",
        "data_type": "VARCHAR"
    },
    {
        "column_name": "MFGR",
        "data_type": "VARCHAR"
    },
    {
        "column_name": "CATEGORY",
        "data_type": "VARCHAR"
    },
    {
        "column_name": "BRAND1",
        "data_type": "VARCHAR"
    },
    {
        "column_name": "COLOR",
        "data_type": "VARCHAR"
    },
    {
        "column_name": "TYPE",
        "data_type": "VARCHAR"
    },
    {
        "column_name": "SIZE",
        "data_type": "INT"
    },
    {
        "column_name": "CONTAINER",
        "data_type": "VARCHAR"
    },
    {
        "column_name": "PRICE",
        "data_type": "FLOAT"
    }
])


@part_operator.register
async def get_part_price(ctx: StatefulFunction, entrypoint_key, line_oder_key):
    value = ctx.get()
    price = value["PRICE"]
    ctx.call_remote_async(
        'line_order',
        'receive_part',
        line_oder_key,
        # needed to get back the reply to the entrypoint
        (entrypoint_key, price)
    )
