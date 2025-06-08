from styx.common.operator import Operator
from styx.common.stateful_function import StatefulFunction


part_operator = Operator('part')
"""
Attributes
-------------
PARTKEY -> ctx.key
ctx.get()
NAME
MFGR
CATEGORY
BRAND1
COLOR
TYPE
SIZE
CONTAINER
PRICE
"""


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
