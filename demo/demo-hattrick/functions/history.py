from styx.common.operator import Operator
from styx.common.stateful_function import StatefulFunction


history_operator = Operator('history')
"""
Attributes
---------
ORDERKEY -> ctx.key
CUSTKEY -> ctx.key
ctx.value
AMOUNT
"""


@history_operator.register
async def register_payment(ctx: StatefulFunction, amount):
    ctx.put({"AMOUNT": amount})
