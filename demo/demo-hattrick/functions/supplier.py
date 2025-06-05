from styx.common.operator import Operator
from styx.common.stateful_function import StatefulFunction


supplier_operator = Operator('supplier')
"""
Attributes
----------
SUPPKEY -> ctx.key
ctx.value
NAME
ADDRESS
CITY
NATION
REGION
PHONE
YTD
"""

@supplier_operator.register
async def register_payment(ctx: StatefulFunction, amount):
    value = ctx.get()
    value["YTD"] += amount
    ctx.put(value)
