from styx.common.operator import Operator
from styx.common.stateful_function import StatefulFunction


date_operator = Operator('date')


@date_operator.register
async def get_date(ctx: StatefulFunction, order_key):
    date_val = ctx.get()
    ctx.call_remote_async(
        'line_order',
        'set_date',
        order_key,
        (date_val,)
    )
