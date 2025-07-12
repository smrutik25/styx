from styx.common.operator import Operator
from styx.common.stateful_function import StatefulFunction


date_idx_operator = Operator('date_idx')


@date_idx_operator.register
async def get_date(ctx: StatefulFunction, order_key):
    date_key = ctx.get()
    ctx.call_remote_async(
        'line_order',
        'set_date',
        order_key,
        (date_key,)
    )
