from styx.common.operator import Operator
from styx.common.stateful_function import StatefulFunction


customer_idx_operator = Operator('customer_idx')


# @customer_idx_operator.register
# async def register_payment(ctx: StatefulFunction, order_key, amount):
#     cust_key = ctx.get()
#     ctx.call_remote_async(
#         'customer',
#         'register_payment',
#         cust_key,
#         (order_key, amount)
#     )


@customer_idx_operator.register
async def get_customer_id(ctx: StatefulFunction, order_key):
    cust_key = ctx.get()
    ctx.call_remote_async(
        'line_order',
        'set_customer_id',
        order_key,
        (cust_key,)
    )
