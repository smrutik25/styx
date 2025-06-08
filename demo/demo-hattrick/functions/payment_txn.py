from styx.common.operator import Operator
from styx.common.stateful_function import StatefulFunction


payment_txn_operator = Operator('payment_txn')
# Stateless


@payment_txn_operator.register
async def payment_txn(ctx: StatefulFunction, params: dict):
    amount = params["AMT"]
    order_key = params["ORDERKEY"]
    supp_key = params["SUPPKEY"]
    if "C_NAME" in params:
        # Customer selection by C_NAME
        ctx.call_remote_async(
            'customer_idx',
            'register_payment',
            params["C_NAME"],
            (order_key, amount)
        )
    else:
        # Customer selection by CUSTKEY
        ctx.call_remote_async(
            'customer',
            'register_payment',
            params["CUSTKEY"],
            (order_key, amount)
        )
    ctx.call_remote_async(
        'supplier',
        'register_payment',
        supp_key,
        (amount, )
    )
