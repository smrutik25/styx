from styx.common.operator import Operator
from styx.common.stateful_function import StatefulFunction


supplier_idx_operator = Operator('supplier_idx')


@supplier_idx_operator.register
async def get_supplier_id(ctx: StatefulFunction, order_key):
    sup_key = ctx.get()
    ctx.call_remote_async(
        'line_order',
        'set_supplier_id',
        order_key,
        (sup_key,)
    )
