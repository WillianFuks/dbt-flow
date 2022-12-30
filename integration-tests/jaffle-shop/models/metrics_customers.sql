select * 
from {{ metrics.calculate(
    metric_list=[metric('average_order_amount'), metric('total_order_amount'), metric('derived_test')],
    grain='all_time',
    dimensions=[],
) }}
