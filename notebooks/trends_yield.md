# Results from trends yield


```python
# Import dependencies
from biotrade.faostat import faostat
from biotrade.common.time_series import segmented_regression, plot_segmented_regression
import pandas as pd
```


```python
# Main producers for each commodity
product_reporter_dict = {
    254: ["Malaysia", "Indonesia"],
    236: ["Brazil"],
    661: ["Côte d'Ivoire"],
    656: ["Brazil", "Viet Nam"],
}
```


```python
# Select yields
yield_data = pd.DataFrame()
for product_code in product_reporter_dict.keys():
    data = faostat.db.select(
        table="crop_production",
        reporter=product_reporter_dict[product_code],
        product_code=product_code,
        element="yield",
        period_start=1986,
    )
    yield_data = pd.concat([yield_data, data], ignore_index=True)

# Use as objective function the residual sum of squares (RSS) with at least 7 points for the linear regression
regression = segmented_regression(
    yield_data,
    last_value=False,
    function="R2",
    alpha=0.05,
    min_data_points=7,
    multi_process=True,
)
# Plot results
plot_segmented_regression(regression)
```
