from pyspark.sql.functions import col, when, avg

# sort installments into 
from pyspark.sql.functions import col, when, avg, round
from pyspark.sql import DataFrame

# clip outliers from per installment value and assigns installment value to the max amount 
# flags installments with high value as 1, absed on the average threshold
# adds a column "used_voucher" if voucher is used as opne of the payment type

def add_high_installment_flag(df: DataFrame,
                               installment_col="payment_installments",
                               value_col="payment_value",
                               sequential_col="payment_sequential",
                               payment_type_col="payment_type") -> DataFrame:
    """
    Adds:
    - 'installment_value': per-installment cost (with outliers capped)
    - 'high_installment_flag': binary flag based on avg thresholds
    - 'used_voucher': 1 if payment_type == 'voucher'
    """

    # Step 1: Flag voucher payments
    df = df.withColumn("used_voucher", when(col(payment_type_col) == "voucher", 1).otherwise(0))

    # Step 2: Calculate raw installment value
    df = df.withColumn(
        "installment_value",
        round(when(col(installment_col) > 0, col(value_col) / col(installment_col)).otherwise(0), 2)
    )

    # Step 3: Compute IQR bounds for capping
    # Using approxQuantile for performance on large data
    iqr_bounds = {}
    for field in ["installment_value", installment_col]:
        q1, q3 = df.approxQuantile(field, [0.25, 0.75], 0.05)
        iqr = q3 - q1
        iqr_bounds[field] = {
            "lower": q1 - 1.5 * iqr,
            "upper": q3 + 1.5 * iqr
        }

    # Step 4: Cap outliers based on IQR
    df = df.withColumn(
        "installment_value_capped",
        when(col("installment_value") < iqr_bounds["installment_value"]["lower"], iqr_bounds["installment_value"]["lower"])
        .when(col("installment_value") > iqr_bounds["installment_value"]["upper"], iqr_bounds["installment_value"]["upper"])
        .otherwise(col("installment_value"))
    ).withColumn(
        "payment_installments_capped",
        when(col(installment_col) < iqr_bounds[installment_col]["lower"], iqr_bounds[installment_col]["lower"])
        .when(col(installment_col) > iqr_bounds[installment_col]["upper"], iqr_bounds[installment_col]["upper"])
        .otherwise(col(installment_col))
    )

    # Step 5: Recalculate averages (exclude vouchers and zeros)
    df_valid = df.filter((col(value_col) > 0) & (col("used_voucher") == 0))
    averages = df_valid.select(
        avg("payment_installments_capped").alias("avg_installments"),
        avg("installment_value_capped").alias("avg_installment_value")
    ).first()

    avg_installments = averages["avg_installments"]
    avg_installment_value = averages["avg_installment_value"]

    # Step 6: Assign high installment flag using capped values
    df_result = df.withColumn(
        "high_installment_flag",
        when(col(sequential_col) == 0, 0)
        .when(
            (col("payment_installments_capped") >= avg_installments) |
            (col("installment_value_capped") <= avg_installment_value),
            1
        ).otherwise(0)
    )

    df_result = df_result.select("order_id", "payment_type", "payment_sequential", "payment_value", "payment_installments", "installment_value", "installment_value_capped", "high_installment_flag", "used_voucher")

    return df_result

df_installments = add_high_installment_flag(df_order_payments)
df_installments.show(10)