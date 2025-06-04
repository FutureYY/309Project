from pyspark.sql.functions import lower, trim, countDistinct, col, when, sum as spark_sum
from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline

# get product category in english by combining with df_order_items

def get_category_in_english(df_order_items, df_products, df_product_category):
 
    df_products_clean = df_products.withColumn(
        "product_category_name",
        lower(trim("product_category_name"))
    )
    df_category_clean = df_product_category.withColumn(
        "product_category_name",
        lower(trim("product_category_name"))
    )

    df_products_english = df_products_clean.join(
        df_category_clean,
        on="product_category_name",
        how="left"
    )
    df_category_price = df_order_items.join(
        df_products_english,
        on="product_id",
        how="left"
    )

    df_category_price = df_category_price.select('product_id', 'order_id', 'order_item_id', 'seller_id', 'price', 'product_category_name_english')

    return df_category_price


# group categories that contribute little to the overall percentage sales as 'others'
# do one hot encoding on all the categories so that the model can process it

from pyspark.sql.functions import col, when, sum as spark_sum
from pyspark.sql.window import Window
from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline

def group_categories_by_sales_with_ohe(df_category_price, category_col="product_category_name_english", value_col="price", threshold=0.8):
    
    # Step 1: Calculate total sales per category
    sales_per_category = df_category_price.groupBy(category_col) \
        .agg(spark_sum(value_col).alias("total_sales"))

    # Step 2: Calculate total overall sales (scalar)
    total_sales = df_category_price.agg(spark_sum(value_col).alias("overall_total")).collect()[0]["overall_total"]

    # Step 3: Calculate percent and cumulative percent
    window_spec = Window.orderBy(col("total_sales").desc()).rowsBetween(Window.unboundedPreceding, 0)

    sales_enriched = sales_per_category \
        .withColumn("category_sales_percent", col("total_sales") / total_sales) \
        .withColumn("cumulative_pct", spark_sum("category_sales_percent").over(window_spec))

    # Step 4: Extract top categories within threshold
    top_categories_rows = sales_enriched \
    .filter(col("cumulative_pct") <= threshold) \
    .select(category_col) \
    .collect()

    top_categories = [row[category_col] for row in top_categories_rows]


    # Step 5: Join stats to original
    df_enriched = df_category_price.join(
        sales_enriched.select(category_col, "total_sales", "category_sales_percent"),
        on=category_col,
        how="left"
    )

    # Step 6: Label top vs "other"
    df_labeled = df_enriched.withColumn(
        "category_grouped",
        when(col(category_col).isin(top_categories), col(category_col)).otherwise("other")
    )

    # Step 7: One-hot encode the grouped category
    indexer = StringIndexer(inputCol="category_grouped", outputCol="category_grouped_index", handleInvalid="keep")
    encoder = OneHotEncoder(inputCols=["category_grouped_index"], outputCols=["category_grouped_ohe"])
    pipeline = Pipeline(stages=[indexer, encoder])
    model = pipeline.fit(df_labeled)
    df_ohe = model.transform(df_labeled)

    return df_ohe
