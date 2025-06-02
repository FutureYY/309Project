from cleaned_data import df_product_category
from pyspark.sql.functions import lower, trim, countDistinct, col, when, sum as spark_sum
from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline

# get product category in english by combining with df_order_items

def get_category_in_english(df_order_items, df_products):

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

def group_categories_by_sales_with_ohe(df, category_col="product_category_name_english", value_col="price", threshold=0.8):

    # Step 1: Calculate total sales per category
    sales_per_category = df.groupBy(category_col) \
        .agg(spark_sum(value_col).alias("total_sales")) \
        .orderBy("total_sales", ascending=False)

    # Step 2: Convert to Pandas to calculate cumulative sales % and rank
    sales_pd = sales_per_category.toPandas()
    total_sales = sales_pd["total_sales"].sum()

    sales_pd["category_sales_rank"] = range(1, len(sales_pd) + 1)
    sales_pd["category_sales_percent"] = sales_pd["total_sales"] / total_sales
    sales_pd["cumulative_pct"] = sales_pd["category_sales_percent"].cumsum()

    # Step 3: Identify top categories
    top_categories = sales_pd[sales_pd["cumulative_pct"] <= threshold][category_col].tolist()

    # Step 4: Convert back to Spark
    spark = df.sparkSession
    sales_enriched = spark.createDataFrame(sales_pd)

    # Step 5: Join stats to original
    df_enriched = df.join(
        sales_enriched.select(
            category_col,
            "total_sales",
            "category_sales_rank",
            "category_sales_percent"
        ),
        on=category_col,
        how="left"
    )

    # Step 6: Add grouped label
    df_labeled = df_enriched.withColumn(
        "category_grouped",
        when(col(category_col).isin(top_categories), col(category_col)).otherwise("other")
    )

    # Step 7: One-hot encode the grouped category
    indexer = StringIndexer(inputCol="category_grouped", outputCol="category_grouped_index", handleInvalid="keep")
    encoder = OneHotEncoder(inputCols=["category_grouped_index"], outputCols=["category_grouped_ohe"])
    pipeline = Pipeline(stages=[indexer, encoder])
    model = pipeline.fit(df_labeled)
    df_final = model.transform(df_labeled)

    df_final.select("product_id","product_category_name_english", "category_grouped", "category_grouped_ohe").show(truncate=False)

    return df_final