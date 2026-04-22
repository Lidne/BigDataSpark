from __future__ import annotations

from urllib.parse import urlencode
from urllib.request import Request, urlopen

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from config import get_settings


def ensure_clickhouse_database(host: str, port: int, db: str, user: str, password: str) -> None:
    query = urlencode(
        {
            "query": f"CREATE DATABASE IF NOT EXISTS {db}",
            "user": user,
            "password": password,
        }
    )
    request = Request(f"http://{host}:{port}/?{query}", data=b"", method="POST")
    with urlopen(request) as response:
        response.read()


def write_clickhouse(dataframe, table_name: str, jdbc_url: str, user: str, password: str) -> None:
    (
        dataframe.write.format("jdbc")
        .mode("overwrite")
        .option("url", jdbc_url)
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .option("dbtable", table_name)
        .option("user", user)
        .option("password", password)
        .option("createTableOptions", "ENGINE = MergeTree() ORDER BY tuple()")
        .save()
    )


def main() -> None:
    settings = get_settings()
    pg_settings = settings.postgres
    ch_settings = settings.clickhouse

    pg_jdbc_url = pg_settings.jdbc_url
    pg_jdbc_properties = pg_settings.jdbc_properties
    ch_jdbc_url = ch_settings.jdbc_url
    ensure_clickhouse_database(
        ch_settings.host,
        ch_settings.port,
        ch_settings.db,
        ch_settings.user,
        ch_settings.password,
    )

    spark = (
        SparkSession.builder.appName("reports-to-clickhouse").config("spark.sql.session.timeZone", "UTC").getOrCreate()
    )

    fact_sales = spark.read.jdbc(pg_jdbc_url, "dwh.fact_sales", properties=pg_jdbc_properties)
    dim_customers = spark.read.jdbc(pg_jdbc_url, "dwh.dim_customers", properties=pg_jdbc_properties)
    dim_products = spark.read.jdbc(pg_jdbc_url, "dwh.dim_products", properties=pg_jdbc_properties)
    dim_dates = spark.read.jdbc(pg_jdbc_url, "dwh.dim_dates", properties=pg_jdbc_properties)
    dim_stores = spark.read.jdbc(pg_jdbc_url, "dwh.dim_stores", properties=pg_jdbc_properties)
    dim_suppliers = spark.read.jdbc(pg_jdbc_url, "dwh.dim_suppliers", properties=pg_jdbc_properties)

    sales = (
        fact_sales.alias("f")
        .join(dim_customers.alias("c"), F.col("f.customer_id") == F.col("c.customer_id"), "left")
        .join(dim_products.alias("p"), F.col("f.product_id") == F.col("p.product_id"), "left")
        .join(dim_dates.alias("d"), F.col("f.date_id") == F.col("d.date_id"), "left")
        .join(dim_stores.alias("st"), F.col("f.store_id") == F.col("st.store_id"), "left")
        .join(dim_suppliers.alias("sp"), F.col("f.supplier_id") == F.col("sp.supplier_id"), "left")
        .select(
            F.col("f.sale_id").alias("sale_id"),
            F.col("f.customer_id").alias("customer_id"),
            F.col("f.product_id").alias("product_id"),
            F.col("f.supplier_id").alias("supplier_id"),
            F.col("f.store_id").alias("store_id"),
            F.col("f.date_id").alias("date_id"),
            F.col("f.sale_quantity").alias("sale_quantity"),
            F.col("f.sale_total_price").alias("sale_total_price"),
            F.col("p.product_name").alias("product_name"),
            F.col("p.product_category").alias("product_category"),
            F.col("p.product_brand").alias("product_brand"),
            F.col("p.product_price").alias("product_price"),
            F.col("p.product_rating").alias("product_rating"),
            F.col("p.product_reviews").alias("product_reviews"),
            F.col("c.customer_first_name").alias("customer_first_name"),
            F.col("c.customer_last_name").alias("customer_last_name"),
            F.col("c.customer_country").alias("customer_country"),
            F.col("d.year").alias("year"),
            F.col("d.quarter").alias("quarter"),
            F.col("d.month").alias("month"),
            F.col("d.month_name").alias("month_name"),
            F.col("st.store_name").alias("store_name"),
            F.col("st.store_city").alias("store_city"),
            F.col("st.store_country").alias("store_country"),
            F.col("sp.supplier_name").alias("supplier_name"),
            F.col("sp.supplier_country").alias("supplier_country"),
        )
    )

    product_window = Window.orderBy(F.desc("total_revenue"))
    customer_window = Window.orderBy(F.desc("total_spent"))
    store_window = Window.orderBy(F.desc("total_revenue"))
    supplier_window = Window.orderBy(F.desc("total_revenue"))
    quality_desc_window = Window.orderBy(F.desc("avg_rating"))
    quality_asc_window = Window.orderBy(F.asc("avg_rating"))
    review_window = Window.orderBy(F.desc("review_count"))

    sales_by_product = (
        sales.groupBy("product_id", "product_name", "product_category", "product_brand")
        .agg(
            F.sum("sale_quantity").alias("total_units_sold"),
            F.round(F.sum("sale_total_price"), 2).alias("total_revenue"),
            F.round(F.avg("product_rating"), 2).alias("avg_rating"),
            F.max("product_reviews").alias("review_count"),
        )
        .withColumn("revenue_rank", F.row_number().over(product_window))
        .orderBy("revenue_rank")
    )

    sales_by_customer = (
        sales.groupBy(
            "customer_id",
            "customer_first_name",
            "customer_last_name",
            "customer_country",
        )
        .agg(
            F.round(F.sum("sale_total_price"), 2).alias("total_spent"),
            F.countDistinct("sale_id").alias("orders_count"),  # type: ignore
            F.round(F.avg("sale_total_price"), 2).alias("avg_check"),
        )
        .withColumn("country_customer_count", F.count("*").over(Window.partitionBy("customer_country")))
        .withColumn("spend_rank", F.row_number().over(customer_window))
        .orderBy("spend_rank")
    )

    sales_by_time = (
        sales.groupBy("year", "quarter", "month", "month_name")
        .agg(
            F.round(F.sum("sale_total_price"), 2).alias("total_revenue"),
            F.sum("sale_quantity").alias("items_sold"),
            F.countDistinct("sale_id").alias("orders_count"),  # type: ignore
            F.round(F.avg("sale_total_price"), 2).alias("avg_order_size"),
        )
        .orderBy("year", "month")
    )

    sales_by_store = (
        sales.groupBy("store_id", "store_name", "store_city", "store_country")
        .agg(
            F.round(F.sum("sale_total_price"), 2).alias("total_revenue"),
            F.countDistinct("sale_id").alias("orders_count"),  # type: ignore
            F.round(F.avg("sale_total_price"), 2).alias("avg_check"),
        )
        .withColumn("revenue_rank", F.row_number().over(store_window))
        .orderBy("revenue_rank")
    )

    sales_by_supplier = (
        sales.groupBy("supplier_id", "supplier_name", "supplier_country")
        .agg(
            F.round(F.sum("sale_total_price"), 2).alias("total_revenue"),
            F.round(F.avg("product_price"), 2).alias("avg_product_price"),
            F.sum("sale_quantity").alias("items_sold"),
        )
        .withColumn("revenue_rank", F.row_number().over(supplier_window))
        .orderBy("revenue_rank")
    )

    rating_sales_corr = (
        sales.groupBy("product_id")
        .agg(F.first("product_rating").alias("product_rating"), F.sum("sale_quantity").alias("units_sold"))
        .agg(F.round(F.corr("product_rating", "units_sold"), 4).alias("rating_sales_corr"))
        .collect()[0]["rating_sales_corr"]
    )

    product_quality_report = (
        sales.groupBy("product_id", "product_name", "product_category")
        .agg(
            F.round(F.avg("product_rating"), 2).alias("avg_rating"),
            F.max("product_reviews").alias("review_count"),
            F.sum("sale_quantity").alias("units_sold"),
            F.round(F.sum("sale_total_price"), 2).alias("total_revenue"),
        )
        .withColumn("rating_rank_desc", F.row_number().over(quality_desc_window))
        .withColumn("rating_rank_asc", F.row_number().over(quality_asc_window))
        .withColumn("review_rank", F.row_number().over(review_window))
        .withColumn("rating_sales_corr", F.lit(rating_sales_corr))
        .orderBy("rating_rank_desc")
    )

    write_clickhouse(
        sales_by_product,
        "sales_by_product",
        ch_jdbc_url,
        ch_settings.user,
        ch_settings.password,
    )
    write_clickhouse(
        sales_by_customer,
        "sales_by_customer",
        ch_jdbc_url,
        ch_settings.user,
        ch_settings.password,
    )
    write_clickhouse(
        sales_by_time,
        "sales_by_time",
        ch_jdbc_url,
        ch_settings.user,
        ch_settings.password,
    )
    write_clickhouse(
        sales_by_store,
        "sales_by_store",
        ch_jdbc_url,
        ch_settings.user,
        ch_settings.password,
    )
    write_clickhouse(
        sales_by_supplier,
        "sales_by_supplier",
        ch_jdbc_url,
        ch_settings.user,
        ch_settings.password,
    )
    write_clickhouse(
        product_quality_report,
        "product_quality_report",
        ch_jdbc_url,
        ch_settings.user,
        ch_settings.password,
    )

    spark.stop()


if __name__ == "__main__":
    main()
