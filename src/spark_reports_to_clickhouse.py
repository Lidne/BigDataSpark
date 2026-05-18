from __future__ import annotations

from urllib.parse import urlencode
from urllib.request import Request, urlopen

from pyspark.sql import DataFrame, SparkSession
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


def execute_clickhouse_query(host: str, port: int, user: str, password: str, query: str) -> None:
    encoded_query = urlencode({"query": query, "user": user, "password": password})
    request = Request(f"http://{host}:{port}/?{encoded_query}", data=b"", method="POST")
    with urlopen(request) as response:
        response.read()


def drop_clickhouse_tables(host: str, port: int, db: str, user: str, password: str, table_names: list[str]) -> None:
    for table_name in table_names:
        execute_clickhouse_query(host, port, user, password, f"DROP TABLE IF EXISTS {db}.{table_name}")


def clickhouse_table_options(table_name: str) -> str:
    order_by_map = {
        "product_top10_sales": "(total_units_sold, total_revenue, product_id)",
        "product_category_revenue": "(product_category)",
        "product_rating_reviews": "(product_id)",
        "customer_top10_spend": "(total_spent, customer_id)",
        "customer_country_distribution": "(customer_country)",
        "customer_avg_check": "(avg_check, customer_id)",
        "time_sales_trends": "(period_type, year, month)",
        "time_revenue_period_comparison": "(year, quarter)",
        "time_avg_order_size_by_month": "(year, month)",
        "store_top5_revenue": "(total_revenue, store_id)",
        "store_sales_distribution": "(store_country, store_city)",
        "store_avg_check": "(avg_check, store_id)",
        "supplier_top5_revenue": "(total_revenue, supplier_id)",
        "supplier_avg_product_price": "(avg_product_price, supplier_id)",
        "supplier_sales_by_country": "(supplier_country)",
        "quality_rating_extremes": "(rating_group, avg_rating, product_id)",
        "quality_rating_sales_correlation": "(rating_sales_corr)",
        "quality_most_reviewed_products": "(review_count, product_id)",
    }
    order_by = order_by_map.get(table_name, "tuple()")
    return f"ENGINE = MergeTree() ORDER BY {order_by}"


def write_clickhouse(dataframe: DataFrame, table_name: str, jdbc_url: str, user: str, password: str) -> None:
    (
        dataframe.write.format("jdbc")
        .mode("overwrite")
        .option("url", jdbc_url)
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .option("dbtable", table_name)
        .option("user", user)
        .option("password", password)
        .option("createTableOptions", clickhouse_table_options(table_name))
        .save()
    )


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("reports-to-clickhouse")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.default.parallelism", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        .config("spark.driver.maxResultSize", "512m")
        .getOrCreate()
    )


def build_sales_dataset(spark: SparkSession, pg_jdbc_url: str, pg_jdbc_properties: dict[str, str]) -> DataFrame:
    fact_sales = spark.read.jdbc(pg_jdbc_url, "dwh.fact_sales", properties=pg_jdbc_properties)
    dim_customers = spark.read.jdbc(pg_jdbc_url, "dwh.dim_customers", properties=pg_jdbc_properties)
    dim_products = spark.read.jdbc(pg_jdbc_url, "dwh.dim_products", properties=pg_jdbc_properties)
    dim_stores = spark.read.jdbc(pg_jdbc_url, "dwh.dim_stores", properties=pg_jdbc_properties)
    dim_suppliers = spark.read.jdbc(pg_jdbc_url, "dwh.dim_suppliers", properties=pg_jdbc_properties)

    sale_date = F.to_date(F.lpad(F.col("f.date_id").cast("string"), 8, "0"), "yyyyMMdd")

    return (
        fact_sales.alias("f")
        .join(dim_customers.alias("c"), F.col("f.customer_id") == F.col("c.customer_id"), "left")
        .join(dim_products.alias("p"), F.col("f.product_id") == F.col("p.product_id"), "left")
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
            F.col("st.store_name").alias("store_name"),
            F.col("st.store_city").alias("store_city"),
            F.col("st.store_country").alias("store_country"),
            F.col("sp.supplier_name").alias("supplier_name"),
            F.col("sp.supplier_country").alias("supplier_country"),
            sale_date.alias("sale_date"),
            F.year(sale_date).alias("year"),
            F.quarter(sale_date).alias("quarter"),
            F.month(sale_date).alias("month"),
            F.date_format(sale_date, "MMMM").alias("month_name"),
        )
    )


def build_report_tables(sales: DataFrame) -> dict[str, DataFrame]:
    product_metrics = sales.groupBy("product_id", "product_name", "product_category", "product_brand").agg(
        F.sum("sale_quantity").alias("total_units_sold"),
        F.round(F.sum("sale_total_price"), 2).alias("total_revenue"),
        F.round(F.avg("product_rating"), 2).alias("avg_rating"),
        F.max("product_reviews").alias("review_count"),
    )

    customer_spending = sales.groupBy(
        "customer_id", "customer_first_name", "customer_last_name", "customer_country"
    ).agg(
        F.round(F.sum("sale_total_price"), 2).alias("total_spent"),
        F.countDistinct("sale_id").alias("orders_count"),
    )

    customer_avg_check = (
        sales.groupBy("customer_id", "customer_first_name", "customer_last_name", "customer_country")
        .agg(
            F.round(F.avg("sale_total_price"), 2).alias("avg_check"),
            F.countDistinct("sale_id").alias("orders_count"),
        )
        .orderBy(F.desc("avg_check"), "customer_id")
    )

    monthly_sales = sales.groupBy("year", "month", "month_name").agg(
        F.round(F.sum("sale_total_price"), 2).alias("total_revenue"),
        F.sum("sale_quantity").alias("items_sold"),
        F.countDistinct("sale_id").alias("orders_count"),
    )

    yearly_sales = sales.groupBy("year").agg(
        F.round(F.sum("sale_total_price"), 2).alias("total_revenue"),
        F.sum("sale_quantity").alias("items_sold"),
        F.countDistinct("sale_id").alias("orders_count"),
    )

    product_quality_metrics = sales.groupBy("product_id", "product_name", "product_category", "product_brand").agg(
        F.round(F.avg("product_rating"), 2).alias("avg_rating"),
        F.max("product_reviews").alias("review_count"),
        F.sum("sale_quantity").alias("units_sold"),
        F.round(F.sum("sale_total_price"), 2).alias("total_revenue"),
    )

    highest_rated_products = (
        product_quality_metrics.orderBy(F.desc("avg_rating"), F.desc("review_count"), F.desc("total_revenue"))
        .limit(5)
        .withColumn("rating_group", F.lit("highest"))
    )
    lowest_rated_products = (
        product_quality_metrics.orderBy(F.asc("avg_rating"), F.desc("review_count"), F.desc("total_revenue"))
        .limit(5)
        .withColumn("rating_group", F.lit("lowest"))
    )

    report_tables = {
        "product_top10_sales": product_metrics.orderBy(F.desc("total_units_sold"), F.desc("total_revenue")).limit(10),
        "product_category_revenue": (
            sales.groupBy("product_category")
            .agg(
                F.round(F.sum("sale_total_price"), 2).alias("total_revenue"),
                F.sum("sale_quantity").alias("total_units_sold"),
            )
            .orderBy(F.desc("total_revenue"), "product_category")
        ),
        "product_rating_reviews": (
            product_metrics.select(
                "product_id",
                "product_name",
                "product_category",
                "product_brand",
                "avg_rating",
                "review_count",
            ).orderBy("product_id")
        ),
        "customer_top10_spend": customer_spending.orderBy(F.desc("total_spent"), "customer_id").limit(10),
        "customer_country_distribution": (
            sales.select("customer_id", "customer_country")
            .dropDuplicates(["customer_id"])
            .groupBy("customer_country")
            .agg(F.count("customer_id").alias("customers_count"))
            .orderBy(F.desc("customers_count"), "customer_country")
        ),
        "customer_avg_check": customer_avg_check,
        "time_sales_trends": (
            monthly_sales.withColumn("period_type", F.lit("month"))
            .select(
                "period_type",
                "year",
                "month",
                "month_name",
                "total_revenue",
                "items_sold",
                "orders_count",
            )
            .unionByName(
                yearly_sales.withColumn("period_type", F.lit("year"))
                .withColumn("month", F.lit(None).cast("int"))
                .withColumn("month_name", F.lit(None).cast("string"))
                .select(
                    "period_type",
                    "year",
                    "month",
                    "month_name",
                    "total_revenue",
                    "items_sold",
                    "orders_count",
                )
            )
            .orderBy("year", "period_type", "month")
        ),
        "time_revenue_period_comparison": (
            sales.groupBy("year", "quarter")
            .agg(
                F.round(F.sum("sale_total_price"), 2).alias("total_revenue"),
                F.sum("sale_quantity").alias("items_sold"),
                F.countDistinct("sale_id").alias("orders_count"),
            )
            .orderBy("year", "quarter")
        ),
        "time_avg_order_size_by_month": (
            sales.groupBy("year", "month", "month_name")
            .agg(
                F.round(F.avg("sale_total_price"), 2).alias("avg_order_size"),
                F.countDistinct("sale_id").alias("orders_count"),
            )
            .orderBy("year", "month")
        ),
        "store_top5_revenue": (
            sales.groupBy("store_id", "store_name", "store_city", "store_country")
            .agg(
                F.round(F.sum("sale_total_price"), 2).alias("total_revenue"),
                F.countDistinct("sale_id").alias("orders_count"),
            )
            .orderBy(F.desc("total_revenue"), "store_id")
            .limit(5)
        ),
        "store_sales_distribution": (
            sales.groupBy("store_country", "store_city")
            .agg(
                F.round(F.sum("sale_total_price"), 2).alias("total_revenue"),
                F.countDistinct("sale_id").alias("orders_count"),
                F.countDistinct("store_id").alias("stores_count"),
            )
            .orderBy(F.desc("total_revenue"), "store_country", "store_city")
        ),
        "store_avg_check": (
            sales.groupBy("store_id", "store_name", "store_city", "store_country")
            .agg(
                F.round(F.avg("sale_total_price"), 2).alias("avg_check"),
                F.countDistinct("sale_id").alias("orders_count"),
            )
            .orderBy(F.desc("avg_check"), "store_id")
        ),
        "supplier_top5_revenue": (
            sales.groupBy("supplier_id", "supplier_name", "supplier_country")
            .agg(
                F.round(F.sum("sale_total_price"), 2).alias("total_revenue"),
                F.sum("sale_quantity").alias("items_sold"),
            )
            .orderBy(F.desc("total_revenue"), "supplier_id")
            .limit(5)
        ),
        "supplier_avg_product_price": (
            sales.select("supplier_id", "supplier_name", "supplier_country", "product_id", "product_price")
            .dropDuplicates(["supplier_id", "product_id"])
            .groupBy("supplier_id", "supplier_name", "supplier_country")
            .agg(F.round(F.avg("product_price"), 2).alias("avg_product_price"))
            .orderBy(F.desc("avg_product_price"), "supplier_id")
        ),
        "supplier_sales_by_country": (
            sales.groupBy("supplier_country")
            .agg(
                F.round(F.sum("sale_total_price"), 2).alias("total_revenue"),
                F.sum("sale_quantity").alias("items_sold"),
                F.countDistinct("supplier_id").alias("suppliers_count"),
            )
            .orderBy(F.desc("total_revenue"), "supplier_country")
        ),
        "quality_rating_extremes": (
            highest_rated_products.unionByName(lowest_rated_products)
            .select(
                "rating_group",
                "product_id",
                "product_name",
                "product_category",
                "product_brand",
                "avg_rating",
                "review_count",
                "units_sold",
                "total_revenue",
            )
            .orderBy("rating_group", F.desc("avg_rating"), F.desc("review_count"))
        ),
        "quality_rating_sales_correlation": product_quality_metrics.agg(
            F.round(F.corr("avg_rating", "units_sold"), 4).alias("rating_sales_corr")
        ),
        "quality_most_reviewed_products": (
            product_quality_metrics.orderBy(
                F.desc("review_count"), F.desc("avg_rating"), F.desc("total_revenue")
            ).limit(10)
        ),
    }

    return report_tables


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

    spark = build_spark()
    sales = build_sales_dataset(spark, pg_jdbc_url, pg_jdbc_properties)
    report_tables = build_report_tables(sales)

    drop_clickhouse_tables(
        ch_settings.host,
        ch_settings.port,
        ch_settings.db,
        ch_settings.user,
        ch_settings.password,
        list(report_tables.keys()),
    )

    for table_name, dataframe in report_tables.items():
        write_clickhouse(dataframe, table_name, ch_jdbc_url, ch_settings.user, ch_settings.password)

    spark.stop()


if __name__ == "__main__":
    main()
