from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config import get_settings


def ensure_postgres_schema(spark: SparkSession, jdbc_url: str, user: str, password: str) -> None:
    jvm = spark._sc._gateway.jvm
    connection = jvm.java.sql.DriverManager.getConnection(jdbc_url, user, password)
    statement = connection.createStatement()
    try:
        statement.execute("CREATE SCHEMA IF NOT EXISTS dwh")
    finally:
        statement.close()
        connection.close()


def not_blank(column_name: str):
    return F.when(F.trim(F.col(column_name)) == "", None).otherwise(F.col(column_name))


def as_int(column_name: str):
    return not_blank(column_name).cast("int")


def as_double(column_name: str):
    return not_blank(column_name).cast("double")


def as_date(column_name: str):
    return F.to_date(not_blank(column_name), "M/d/yyyy")


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("star-to-postgres")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.default.parallelism", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        .config("spark.driver.maxResultSize", "512m")
        .getOrCreate()
    )


def main() -> None:
    pg_settings = get_settings().postgres

    jdbc_url = pg_settings.jdbc_url
    jdbc_properties = pg_settings.jdbc_properties

    spark = build_spark()
    ensure_postgres_schema(spark, jdbc_url, pg_settings.user, pg_settings.password)

    raw_df = spark.read.jdbc(url=jdbc_url, table="raw.mock_data", properties=jdbc_properties)

    base_df = (
        raw_df.withColumn("id", as_int("id"))
        .withColumn("customer_age", as_int("customer_age"))
        .withColumn("product_price", as_double("product_price"))
        .withColumn("product_quantity", as_int("product_quantity"))
        .withColumn("sale_customer_id", as_int("sale_customer_id"))
        .withColumn("sale_seller_id", as_int("sale_seller_id"))
        .withColumn("sale_product_id", as_int("sale_product_id"))
        .withColumn("sale_quantity", as_int("sale_quantity"))
        .withColumn("sale_total_price", as_double("sale_total_price"))
        .withColumn("product_weight", as_double("product_weight"))
        .withColumn("product_rating", as_double("product_rating"))
        .withColumn("product_reviews", as_int("product_reviews"))
        .withColumn("sale_date_parsed", as_date("sale_date"))
        .withColumn("product_release_date_parsed", as_date("product_release_date"))
        .withColumn("product_expiry_date_parsed", as_date("product_expiry_date"))
        .withColumn(
            "supplier_id",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.coalesce(F.col("supplier_name"), F.lit("")),
                    F.coalesce(F.col("supplier_email"), F.lit("")),
                    F.coalesce(F.col("supplier_phone"), F.lit("")),
                ),
                256,
            ),
        )
        .withColumn(
            "store_id",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.coalesce(F.col("store_name"), F.lit("")),
                    F.coalesce(F.col("store_email"), F.lit("")),
                    F.coalesce(F.col("store_phone"), F.lit("")),
                ),
                256,
            ),
        )
        .withColumn("date_id", F.date_format(F.col("sale_date_parsed"), "yyyyMMdd").cast("int"))
    )

    dim_customers = (
        base_df.select(
            F.col("sale_customer_id").alias("customer_id"),
            "customer_first_name",
            "customer_last_name",
            "customer_age",
            "customer_email",
            "customer_country",
            "customer_postal_code",
            "customer_pet_type",
            "customer_pet_name",
            "customer_pet_breed",
            "pet_category",
        )
        .dropDuplicates(["customer_id"])
        .orderBy("customer_id")
    )

    dim_sellers = (
        base_df.select(
            F.col("sale_seller_id").alias("seller_id"),
            "seller_first_name",
            "seller_last_name",
            "seller_email",
            "seller_country",
            "seller_postal_code",
        )
        .dropDuplicates(["seller_id"])
        .orderBy("seller_id")
    )

    dim_suppliers = (
        base_df.select(
            "supplier_id",
            "supplier_name",
            "supplier_contact",
            "supplier_email",
            "supplier_phone",
            "supplier_address",
            "supplier_city",
            "supplier_country",
        )
        .dropDuplicates(["supplier_id"])
        .orderBy("supplier_name")
    )

    dim_stores = (
        base_df.select(
            "store_id",
            "store_name",
            "store_location",
            "store_city",
            "store_state",
            "store_country",
            "store_phone",
            "store_email",
        )
        .dropDuplicates(["store_id"])
        .orderBy("store_name")
    )

    dim_products = (
        base_df.select(
            F.col("sale_product_id").alias("product_id"),
            "supplier_id",
            "product_name",
            "product_category",
            "product_price",
            "product_quantity",
            "product_weight",
            "product_color",
            "product_size",
            "product_brand",
            "product_material",
            "product_description",
            "product_rating",
            "product_reviews",
            F.col("product_release_date_parsed").alias("product_release_date"),
            F.col("product_expiry_date_parsed").alias("product_expiry_date"),
        )
        .dropDuplicates(["product_id"])
        .orderBy("product_id")
    )

    fact_sales = (
        base_df.select(
            F.col("id").alias("sale_id"),
            F.col("sale_customer_id").alias("customer_id"),
            F.col("sale_seller_id").alias("seller_id"),
            F.col("sale_product_id").alias("product_id"),
            "supplier_id",
            "store_id",
            "date_id",
            "sale_quantity",
            "sale_total_price",
            "product_price",
            "product_quantity",
            "product_rating",
            "product_reviews",
        )
        .where(F.col("sale_id").isNotNull())
        .orderBy("sale_id")
    )

    tables = {
        "dwh.dim_customers": dim_customers,
        "dwh.dim_sellers": dim_sellers,
        "dwh.dim_suppliers": dim_suppliers,
        "dwh.dim_stores": dim_stores,
        "dwh.dim_products": dim_products,
        "dwh.fact_sales": fact_sales,
    }

    for table_name, dataframe in tables.items():
        dataframe.write.mode("overwrite").jdbc(jdbc_url, table_name, properties=jdbc_properties)

    spark.stop()


if __name__ == "__main__":
    main()
