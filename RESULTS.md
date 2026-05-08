# Результаты
> Были проблемы с чтением non-ASCII символов на windows поэтому переименовал папку с данными

## Конфигурация

- PostgreSQL: `localhost:5432`
- pgAdmin: `http://localhost:5433`
- ClickHouse HTTP: `http://localhost:8123`
- Tabix для ClickHouse: `http://localhost:8125`

Логины:

- PostgreSQL:
    username: `app`
    password: `app`
    db: `bigdata_lab`
- pgAdmin:
    login: `george.ermekov@gmail.com`
    password: `test`
- ClickHouse:
    login: `app`
    password: `app`

## Поднимаем всё

```bash
docker compose up -d --build
```

## Прогнать Spark jobs:
> У меня джупитер ноутбук через одно место работает с poetry, поэтому я сделал так через простые скрипты
> P.S. во всем виноват python environments, который решил сам включиться

- Из raw в dwh
```bash
docker compose exec -T spark /opt/spark/bin/spark-submit --master 'local[*]' /opt/project/src/spark_star_to_postgres.py
```

- Отчеты в clickhouse
```bash
docker compose exec -T spark /opt/spark/bin/spark-submit --master 'local[*]' /opt/project/src/spark_reports_to_clickhouse.py
```

## Скрипты

1. `load_raw_to_postgres.py` загружает все CSV в `raw.mock_data`.
2. `spark_star_to_postgres.py` собирает витрину-звезду в PostgreSQL в схеме `dwh`.
3. `spark_reports_to_clickhouse.py` считает 6 отчётных витрин и пишет их в ClickHouse в базу `reports`.

## Результат

Проверка PostgreSQL:

```sql
SELECT count(*) FROM raw.mock_data;
-- 10000

SELECT count(*) FROM dwh.fact_sales;
-- 10000

SELECT * FROM dwh.dim_products LIMIT 10;
-- 1	"5dfb8688ea110023a5daa40cab5ceccb390a8dc17ee62abe482786492a148eaf"	"Bird Cage"	"Food"	18.57	87	49.9 <....> "2022-09-29"	"2026-02-28"
-- 2	"7aa2aaa54f42d1026ce344debaa1236c932bbb86da668439b3ab685a8b132964"	"Dog Food"	"Cage"	67.72	88	44.5 <....> "2016-05-31"	"2029-09-11"
-- 3	"a48a9780ed3ea13f82d3f7337978b70d9c8a7bbbbb82bd7b3218456e7da78bf9"	"Dog Food"	"Food"	4.49	77	42.6 <....> "2012-12-14"	"2024-01-05"
-- 4	"2424a2017d368e40872d5482776dd1fb0b64f9840d6bdb9f2d5cca1c4be55c6b"	"Cat Toy"	"Cage"	28.41	91	5.5 <....> "2010-11-06"	"2025-06-03"
-- 5	"be74cc896860df3791c569c165f979bec3976fa0580b7dd58c91f374f671c57e"	"Bird Cage"	"Cage"	88.39	35	36.4 <....> "2018-11-02"	"2030-07-10"
-- 6	"824291d2e7d02893ce6bc3a54b31802edb4822d6764b091ef6f33d3ec8bb801a"	"Bird Cage"	"Food"	85.35	21	4.5 <....> "2022-07-20"	"2027-02-21"
-- 7	"f01d0304249795c301dc3e6e8cf5596097d96b5be9020b362f57300511175b3d"	"Cat Toy"	"Toy"	25.03	86	37.7 <....> "2020-07-03"	"2025-05-22"
-- 8	"48e833a4f9630f47bb1407ba95f6a24da0beeeb71e6da8d55974a3d2ad565b1e"	"Bird Cage"	"Food"	11.63	63	1.7 <....> "2020-01-08"	"2027-04-08"
-- 9	"636ecb7dd6382cda02ddd44d105f4ca147cd2f64e2a35a7f5cc325c5848fa97f"	"Bird Cage"	"Cage"	67.8	93	17 <....> "2011-06-10"	"2030-05-07"
-- 10	"81bb6bcc161b5ad58d23838e99ec7b8c1a2ae21a30f4902e67d84b19853e1341"	"Cat Toy"	"Cage"	86.21	45	31.3 <....> "2022-01-07"	"2025-02-07"
```

Проверка ClickHouse:

```sql
SELECT * FROM reports.sales_by_product LIMIT 3;
-- 	product_id	product_name	product_category	product_brand	total_units_sold	total_revenue	avg_rating	review_count	revenue_rank
-- 611	Bird Cage	Cage	Jayo	63	4005.98	2.7	482	1
-- 779	Cat Toy	Cage	Jayo	59	3784.44	2.8	855	2
-- 434	Bird Cage	Toy	Vinte	60	3751.09	5	678	3

SELECT * FROM reports.sales_by_customer LIMIT 3;
-- customer_id	customer_first_name	customer_last_name	customer_country	total_spent	orders_count	avg_check	country_customer_count	spend_rank
-- 611	Hannie	Braddon	China	4005.98	1	400.6	174	1
-- 779	Mercy	Antonomoli	Laos	3784.44	1	378.44	1	2
-- 434	Genni	Schultze	United States	3751.09	1	375.11	19	3

SELECT * FROM reports.sales_by_time LIMIT 3;
-- year	quarter	month	month_name	total_revenue	items_sold	orders_count	avg_order_size
-- 2021	1	1	January	224158.54	4856	601	256.47
-- 2021	1	2	February	192348.31	4070	523	260.28
-- 2021	1	3	March	207282.2	4561	582	245.89

SELECT * FROM reports.sales_by_store LIMIT 3;
-- store_id	store_name	store_city	store_country	total_revenue	orders_count	avg_check	revenue_rank
-- c15899c7c164c3ea265478e752fe1825cbb04cb976ced0b094cf730459677bfa	DabZ	Grekan	South Africa	499.85	1	499.85	1
-- b8343539b054887d0fbd9bc1f10a666293f26b571a11ae8db7d4119cf8fc7626	Thoughtblab	Fonte	Poland	499.8	1	499.8	2
-- 21074b50178814e7637819b688921a31829a2db60c96586c79034bbea6dbc3ef	Edgeblab	Pesek	Indonesia	499.76	1	499.76	3

SELECT * FROM reports.sales_by_supplier LIMIT 3;
-- supplier_id	supplier_name	supplier_country	total_revenue	avg_product_price	items_sold	revenue_rank
-- 7d9bd3e3a2275f4991c2ee979ccfad903c8cf2517bb454b34c83173dbeed0a39	Brainverse	Ireland	499.85	44.8	7	1
-- 693416d39a25844d0232a303aecbf50c0ffe10ea9cd0817b5dc93d1a9e261999	Jamia	Russia	499.8	98.99	9	2
-- 2ea5252b46b8e962175e2396cb06636b1b9d91dbf546c36b7181c56c2ca409ab	Eabox	Portugal	499.76	52.25	2	3

SELECT * FROM reports.product_quality_report LIMIT 3;
-- product_id	product_name	product_category	avg_rating	review_count	units_sold	total_revenue	rating_rank_desc	rating_rank_asc	review_rank	rating_sales_corr
-- 530	Dog Food	Cage	5	352	58	2659.48	1	987	655	-0.0458
-- 810	Cat Toy	Toy	5	141	49	2189.79	2	988	864	-0.0458
-- 524	Cat Toy	Toy	5	177	44	2569.42	3	989	832	-0.0458
```
