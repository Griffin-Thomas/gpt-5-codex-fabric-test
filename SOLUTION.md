Architecture (ASCII diagram)
---------------------------------
```
             +------------------------+
             |   OneLake / Files      |
             |  input/orders.csv      |
             |  input/shipments.csv   |
             |  input/carriers.csv    |
             +-----------+------------+
                         |
                         v (Copy Activity)
                +---------------------+
                | Pipeline Activity 1 |
                |  Land to Bronze     |
                +----------+----------+
                           |
                   Delta Bronze Tables
                           |
                           v (Notebook: PySpark)
                +---------------------+
                | Pipeline Activity 2 |
                |  Bronze -> Silver   |
                +----------+----------+
                           |
                   Delta Silver Tables
                           |
                           v (Notebook: PySpark ML)
                +---------------------+
                | Pipeline Activity 3 |
                |  Silver -> Gold     |
                +----------+----------+
                           |
                   Delta Gold Tables & ML Scores
                           |
                +----------+----------+
                | Pipeline Activity 4 |
                |  Publish to WH/BI   |
                +----------+----------+
                           |
                   Fabric Warehouse / Power BI
```

Pipeline Steps (numbered)
-------------------------
1. **Ingest CSVs to Bronze:** Pipeline copy activity loads `Files/input/*.csv` into Lakehouse Bronze Delta tables (`bronze.orders`, `bronze.shipments`, `bronze.carriers`). Schema-on-read with minimal transformations.
2. **Transform to Silver:** PySpark notebook cleans data: cast types, deduplicate `orders`/`shipments`, filter null `order_id`, enrich with basic quality checks. Outputs `silver.orders_clean`, `silver.shipments_clean`, `silver.carriers_ref` Delta tables.
3. **Engineer Features & Train Model:** Notebook step calculates features (ship/promise intervals, carrier OTP, weekdays). Train logistic regression on historical labels, log model metadata in Lakehouse. Persist scaler & coefficients to `ml.models.shipment_lr`.
4. **Batch Score & Publish Gold:** Daily notebook loads latest Silver tables, applies trained model to predict risk; writes `gold.shipment_risk` with columns `[order_id, risk_score, predicted_late_flag, top_factors, as_of_date]`. Also maintain `gold.fact_shipments` and dimension tables.
5. **Warehouse Publish:** SQL script (activity) creates or refreshes views/tables in Fabric Warehouse pointing to Gold Delta tables via shortcuts or COPY INTO, enabling Power BI consumption.
6. **Report Refresh:** Power BI dataset scheduled post-pipeline run; dataset uses Warehouse connection with row-level security.

Core Code (PySpark + SQL + 2–3 DAX measures)
---------------------------------------------
```python
# PySpark Bronze -> Silver
from pyspark.sql import functions as F
from pyspark.sql.window import Window

orders = spark.read.format("delta").table("lakehouse.bronze.orders")
shipments = spark.read.format("delta").table("lakehouse.bronze.shipments")
carriers = spark.read.format("delta").table("lakehouse.bronze.carriers")

orders_clean = (
    orders.dropDuplicates(["order_id"]) 
          .withColumn("order_date", F.to_date("order_date"))
          .withColumn("promised_ship_date", F.to_date("promised_ship_date"))
          .filter(F.col("order_id").isNotNull())
)

shipments_clean = (
    shipments.dropDuplicates(["shipment_id"]) 
             .withColumn("ship_date", F.to_date("ship_date"))
             .withColumn("delivery_date", F.to_date("delivery_date"))
             .filter(F.col("order_id").isNotNull())
)

orders_clean.write.mode("overwrite").format("delta").saveAsTable("lakehouse.silver.orders_clean")
shipments_clean.write.mode("overwrite").format("delta").saveAsTable("lakehouse.silver.shipments_clean")
carriers.write.mode("overwrite").format("delta").saveAsTable("lakehouse.silver.carriers_ref")
```

```python
# Feature engineering & scoring in Gold
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline

silver_orders = spark.table("lakehouse.silver.orders_clean")
silver_shipments = spark.table("lakehouse.silver.shipments_clean")
carriers_ref = spark.table("lakehouse.silver.carriers_ref")

fact = (silver_orders.alias("o")
    .join(silver_shipments.alias("s"), "order_id", "left")
    .join(carriers_ref.alias("c"), "carrier", "left")
    .withColumn("days_to_ship", F.datediff("s.ship_date", "o.order_date"))
    .withColumn("days_delivered", F.datediff("s.delivery_date", "o.order_date"))
    .withColumn("days_promised", F.datediff("o.promised_ship_date", "o.order_date"))
    .withColumn("late_flag", (F.col("s.delivery_date") > F.col("o.promised_ship_date")).cast("int"))
    .withColumn("ship_weekday", F.date_format("s.ship_date", "E"))
)

string_indexer = StringIndexer(inputCol="region", outputCol="region_idx", handleInvalid="keep")
carrier_indexer = StringIndexer(inputCol="carrier", outputCol="carrier_idx", handleInvalid="keep")
weekday_indexer = StringIndexer(inputCol="ship_weekday", outputCol="weekday_idx", handleInvalid="keep")
one_hot = OneHotEncoder(inputCols=["region_idx", "carrier_idx", "weekday_idx"],
                       outputCols=["region_vec", "carrier_vec", "weekday_vec"])
assembler = VectorAssembler(inputCols=["days_to_ship", "days_promised", "carrier_otp_rate_90d",
                                       "base_transit_days", "region_vec", "carrier_vec", "weekday_vec"],
                            outputCol="features")
logreg = LogisticRegression(featuresCol="features", labelCol="late_flag", probabilityCol="risk_score")
ml_pipeline = Pipeline(stages=[string_indexer, carrier_indexer, weekday_indexer, one_hot, assembler, logreg])
model = ml_pipeline.fit(fact.na.fill({"days_to_ship":0,"days_promised":0,"carrier_otp_rate_90d":0,"base_transit_days":0}))

scored = model.transform(fact).select(
    "order_id",
    F.col("risk_score").getItem(1).alias("risk_score"),
    (F.col("risk_score").getItem(1) >= F.lit(0.5)).cast("int").alias("predicted_late_flag"),
    F.when(F.col("late_flag") == 1, F.lit("Delivery beyond promise"))
     .otherwise(F.lit("On time"))
     .alias("top_factors"),
    F.current_date().alias("as_of_date")
)

scored.write.mode("overwrite").format("delta").saveAsTable("lakehouse.gold.shipment_risk")
```

```sql
-- Warehouse publish (SQL)
CREATE OR ALTER VIEW dbo.vw_shipment_risk AS
SELECT order_id, risk_score, predicted_late_flag, top_factors, as_of_date
FROM gold.shipment_risk;

CREATE OR ALTER VIEW dbo.vw_shipments_summary AS
SELECT o.order_id,
       o.customer_id,
       o.region,
       s.ship_date,
       s.delivery_date,
       g.risk_score,
       g.predicted_late_flag
FROM gold.fact_shipments g
JOIN silver.orders_clean o ON g.order_id = o.order_id
JOIN silver.shipments_clean s ON g.order_id = s.order_id;
```

```DAX
Late Orders % = 
DIVIDE(
    CALCULATE(COUNTROWS('Shipment Risk'), 'Shipment Risk'[predicted_late_flag] = 1),
    COUNTROWS('Shipment Risk')
)

Avg Risk by Carrier = 
AVERAGEX(VALUES('Shipment Risk'[carrier]), CALCULATE(AVERAGE('Shipment Risk'[risk_score])))

Late Orders Delta vs Promise = 
VAR LateOrders = CALCULATE(COUNTROWS('Shipment Risk'), 'Shipment Risk'[predicted_late_flag] = 1)
VAR TotalPromise = CALCULATE(COUNTROWS('Orders'))
RETURN DIVIDE(LateOrders, TotalPromise)
```

Ops & Governance
----------------
- **Scheduling:** Pipeline scheduled daily at 02:00 local time, after warehouse close. Notebook activities configured with retry policies and timeout alerts.
- **Monitoring:** Use Data Factory-style pipeline run metrics plus Lakehouse table row-count checks (`spark.table(...).count()`) and Great Expectations-like assertions (simple PySpark checks for null `order_id`, negative intervals). Store metrics in `gold.pipeline_audit` table.
- **Alerting:** Configure Fabric pipeline to push alerts to Teams/webhook on failures or row-count deviations beyond ±5%.
- **RLS:** Implement Power BI row-level security role `RegionManager` using DAX filter `[region] = USERPRINCIPALNAME()` mapping via user-region bridge table stored in Warehouse.
- **Data retention:** Bronze retains 30 days of raw files; Silver/Gold maintain SCD2 dims with partition pruning. Apply auto-optimization/compaction in Lakehouse.

Validation Plan
---------------
- **Late flag validation query:**
  ```sql
  SELECT order_id, delivery_date, promised_ship_date, late_flag
  FROM gold.fact_shipments
  WHERE (delivery_date > promised_ship_date) <> (late_flag = 1);
  ```
  Expect zero rows.
- **Silver vs Gold reconciliation:**
  ```sql
  SELECT COUNT(*) AS silver_orders, 
         (SELECT COUNT(*) FROM gold.fact_shipments) AS gold_shipments
  FROM silver.orders_clean;
  ```
  Investigate if counts diverge > 0.

Assumptions & Trade-offs
------------------------
- Historical labels available within Silver to train logistic regression; limited dataset but suffices for baseline risk scoring.
- Chosen Fabric Warehouse for reporting due to better Power BI integration, governance, and SQL capabilities compared to Lakehouse endpoint.
- Logistic regression favored for interpretability; future iterations could evaluate gradient boosting using SynapseML if more data.
- Risk scores refreshed daily; real-time scoring out of scope given batch constraints and available tooling.
- `top_factors` placeholder text; future improvement would use coefficient-based feature importance mapping.
