# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a25f22fa-6b5a-4f10-8a8d-996376d09810",
# META       "default_lakehouse_name": "BUSpendLakehouse",
# META       "default_lakehouse_workspace_id": "86bfda8c-0fef-41f1-8843-aee882e28b03",
# META       "known_lakehouses": [
# META         {
# META           "id": "a25f22fa-6b5a-4f10-8a8d-996376d09810"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import *

# 1. Business units
data_business_unit = [
    (1, "Retail Banking", "Manages consumer-facing products"),
    (2, "Corporate Banking", "Serves large corporate clients"),
    (3, "Investment Banking", "Capital markets and advisory"),
    (4, "Technology & Operations", "Internal tech and ops"),
]

schema_business_unit = StructType([
    StructField("BusinessUnitKey", IntegerType(), False),
    StructField("BusinessUnitName", StringType(), False),
    StructField("BusinessUnitDescription", StringType(), True),
])

dimBusinessUnit = spark.createDataFrame(data_business_unit, schema_business_unit)

# 2. Departments
# Link each department to a business unit

data_department = [
    (10, 1, "Digital Channels", "Retail digital banking"),
    (11, 1, "Cards", "Credit and debit card products"),
    (12, 1, "Mortgages", "Home lending"),
    (20, 2, "Transaction Banking", "Cash management & payments"),
    (21, 2, "Trade Finance", "Trade related services"),
    (30, 3, "Equities", "Equity trading"),
    (31, 3, "Fixed Income", "FI trading"),
    (40, 4, "Cloud Platform", "Cloud infrastructure team"),
    (41, 4, "Data & Analytics", "Data platform and analytics"),
]

schema_department = StructType([
    StructField("DepartmentKey", IntegerType(), False),
    StructField("BusinessUnitKey", IntegerType(), False),
    StructField("DepartmentName", StringType(), False),
    StructField("DepartmentDescription", StringType(), True),
])

dimDepartment = spark.createDataFrame(data_department, schema_department)

# 3. Cost centers (granularity for budgeting and chargeback)

data_cost_center = [
    (1001, 10, "RB-DIG-ENG-01", "Digital Engineering Squad 1"),
    (1002, 10, "RB-DIG-ENG-02", "Digital Engineering Squad 2"),
    (1003, 11, "RB-CARD-OPS-01", "Card Operations"),
    (1004, 11, "RB-CARD-ANL-01", "Card Analytics"),
    (2001, 20, "CB-TX-PLT-01", "Tx Banking Platform"),
    (2002, 21, "CB-TRD-OPS-01", "Trade Ops"),
    (3001, 30, "IB-EQ-TRD-01", "Equities Trading"),
    (3002, 31, "IB-FI-TRD-01", "FI Trading"),
    (4001, 40, "TO-CLD-PLT-01", "Cloud Platform Core"),
    (4002, 40, "TO-CLD-SRE-01", "Cloud SRE"),
    (4101, 41, "TO-DATA-ENG-01", "Data Engineering"),
    (4102, 41, "TO-ANL-01", "Analytics CoE"),
]

schema_cost_center = StructType([
    StructField("CostCenterKey", IntegerType(), False),
    StructField("DepartmentKey", IntegerType(), False),
    StructField("CostCenterCode", StringType(), False),
    StructField("CostCenterName", StringType(), False),
])

dimCostCenter = spark.createDataFrame(data_cost_center, schema_cost_center)

# 3b. Chargeback owners (e.g., product owners / BU finance leads)

data_chargeback_owner = [
    (1, "Alice Smith", "Retail Banking", "Head of Retail Technology"),
    (2, "Bob Johnson", "Corporate Banking", "Head of Corporate Platforms"),
    (3, "Carol Lee", "Investment Banking", "IB Tech Lead"),
    (4, "David Brown", "Technology & Operations", "CIO"),
]

schema_chargeback_owner = StructType([
    StructField("ChargebackOwnerKey", IntegerType(), False),
    StructField("ChargebackOwnerName", StringType(), False),
    StructField("OwnerBusinessUnit", StringType(), False),
    StructField("OwnerRole", StringType(), True),
])

dimChargebackOwner = spark.createDataFrame(data_chargeback_owner, schema_chargeback_owner)

# 3c. Projects / initiatives that incur cloud spend

data_project = [
    (101, 1, 1001, "RB Mobile App Modernisation", "Modernise retail mobile channels"),
    (102, 1, 1002, "RB Personalisation Engine", "Real-time offers and next-best action"),
    (201, 2, 2001, "CB Payments Hub", "Next-gen transaction banking platform"),
    (202, 2, 2002, "CB Trade Digitisation", "Digitise trade finance documentation"),
    (301, 3, 3001, "IB Equities Analytics", "Advanced analytics for equities"),
    (302, 3, 3002, "IB FI Risk Analytics", "Risk analytics for fixed income"),
    (401, 4, 4001, "Cloud Foundation", "Shared cloud landing zone"),
    (402, 4, 4002, "Observability Platform", "Central monitoring and logging"),
    (403, 4, 4101, "Data Platform", "Enterprise data platform"),
    (404, 4, 4102, "Analytics CoE", "Centre of Excellence for analytics"),
]

schema_project = StructType([
    StructField("ProjectKey", IntegerType(), False),
    StructField("ChargebackOwnerKey", IntegerType(), False),
    StructField("PrimaryCostCenterKey", IntegerType(), False),
    StructField("ProjectName", StringType(), False),
    StructField("ProjectDescription", StringType(), True),
])

dimProject = spark.createDataFrame(data_project, schema_project)

# 4. Cloud providers

data_cloud_provider = [
    (1, "Azure", "Microsoft Azure"),
    (2, "AWS", "Amazon Web Services"),
    (3, "GCP", "Google Cloud Platform"),
]

schema_cloud_provider = StructType([
    StructField("CloudProviderKey", IntegerType(), False),
    StructField("CloudProviderName", StringType(), False),
    StructField("CloudProviderDescription", StringType(), True),
])

dimCloudProvider = spark.createDataFrame(data_cloud_provider, schema_cloud_provider)

# 5. Cloud accounts/subscriptions (child of provider and optionally department)

data_cloud_account = [
    (101, 1, 40, "rb-core-azure-sub", "Subscription for core retail workloads"),
    (102, 1, 41, "rb-data-analytics-sub", "Subscription for retail analytics"),
    (201, 2, 20, "cb-aws-transact", "AWS for transaction banking"),
    (202, 2, 21, "cb-aws-trade", "AWS for trade finance"),
    (301, 3, 30, "ib-gcp-eq", "GCP for equities analytics"),
    (302, 3, 31, "ib-gcp-fi", "GCP for FI analytics"),
]

schema_cloud_account = StructType([
    StructField("CloudAccountKey", IntegerType(), False),
    StructField("CloudProviderKey", IntegerType(), False),
    StructField("OwningDepartmentKey", IntegerType(), False),
    StructField("AccountIdentifier", StringType(), False),
    StructField("AccountDescription", StringType(), True),
])

dimCloudAccount = spark.createDataFrame(data_cloud_account, schema_cloud_account)

# 6. Date dimension (monthly granularity for 2 years)

date_rows = []
start_date = F.to_date(F.lit("2023-01-01"))

# Use a range and add months in Spark to avoid Python loops for large dates, but here we'll 
# materialize small set (24 months) via Python for simplicity.
import datetime as dt

current = dt.date(2023, 1, 1)
for i in range(0, 24):
    d = current + dt.timedelta(days=30 * i)  # approx month; fine for sample
    first_of_month = dt.date(d.year, d.month, 1)
    date_rows.append(
        (
            i + 1,
            first_of_month,
            d.year,
            d.month,
            f"{d.year}-{d.month:02d}",
        )
    )

schema_date = StructType([
    StructField("DateKey", IntegerType(), False),
    StructField("Date", DateType(), False),
    StructField("Year", IntegerType(), False),
    StructField("Month", IntegerType(), False),
    StructField("YearMonth", StringType(), False),
])

from pyspark.sql import Row

dimDate = spark.createDataFrame(date_rows, schema_date)

# 7. Cloud service dimension (to track different cost buckets)

data_cloud_service = [
    (1, "Compute", "VMs, container services, serverless compute"),
    (2, "Storage", "Blob, file, disk storage"),
    (3, "Database", "Managed SQL/NoSQL"),
    (4, "Networking", "Bandwidth, load balancers"),
    (5, "Security", "Security center, key vault"),
    (6, "Management", "Monitoring, logging, management tools"),
]

schema_cloud_service = StructType([
    StructField("CloudServiceKey", IntegerType(), False),
    StructField("CloudServiceName", StringType(), False),
    StructField("CloudServiceDescription", StringType(), True),
])

dimCloudService = spark.createDataFrame(data_cloud_service, schema_cloud_service)

# 8. Spend fact table
# We'll generate synthetic monthly spend for each (CostCenter, CloudAccount, CloudService, Month)
# and also assign the spend to a project and chargeback owner

import random

random.seed(42)

fact_rows = []

# join helpers as in-memory dicts for nicer generation (not required for model)
cost_centers = [(r.CostCenterKey, r.DepartmentKey) for r in dimCostCenter.collect()]
cloud_accounts = [(r.CloudAccountKey, r.OwningDepartmentKey) for r in dimCloudAccount.collect()]
cloud_services = [r.CloudServiceKey for r in dimCloudService.collect()]
dates = [
    (r.DateKey, r.Year, r.Month)
    for r in dimDate.orderBy("Date").collect()
]

# map cost centers to projects (many spends per project, but one primary cost center per project)
project_df = dimProject.select("ProjectKey", "ChargebackOwnerKey", "PrimaryCostCenterKey").collect()
projects_by_cc = {}
for row in project_df:
    projects_by_cc.setdefault(row["PrimaryCostCenterKey"], []).append(
        (row["ProjectKey"], row["ChargebackOwnerKey"])
    )

for (cc_key, dept_key) in cost_centers:
    # pick a subset of accounts associated with the same or related departments
    related_accounts = [a for a in cloud_accounts if a[1] == dept_key]
    if not related_accounts:
        # fallback to any account
        related_accounts = cloud_accounts

    # candidate projects for this cost center
    cc_projects = projects_by_cc.get(cc_key)
    if not cc_projects:
        # if no explicit project, just map to any project (for completeness)
        cc_projects = [(row["ProjectKey"], row["ChargebackOwnerKey"]) for row in project_df]

    for (date_key, year, month) in dates:
        # concentrate spend in more recent months
        month_factor = 1 + (date_key / len(dates)) * 0.5
        for (account_key, owning_dept) in random.sample(related_accounts, min(len(related_accounts), 2)):
            for service_key in random.sample(cloud_services, 3):  # 3 services per month
                base = random.uniform(500, 5000)
                # Add some variation by department and service
                dept_factor = 1 + (dept_key % 5) * 0.1
                service_factor = 1 + (service_key % 3) * 0.05

                # pick a project & owner for this spend line
                project_key, owner_key = random.choice(cc_projects)

                amount = round(base * month_factor * dept_factor * service_factor, 2)

                fact_rows.append(
                    (
                        cc_key,
                        account_key,
                        service_key,
                        date_key,
                        project_key,
                        owner_key,
                        float(amount),
                    )
                )

schema_fact_spend = StructType([
    StructField("CostCenterKey", IntegerType(), False),
    StructField("CloudAccountKey", IntegerType(), False),
    StructField("CloudServiceKey", IntegerType(), False),
    StructField("DateKey", IntegerType(), False),
    StructField("ProjectKey", IntegerType(), False),
    StructField("ChargebackOwnerKey", IntegerType(), False),
    StructField("SpendAmount", DoubleType(), False),
])

factCloudSpend = spark.createDataFrame(fact_rows, schema_fact_spend)

# Optional: add surrogate key for fact
factCloudSpend = factCloudSpend.withColumn("FactCloudSpendKey", F.monotonically_increasing_id())

# 9. Write to Lakehouse as Delta tables (schema-enabled: use dbo schema)

# If your lakehouse is not schema-enabled, you can drop `dbo.` prefix in the table names

dimBusinessUnit.write.mode("overwrite").saveAsTable("dbo.DimBusinessUnit")
dimDepartment.write.mode("overwrite").saveAsTable("dbo.DimDepartment")
dimCostCenter.write.mode("overwrite").saveAsTable("dbo.DimCostCenter")
dimChargebackOwner.write.mode("overwrite").saveAsTable("dbo.DimChargebackOwner")
dimProject.write.mode("overwrite").saveAsTable("dbo.DimProject")
dimCloudProvider.write.mode("overwrite").saveAsTable("dbo.DimCloudProvider")
dimCloudAccount.write.mode("overwrite").saveAsTable("dbo.DimCloudAccount")
dimDate.write.mode("overwrite").saveAsTable("dbo.DimDate")
dimCloudService.write.mode("overwrite").saveAsTable("dbo.DimCloudService")
(factCloudSpend
    .write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("dbo.FactCloudSpend")
)

print("Sample dimension and fact tables created in Lakehouse:")
print(" - dbo.DimBusinessUnit")
print(" - dbo.DimDepartment")
print(" - dbo.DimCostCenter")
print(" - dbo.DimChargebackOwner")
print(" - dbo.DimProject")
print(" - dbo.DimCloudProvider")
print(" - dbo.DimCloudAccount")
print(" - dbo.DimDate")
print(" - dbo.DimCloudService")
print(" - dbo.FactCloudSpend")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
