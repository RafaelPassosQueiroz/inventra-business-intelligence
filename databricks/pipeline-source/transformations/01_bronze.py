from pyspark import pipelines as dp


# ============================================================
# CONFIGURAÇÃO DA FONTE POSTGRESQL
# ============================================================

SCOPE = "inventra-secrets"

DB_HOST = dbutils.secrets.get(scope=SCOPE, key="db-host")
DB_PORT = dbutils.secrets.get(scope=SCOPE, key="db-port")
DB_NAME = dbutils.secrets.get(scope=SCOPE, key="db-name")
DB_USER = dbutils.secrets.get(scope=SCOPE, key="db-user")
DB_PASSWORD = dbutils.secrets.get(scope=SCOPE, key="db-password")


def read_postgres(table_name: str):
    return (
        spark.read
        .format("postgresql")
        .option("host", DB_HOST)
        .option("port", DB_PORT)
        .option("database", DB_NAME)
        .option("user", DB_USER)
        .option("password", DB_PASSWORD)
        .option("sslmode", "require")
        .option("dbtable", f"public.{table_name}")
        .load()
    )


# ============================================================
# CADASTROS
# ============================================================

@dp.materialized_view(
    name="inventra.inventra_bronze.tb_kitchen",
    comment="Bronze da tabela de cozinhas do Inventra."
)
def bronze_kitchen():
    return read_postgres("tb_kitchen")


@dp.materialized_view(
    name="inventra.inventra_bronze.tb_profile",
    comment="Bronze da tabela de perfis."
)
def bronze_profile():
    return read_postgres("tb_profile")


@dp.materialized_view(
    name="inventra.inventra_bronze.tb_category",
    comment="Bronze da tabela de categorias."
)
def bronze_category():
    return read_postgres("tb_category")


@dp.materialized_view(
    name="inventra.inventra_bronze.tb_measurement_unit",
    comment="Bronze das unidades de medida."
)
def bronze_measurement_unit():
    return read_postgres("tb_measurement_unit")


@dp.materialized_view(
    name="inventra.inventra_bronze.tb_user",
    comment="Bronze dos usuários."
)
def bronze_user():
    return read_postgres("tb_user")


@dp.materialized_view(
    name="inventra.inventra_bronze.tb_supplier",
    comment="Bronze dos fornecedores."
)
def bronze_supplier():
    return read_postgres("tb_supplier")


@dp.materialized_view(
    name="inventra.inventra_bronze.tb_product",
    comment="Bronze dos produtos."
)
def bronze_product():
    return read_postgres("tb_product")


# ============================================================
# RELACIONAMENTOS
# ============================================================

@dp.materialized_view(
    name="inventra.inventra_bronze.tb_product_supplier"
)
def bronze_product_supplier():
    return read_postgres("tb_product_supplier")


@dp.materialized_view(
    name="inventra.inventra_bronze.tb_product_kitchen_parameter"
)
def bronze_product_kitchen_parameter():
    return read_postgres("tb_product_kitchen_parameter")


# ============================================================
# OPERAÇÃO
# ============================================================

@dp.materialized_view(
    name="inventra.inventra_bronze.tb_stock_batch"
)
def bronze_stock_batch():
    return read_postgres("tb_stock_batch")


@dp.materialized_view(
    name="inventra.inventra_bronze.tb_requisition"
)
def bronze_requisition():
    return read_postgres("tb_requisition")


@dp.materialized_view(
    name="inventra.inventra_bronze.tb_requisition_item"
)
def bronze_requisition_item():
    return read_postgres("tb_requisition_item")


@dp.materialized_view(
    name="inventra.inventra_bronze.tb_inventory"
)
def bronze_inventory():
    return read_postgres("tb_inventory")


@dp.materialized_view(
    name="inventra.inventra_bronze.tb_inventory_count"
)
def bronze_inventory_count():
    return read_postgres("tb_inventory_count")


@dp.materialized_view(
    name="inventra.inventra_bronze.tb_alert"
)
def bronze_alert():
    return read_postgres("tb_alert")