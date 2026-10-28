from pyspark import pipelines as dp
from pyspark.sql import functions as F


def bronze(table_name: str):
    return spark.read.table(
        f"inventra.inventra_bronze.{table_name}"
    )


# ============================================================
# COZINHAS
# ============================================================

@dp.materialized_view(
    name="inventra.inventra_silver.tb_kitchen"
)
@dp.expect("pk_cozinha_preenchida", "id_kitchen IS NOT NULL")
def silver_kitchen():
    return (
        bronze("tb_kitchen")
        .dropDuplicates(["id_kitchen"])
        .withColumn(
            "name",
            F.regexp_replace(
                F.trim(F.col("name")),
                r"^Cozinha Industrial\s*",
                ""
            )
        )
        .withColumn("code", F.trim(F.col("code")))
        .withColumn("address", F.trim(F.col("address")))
        .withColumn("active", F.col("active").cast("boolean"))
        .withColumn("created_at", F.to_timestamp("created_at"))
    )


# ============================================================
# PERFIS
# ============================================================

@dp.materialized_view(
    name="inventra.inventra_silver.tb_profile"
)
@dp.expect("pk_perfil_preenchida", "id_profile IS NOT NULL")
def silver_profile():
    return (
        bronze("tb_profile")
        .dropDuplicates(["id_profile"])
        .withColumn("access_type", F.trim("access_type"))
        .withColumn("description", F.trim("description"))
    )


# ============================================================
# CATEGORIAS
# ============================================================

@dp.materialized_view(
    name="inventra.inventra_silver.tb_category"
)
@dp.expect("pk_categoria_preenchida", "id_category IS NOT NULL")
def silver_category():
    return (
        bronze("tb_category")
        .dropDuplicates(["id_category"])
        .withColumn("name", F.trim("name"))
        .withColumn("description", F.trim("description"))
    )


# ============================================================
# UNIDADES
# ============================================================

@dp.materialized_view(
    name="inventra.inventra_silver.tb_measurement_unit"
)
@dp.expect("pk_unidade_preenchida", "id_unit IS NOT NULL")
def silver_measurement_unit():
    return (
        bronze("tb_measurement_unit")
        .dropDuplicates(["id_unit"])
        .withColumn("symbol", F.trim("symbol"))
        .withColumn("description", F.trim("description"))
    )


# ============================================================
# USUÁRIOS
# ============================================================

@dp.materialized_view(
    name="inventra.inventra_silver.tb_user"
)
@dp.expect("pk_usuario_preenchida", "id_user IS NOT NULL")
def silver_user():
    return (
        bronze("tb_user")
        .dropDuplicates(["id_user"])
        .withColumn("name", F.trim("name"))
        .withColumn("email", F.lower(F.trim("email")))
        .withColumn("active", F.col("active").cast("boolean"))
        .withColumn("last_login", F.to_timestamp("last_login"))
        .withColumn("created_at", F.to_timestamp("created_at"))
    )


# ============================================================
# FORNECEDORES
# ============================================================

@dp.materialized_view(
    name="inventra.inventra_silver.tb_supplier"
)
@dp.expect("pk_fornecedor_preenchida", "id_supplier IS NOT NULL")
def silver_supplier():
    return (
        bronze("tb_supplier")
        .dropDuplicates(["id_supplier"])
        .withColumn("legal_name", F.trim("legal_name"))
        .withColumn("cnpj", F.trim(F.col("cnpj").cast("string")))
        .withColumn("email", F.lower(F.trim("email")))
        .withColumn("whatsapp", F.trim(F.col("whatsapp").cast("string")))
        .withColumn("rating", F.col("rating").cast("double"))
        .withColumn("active", F.col("active").cast("boolean"))
        .withColumn("created_at", F.to_timestamp("created_at"))
    )


# ============================================================
# PRODUTOS
# ============================================================

@dp.materialized_view(
    name="inventra.inventra_silver.tb_product"
)
@dp.expect("pk_produto_preenchida", "id_product IS NOT NULL")
def silver_product():
    return (
        bronze("tb_product")
        .dropDuplicates(["id_product"])
        .withColumn("name", F.trim("name"))
        .withColumn("brand", F.trim("brand"))
        .withColumn(
            "barcode",
            F.trim(F.col("barcode").cast("string"))
        )
        .withColumn("photo_url", F.trim("photo_url"))
        .withColumn("active", F.col("active").cast("boolean"))
        .withColumn("created_at", F.to_timestamp("created_at"))
    )


# ============================================================
# PRODUTO X FORNECEDOR
# ============================================================

@dp.materialized_view(
    name="inventra.inventra_silver.tb_product_supplier"
)
def silver_product_supplier():
    return (
        bronze("tb_product_supplier")
        .dropDuplicates(["id_product", "id_supplier"])
        .withColumn(
            "key_produto_fornecedor",
            F.sha2(
                F.concat_ws(
                    "|",
                    F.col("id_product").cast("string"),
                    F.col("id_supplier").cast("string")
                ),
                256
            )
        )
        .withColumn(
            "supplier_code",
            F.trim(F.col("supplier_code").cast("string"))
        )
        .withColumn(
            "reference_price",
            F.col("reference_price").cast("decimal(18,2)")
        )
        .withColumn(
            "lead_time_days",
            F.col("lead_time_days").cast("int")
        )
    )


# ============================================================
# PRODUTO X COZINHA
# ============================================================

@dp.materialized_view(
    name="inventra.inventra_silver.tb_product_kitchen_parameter"
)
def silver_product_kitchen_parameter():
    return (
        bronze("tb_product_kitchen_parameter")
        .dropDuplicates(["id_product", "id_kitchen"])
        .withColumn(
            "key_produto_cozinha",
            F.sha2(
                F.concat_ws(
                    "|",
                    F.col("id_product").cast("string"),
                    F.col("id_kitchen").cast("string")
                ),
                256
            )
        )
        .withColumn("min_stock", F.col("min_stock").cast("double"))
        .withColumn("max_stock", F.col("max_stock").cast("double"))
        .withColumn(
            "average_daily_consumption",
            F.col("average_daily_consumption").cast("double")
        )
    )


# ============================================================
# LOTES
# ============================================================

@dp.materialized_view(
    name="inventra.inventra_silver.tb_stock_batch"
)
@dp.expect("pk_lote_preenchida", "id_batch IS NOT NULL")
def silver_stock_batch():
    return (
        bronze("tb_stock_batch")
        .dropDuplicates(["id_batch"])
        .withColumn(
            "key_produto_fornecedor",
            F.sha2(
                F.concat_ws(
                    "|",
                    F.col("id_product").cast("string"),
                    F.col("id_supplier").cast("string")
                ),
                256
            )
        )
        .withColumn(
            "key_produto_cozinha",
            F.sha2(
                F.concat_ws(
                    "|",
                    F.col("id_product").cast("string"),
                    F.col("id_kitchen").cast("string")
                ),
                256
            )
        )
        .withColumn("batch_number", F.trim("batch_number"))
        .withColumn("invoice_number", F.trim("invoice_number"))
        .withColumn(
            "initial_quantity",
            F.col("initial_quantity").cast("double")
        )
        .withColumn(
            "current_quantity",
            F.col("current_quantity").cast("double")
        )
        .withColumn("entry_date", F.to_date("entry_date"))
        .withColumn(
            "expiration_date",
            F.to_date("expiration_date")
        )
        .withColumn(
            "unit_price",
            F.col("unit_price").cast("decimal(18,2)")
        )
        .withColumn("status", F.trim("status"))
    )


# ============================================================
# REQUISIÇÕES
# ============================================================

@dp.materialized_view(
    name="inventra.inventra_silver.tb_requisition"
)
@dp.expect("pk_requisicao_preenchida", "id_requisition IS NOT NULL")
def silver_requisition():
    return (
        bronze("tb_requisition")
        .dropDuplicates(["id_requisition"])
        .withColumn("requisition_type", F.trim("requisition_type"))
        .withColumn("origin", F.trim("origin"))
        .withColumn("status", F.trim("status"))
        .withColumn("reason", F.trim("reason"))
        .withColumn("created_at", F.to_timestamp("created_at"))
        .withColumn("approved_at", F.to_timestamp("approved_at"))
    )


# ============================================================
# ITENS DAS REQUISIÇÕES
# ============================================================

@dp.materialized_view(
    name="inventra.inventra_silver.tb_requisition_item"
)
@dp.expect(
    "pk_item_requisicao_preenchida",
    "id_requisition_item IS NOT NULL"
)
def silver_requisition_item():
    return (
        bronze("tb_requisition_item")
        .dropDuplicates(["id_requisition_item"])
        .withColumn("quantity", F.col("quantity").cast("double"))
        .withColumn(
            "estimated_price",
            F.col("estimated_price").cast("decimal(18,2)")
        )
        .withColumn("note", F.trim("note"))
    )


# ============================================================
# INVENTÁRIOS
# ============================================================

@dp.materialized_view(
    name="inventra.inventra_silver.tb_inventory"
)
@dp.expect("pk_inventario_preenchida", "id_inventory IS NOT NULL")
def silver_inventory():
    return (
        bronze("tb_inventory")
        .dropDuplicates(["id_inventory"])
        .withColumn("started_at", F.to_timestamp("started_at"))
        .withColumn("closed_at", F.to_timestamp("closed_at"))
        .withColumn("status", F.trim("status"))
        .withColumn("note", F.trim("note"))
    )


# ============================================================
# CONTAGENS
# ============================================================

@dp.materialized_view(
    name="inventra.inventra_silver.tb_inventory_count"
)
@dp.expect(
    "pk_contagem_preenchida",
    "id_count IS NOT NULL"
)
def silver_inventory_count():
    return (
        bronze("tb_inventory_count")
        .dropDuplicates(["id_count"])
        .withColumn(
            "registered_quantity",
            F.col("registered_quantity").cast("double")
        )
        .withColumn(
            "physical_quantity",
            F.col("physical_quantity").cast("double")
        )
        .withColumn(
            "divergence",
            F.col("divergence").cast("double")
        )
        .withColumn("note", F.trim("note"))
    )


# ============================================================
# ALERTAS
# ============================================================

@dp.materialized_view(
    name="inventra.inventra_silver.tb_alert"
)
@dp.expect("pk_alerta_preenchida", "id_alert IS NOT NULL")
def silver_alert():
    return (
        bronze("tb_alert")
        .dropDuplicates(["id_alert"])
        .withColumn("type", F.trim("type"))
        .withColumn("severity", F.trim("severity"))
        .withColumn("message", F.trim("message"))
        .withColumn("is_read", F.col("is_read").cast("boolean"))
        .withColumn("created_at", F.to_timestamp("created_at"))
    )