from pyspark import pipelines as dp
from pyspark.sql import functions as F


def silver(table_name: str):
    return spark.read.table(
        f"inventra.inventra_silver.{table_name}"
    )


# ============================================================
# COZINHAS
# ============================================================

@dp.materialized_view(
    name="inventra.inventra_gold.gld_cozinhas"
)
def gold_cozinhas():
    return silver("tb_kitchen").select(
        F.col("id_kitchen").alias("nCdCozinha"),
        F.col("name").alias("cNmCozinha"),
        F.col("code").alias("nCdCozinhaOperacional"),
        F.col("address").alias("cEnderecoCozinha"),
        F.col("active").alias("cFlgCozinhaAtiva"),
        F.to_date("created_at").alias("dDataCriacaoCozinha")
    )


# ============================================================
# PERFIS
# ============================================================

@dp.materialized_view(
    name="inventra.inventra_gold.gld_perfis"
)
def gold_perfis():
    return silver("tb_profile").select(
        F.col("id_profile").alias("nCdPerfil"),
        F.col("access_type").alias("cTipoAcesso"),
        F.col("description").alias("cDescricaoPerfil")
    )


# ============================================================
# CATEGORIAS
# ============================================================

@dp.materialized_view(
    name="inventra.inventra_gold.gld_categorias"
)
def gold_categorias():
    return silver("tb_category").select(
        F.col("id_category").alias("nCdCategoria"),
        F.col("name").alias("cNmCategoria"),
        F.col("description").alias("cDescricaoCategoria")
    )


# ============================================================
# UNIDADES
# ============================================================

@dp.materialized_view(
    name="inventra.inventra_gold.gld_unidades_medida"
)
def gold_unidades():
    return silver("tb_measurement_unit").select(
        F.col("id_unit").alias("nCdUnidade"),
        F.col("symbol").alias("cNmSimboloUnidade"),
        F.col("description").alias("cDescricaoUnidade")
    )


# ============================================================
# USUÁRIOS
# ============================================================

@dp.materialized_view(
    name="inventra.inventra_gold.gld_usuarios"
)
def gold_usuarios():
    return silver("tb_user").select(
        F.col("id_user").alias("nCdUsuario"),
        F.col("name").alias("cNmUsuario"),
        F.col("email").alias("cEmailUsuario"),
        F.col("id_kitchen").alias("nCdCozinha"),
        F.col("id_profile").alias("nCdPerfil"),
        F.col("active").alias("cFlgUsuarioAtivo"),
        F.col("last_login").alias("dDataUltimoLogin"),
        F.col("created_at").alias("dDataCriacaoUsuario")
    )


# ============================================================
# FORNECEDORES
# ============================================================

@dp.materialized_view(
    name="inventra.inventra_gold.gld_fornecedores"
)
def gold_fornecedores():
    return silver("tb_supplier").select(
        F.col("id_supplier").alias("nCdFornecedor"),
        F.col("legal_name").alias("cNmFornecedor"),
        F.col("cnpj").alias("nCdCNPJ"),
        F.col("email").alias("cEmailFornecedor"),
        F.col("whatsapp").alias("cWhatsAppFornecedor"),
        F.col("rating").alias("nNotaAvaliacaoFornecedor"),
        F.col("active").alias("cFlgFornecedorAtivo"),
        F.col("created_at").alias("dDataCriacaoFornecedor")
    )


# ============================================================
# PRODUTOS
# ============================================================

@dp.materialized_view(
    name="inventra.inventra_gold.gld_produtos"
)
def gold_produtos():
    return silver("tb_product").select(
        F.col("id_product").alias("nCdProduto"),
        F.col("name").alias("cNmProduto"),
        F.col("brand").alias("cNmMarca"),
        F.col("id_category").alias("nCdCategoria"),
        F.col("id_unit").alias("nCdUnidade"),
        F.col("barcode").alias("nCdCodigoBarras"),
        F.col("photo_url").alias("cUrlFotoProduto"),
        F.col("active").alias("cFlgProdutoAtivo"),
        F.col("created_at").alias("dDataCriacaoProduto")
    )


# ============================================================
# PRODUTO X FORNECEDOR
# ============================================================

@dp.materialized_view(
    name="inventra.inventra_gold.gld_produtos_fornecedores"
)
def gold_produtos_fornecedores():

    df = silver("tb_product_supplier")

    return (
        df
        .withColumn(
            "cFlgPrazoValido",
            F.when(
                F.col("lead_time_days").isNotNull()
                & (F.col("lead_time_days") >= 0),
                1
            ).otherwise(0)
        )
        .select(
            F.col("key_produto_fornecedor")
             .alias("Key_Produto_Fornecedor"),
            F.col("supplier_code")
             .alias("nCdProdutoFornecedor"),
            F.col("reference_price")
             .alias("nValorPrecoReferencia"),
            F.col("lead_time_days")
             .alias("nQtdeDiasPrazoEntrega"),
            F.col("cFlgPrazoValido")
        )
    )


# ============================================================
# LOTES
# ============================================================

@dp.materialized_view(
    name="inventra.inventra_gold.gld_lotes_estoque"
)
def gold_lotes():

    df = silver("tb_stock_batch")

    return (
        df
        .withColumn(
            "nValorEstoqueAtual",
            F.col("current_quantity") * F.col("unit_price")
        )
        .withColumn(
            "nValorEntrada",
            F.col("initial_quantity") * F.col("unit_price")
        )
        .withColumn(
            "cFlgComSaldo",
            F.when(F.col("current_quantity") > 0, 1).otherwise(0)
        )
        .withColumn(
            "cFlgVencido",
            F.when(
                (F.col("current_quantity") > 0)
                & (F.col("expiration_date") < F.current_date()),
                1
            ).otherwise(0)
        )
        .withColumn(
            "cFlgVence7Dias",
            F.when(
                (F.col("current_quantity") > 0)
                & (F.col("expiration_date") >= F.current_date())
                & (F.col("expiration_date") < F.date_add(F.current_date(), 8)),
                1
            ).otherwise(0)
        )
        .select(
            F.col("id_batch").alias("nCdLote"),
            F.col("id_product").alias("nCdProduto"),
            F.col("id_kitchen").alias("nCdCozinha"),
            F.col("id_supplier").alias("nCdFornecedor"),

            F.col("key_produto_fornecedor")
             .alias("Key_Produto_Fornecedor"),

            F.col("key_produto_cozinha")
             .alias("Key_Produto_Cozinha"),

            F.col("batch_number").alias("nCdLoteFornecedor"),
            F.col("invoice_number").alias("nCdNotaFiscal"),

            F.col("initial_quantity").alias("nQtdeInicial"),
            F.col("current_quantity").alias("nQtdeAtual"),

            F.col("entry_date").alias("dDataEntrada"),
            F.col("expiration_date").alias("dDataVencimento"),

            F.col("unit_price").alias("nValorPrecoUnitario"),
            F.col("status").alias("cStatusLote"),

            F.col("nValorEstoqueAtual"),
            F.col("nValorEntrada"),
            F.col("cFlgComSaldo"),
            F.col("cFlgVencido"),
            F.col("cFlgVence7Dias")
        )
    )


# ============================================================
# PARÂMETROS PRODUTO X COZINHA
# ============================================================

@dp.materialized_view(
    name="inventra.inventra_gold.gld_parametros_produto_cozinha"
)
def gold_parametros_produto_cozinha():

    parametros = silver("tb_product_kitchen_parameter")

    saldo = (
        silver("tb_stock_batch")
        .groupBy("id_product", "id_kitchen")
        .agg(
            F.sum("current_quantity").alias("nQtdeSaldoTotal")
        )
    )

    df = (
        parametros
        .join(
            saldo,
            ["id_product", "id_kitchen"],
            "left"
        )
    )

    return (
        df
        .withColumn(
            "nCoberturaMinimo",
            F.when(
                F.col("min_stock") > 0,
                F.col("nQtdeSaldoTotal") / F.col("min_stock")
            )
        )
        .withColumn(
            "cFlgAbaixoMinimo",
            F.when(
                F.col("min_stock").isNotNull()
                & F.col("nQtdeSaldoTotal").isNotNull()
                & (F.col("nQtdeSaldoTotal") < F.col("min_stock")),
                1
            ).otherwise(0)
        )
        .withColumn(
            "cFlgAcimaMaximo",
            F.when(
                F.col("max_stock").isNotNull()
                & F.col("nQtdeSaldoTotal").isNotNull()
                & (F.col("nQtdeSaldoTotal") > F.col("max_stock")),
                1
            ).otherwise(0)
        )
        .select(
            F.col("key_produto_cozinha")
             .alias("Key_Produto_Cozinha"),

            F.col("min_stock")
             .alias("nQtdeEstoqueMinimo"),

            F.col("max_stock")
             .alias("nQtdeEstoqueMaximo"),

            F.col("average_daily_consumption")
             .alias("nQtdeConsumoMedioDiario"),

            F.col("nQtdeSaldoTotal"),
            F.col("nCoberturaMinimo"),
            F.col("cFlgAbaixoMinimo"),
            F.col("cFlgAcimaMaximo")
        )
    )


# ============================================================
# REQUISIÇÕES
# ============================================================

@dp.materialized_view(
    name="inventra.inventra_gold.gld_requisicoes"
)
def gold_requisicoes():

    return (
        silver("tb_requisition")
        .withColumn(
            "cFlgSemDataAprovacao",
            F.when(
                F.col("approved_at").isNull(),
                1
            ).otherwise(0)
        )
        .select(
            F.col("id_requisition").alias("nCdRequisicao"),
            F.col("requisition_type").alias("cTipoRequisicao"),
            F.col("origin").alias("cOrigemRequisicao"),
            F.col("status").alias("cStatusRequisicao"),
            F.col("reason").alias("cMotivoRequisicao"),
            F.col("id_kitchen").alias("nCdCozinha"),

            F.col("id_requester_user")
             .alias("nCdUsuarioSolicitante"),

            F.col("id_approver_user")
             .alias("nCdUsuarioAprovador"),

            F.to_date("created_at")
             .alias("dDataCriacaoRequisicao"),

            F.to_date("approved_at")
             .alias("dDataAprovacaoRequisicao"),

            F.col("cFlgSemDataAprovacao")
        )
    )


# ============================================================
# ITENS REQUISIÇÃO
# ============================================================

@dp.materialized_view(
    name="inventra.inventra_gold.gld_itens_requisicoes"
)
def gold_itens_requisicoes():

    return silver("tb_requisition_item").select(
        F.col("id_requisition_item")
         .alias("nCdItemRequisicao"),

        F.col("id_requisition")
         .alias("nCdRequisicao"),

        F.col("id_product")
         .alias("nCdProduto"),

        F.col("id_suggested_supplier")
         .alias("nCdFornecedorSugerido"),

        F.col("quantity")
         .alias("nQtdeSolicitada"),

        F.col("estimated_price")
         .alias("nValorPrecoEstimado"),

        F.col("note")
         .alias("cObservacaoItemRequisicao")
    )


# ============================================================
# INVENTÁRIOS
# ============================================================

@dp.materialized_view(
    name="inventra.inventra_gold.gld_inventarios"
)
def gold_inventarios():

    return silver("tb_inventory").select(
        F.col("id_inventory").alias("nCdInventario"),
        F.col("id_kitchen").alias("nCdCozinha"),

        F.col("id_responsible_user")
         .alias("nCdUsuarioResponsavel"),

        F.to_date("started_at")
         .alias("dDataInicioInventario"),

        F.to_date("closed_at")
         .alias("dDataFechamentoInventario"),

        F.col("status").alias("cStatusInventario"),
        F.col("note").alias("cObservacaoInventario")
    )


# ============================================================
# CONTAGENS
# ============================================================

@dp.materialized_view(
    name="inventra.inventra_gold.gld_contagens_inventario"
)
def gold_contagens():

    return (
        silver("tb_inventory_count")
        .withColumn(
            "cFlgDivergencia",
            F.when(
                F.col("divergence") != 0,
                1
            ).otherwise(0)
        )
        .select(
            F.col("id_count")
             .alias("nCdContagemInventario"),

            F.col("id_inventory")
             .alias("nCdInventario"),

            F.col("id_batch")
             .alias("nCdLote"),

            F.col("registered_quantity")
             .alias("nQtdeRegistrada"),

            F.col("physical_quantity")
             .alias("nQtdeFisica"),

            F.col("divergence")
             .alias("nQtdeDivergencia"),

            F.col("note")
             .alias("cObservacaoContagem"),

            F.col("cFlgDivergencia")
        )
    )


# ============================================================
# ALERTAS
# ============================================================

@dp.materialized_view(
    name="inventra.inventra_gold.gld_alertas"
)
def gold_alertas():

    return (
        silver("tb_alert")
        .withColumn(
            "cFlgComLote",
            F.when(
                F.col("id_batch").isNotNull(),
                1
            ).otherwise(0)
        )
        .select(
            F.col("id_alert").alias("nCdAlerta"),
            F.col("type").alias("cTipoAlerta"),
            F.col("severity").alias("cNivelSeveridade"),
            F.col("id_batch").alias("nCdLote"),

            F.col("id_product")
             .alias("nCdProdutoAlerta"),

            F.col("id_kitchen")
             .alias("nCdCozinhaAlerta"),

            F.col("message")
             .alias("cMensagemAlerta"),

            F.col("is_read")
             .alias("cFlgAlertaLido"),

            F.to_date("created_at")
             .alias("dDataCriacaoAlerta"),

            F.col("cFlgComLote")
        )
    )


# ============================================================
# CALENDÁRIO MESTRE
# ============================================================

@dp.materialized_view(
    name="inventra.inventra_gold.gld_calendario_mestre"
)
def gold_calendario_mestre():

    lotes = spark.read.table(
        "inventra.inventra_gold.gld_lotes_estoque"
    )

    limites = lotes.agg(
        F.min("dDataEntrada").alias("MinData"),
        F.max("dDataEntrada").alias("MaxData")
    )

    datas = limites.select(
        F.explode(
            F.sequence(
                F.coalesce(
                    F.col("MinData"),
                    F.trunc(F.current_date(), "year")
                ),
                F.coalesce(
                    F.col("MaxData"),
                    F.current_date()
                ),
                F.expr("INTERVAL 1 DAY")
            )
        ).alias("dDataEntrada")
    )

    return datas.select(
        F.col("dDataEntrada"),

        F.dayofmonth("dDataEntrada").alias("Dia"),
        F.month("dDataEntrada").alias("Mes"),
        F.year("dDataEntrada").alias("Ano"),

        F.date_format("dDataEntrada", "MMM")
         .alias("MesNome"),

        F.date_trunc("month", "dDataEntrada")
         .alias("MesAno"),

        F.weekofyear("dDataEntrada")
         .alias("Semana"),

        F.concat(
            F.lit("T"),
            F.quarter("dDataEntrada")
        ).alias("Trimestre"),

        F.concat(
            F.lit("S"),
            F.when(
                F.month("dDataEntrada") <= 6,
                1
            ).otherwise(2)
        ).alias("Semestre"),

        F.date_format(
            "dDataEntrada",
            "yyyy-MM"
        ).alias("AnoMesNum"),

        F.month(F.current_date())
         .alias("MesAtual")
    )


# ============================================================
# KPIs RESUMO
# ============================================================

@dp.materialized_view(
    name="inventra.inventra_gold.gld_kpis_resumo"
)
def gold_kpis_resumo():

    lotes = spark.read.table(
        "inventra.inventra_gold.gld_lotes_estoque"
    )

    parametros = spark.read.table(
        "inventra.inventra_gold.gld_parametros_produto_cozinha"
    )

    requisicoes = spark.read.table(
        "inventra.inventra_gold.gld_requisicoes"
    )

    itens = spark.read.table(
        "inventra.inventra_gold.gld_itens_requisicoes"
    )

    contagens = spark.read.table(
        "inventra.inventra_gold.gld_contagens_inventario"
    )

    alertas = spark.read.table(
        "inventra.inventra_gold.gld_alertas"
    )


    # --------------------------------------------------------
    # ESTOQUE
    # --------------------------------------------------------

    kpi_estoque = lotes.agg(

        F.sum("nValorEstoqueAtual")
         .alias("kpi_valor_atual_estoque"),

        F.countDistinct(
            F.when(
                F.col("nQtdeAtual") > 0,
                F.col("nCdLote")
            )
        ).alias("kpi_lotes_com_saldo"),

        F.countDistinct(
            F.when(
                F.col("nQtdeAtual") > 0,
                F.col("nCdProduto")
            )
        ).alias("kpi_produtos_com_saldo"),

        F.countDistinct(
            F.when(
                F.col("cFlgVencido") == 1,
                F.col("nCdLote")
            )
        ).alias("kpi_lotes_vencidos_com_saldo"),

        F.countDistinct(
            F.when(
                F.col("cFlgVence7Dias") == 1,
                F.col("nCdLote")
            )
        ).alias("kpi_lotes_a_vencer_7_dias"),

        F.sum(
            F.when(
                F.col("cFlgVence7Dias") == 1,
                F.col("nValorEstoqueAtual")
            ).otherwise(0)
        ).alias("kpi_valor_a_vencer_7_dias")
    )


    # --------------------------------------------------------
    # PARÂMETROS
    # --------------------------------------------------------

    kpi_parametros = parametros.agg(

        F.sum("cFlgAbaixoMinimo")
         .alias("kpi_combinacoes_abaixo_minimo"),

        F.sum("cFlgAcimaMaximo")
         .alias("kpi_combinacoes_acima_maximo")
    )


    # --------------------------------------------------------
    # REQUISIÇÕES
    # --------------------------------------------------------

    kpi_requisicoes = requisicoes.agg(

        F.countDistinct("nCdRequisicao")
         .alias("kpi_requisicoes_registradas"),

        F.countDistinct(
            F.when(
                F.col("cFlgSemDataAprovacao") == 1,
                F.col("nCdRequisicao")
            )
        ).alias("kpi_requisicoes_sem_data_aprovacao")
    )


    kpi_itens = itens.agg(

        F.countDistinct("nCdItemRequisicao")
         .alias("kpi_itens_requisitados")
    )


    # --------------------------------------------------------
    # INVENTÁRIO
    # --------------------------------------------------------

    kpi_contagens = contagens.agg(

        F.countDistinct("nCdInventario")
         .alias("kpi_inventarios_com_contagem"),

        F.countDistinct("nCdContagemInventario")
         .alias("qtd_contagens"),

        F.countDistinct(
            F.when(
                F.col("cFlgDivergencia") == 1,
                F.col("nCdContagemInventario")
            )
        ).alias("qtd_contagens_divergentes")
    )

    kpi_contagens = kpi_contagens.withColumn(
        "kpi_percentual_contagens_divergentes",
        F.when(
            F.col("qtd_contagens") > 0,
            F.col("qtd_contagens_divergentes")
            / F.col("qtd_contagens")
        )
    )


    # --------------------------------------------------------
    # ALERTAS
    # --------------------------------------------------------

    kpi_alertas = alertas.agg(

        F.countDistinct(
            F.when(
                F.col("cFlgComLote") == 1,
                F.col("nCdAlerta")
            )
        ).alias("kpi_alertas_registrados")
    )


    # --------------------------------------------------------
    # UMA ÚNICA LINHA DE RESUMO
    # --------------------------------------------------------

    return (
        kpi_estoque
        .crossJoin(kpi_parametros)
        .crossJoin(kpi_requisicoes)
        .crossJoin(kpi_itens)
        .crossJoin(kpi_contagens)
        .crossJoin(kpi_alertas)
        .withColumn(
            "dDataReferencia",
            F.current_date()
        )
        .withColumn(
            "dTimestampAtualizacao",
            F.current_timestamp()
        )
    )