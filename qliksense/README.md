# Qlik Sense — Inventra BI

Esta pasta documenta o aplicativo de Business Intelligence do Inventra desenvolvido no Qlik Sense.

Aplicativo:

`Painel - Inventra`

## Fonte analítica

O aplicativo utiliza uma conexão identificada como:

`Databricks_Inventra`

O script de carga consulta estruturas da camada Gold do Catalog `inventra`.

Exemplo observado:

```sql
SQL SELECT *
FROM inventra.inventra_gold.gld_perfis;
```

O script completo utilizado pelo aplicativo está versionado em:

`load-script/inventra_load_script.qvs`

Credenciais, endpoints privados e secrets não são armazenados neste repositório.

## Filtros globais

Foram identificados os seguintes filtros:

- Cozinha
- Categoria
- Produto
- Unidade
- Ano
- Mês

## Páginas

### Visão geral

Apresenta o panorama operacional de estoque e os principais sinais de atenção.

Principais componentes:

- valor atual do estoque;
- lotes com saldo;
- abaixo do mínimo;
- lotes vencidos com saldo;
- valor em estoque por cozinha;
- composição por categoria;
- entradas registradas por mês;
- detalhamento por produto/cozinha.

### Estoque e validade

Direcionada à investigação de validade, concentração financeira e excesso.

Principais componentes:

- lotes a vencer em sete dias;
- valor a vencer em sete dias;
- acima do máximo;
- concentração por produto;
- distribuição dos valores dos lotes por cozinha;
- tabelas de validade e quantidade.

### Compras e fornecedores

Relaciona requisições às alternativas de abastecimento.

Principais componentes:

- requisições registradas;
- requisições sem aprovação datada;
- itens;
- requisições por cozinha;
- produtos mais requisitados;
- comparativo de fornecedores;
- distribuição dos prazos;
- preço versus prazo de entrega.

### Inventário, alertas e controle

Direcionada à conferência física e à consistência dos registros.

Principais componentes:

- inventários;
- percentual de contagens divergentes;
- alertas;
- alertas por cozinha;
- divergências por cozinha;
- divergências por semana;
- inventários e responsáveis;
- detalhamento das contagens.

## Observação sobre os indicadores

Os materiais utilizados na documentação representam diferentes momentos de atualização.

Por esse motivo, números observados na Gold e no Qlik podem divergir e não devem ser tratados como uma única posição operacional homologada sem uma conciliação prévia.

## Aplicativo QVF

Quando disponibilizado neste diretório, recomenda-se armazenar apenas uma versão exportada **sem dados**:

`app/Painel-Inventra-sem-dados.qvf`

O QVF é um artefato complementar.

A documentação principal do aplicativo permanece composta pelo script de carga, screenshots e descrição das análises.