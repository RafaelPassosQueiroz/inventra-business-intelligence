________________pt-BR________________

# Databricks — Inventra BI

Esta pasta reúne os arquivos relacionados à camada de **Engenharia de Dados** do Inventra BI no Databricks.

O projeto utiliza o Catalog `inventra` e segue a arquitetura Medalhão:

- `inventra_bronze` — ingestão dos dados;
- `inventra_silver` — tratamento e padronização;
- `inventra_gold` — preparação dos dados para análise.

O processamento é realizado pelo pipeline:

`Pipeline_Inventra_Medalhao`

e sua execução é orquestrada pelo Job:

`Job_Inventra_BI`

com a task:

`Executar_Pipeline_Inventra`

O código exportado do Databricks está disponível em:

`pipeline-source/`

Este README funciona apenas como uma visão geral. Os arquivos do diretório representam o código-fonte e a configuração utilizada no pipeline.

## Segurança

Tokens, senhas, secrets, arquivos `.env` e outras credenciais não devem ser armazenados neste diretório.

<hr>

________________en-US________________

# Databricks — Inventra BI

This folder contains the files related to the **Data Engineering** layer of Inventra BI in Databricks.

The project uses the `inventra` Catalog and follows the Medallion Architecture:

- `inventra_bronze` — data ingestion;
- `inventra_silver` — data cleaning and transformation;
- `inventra_gold` — analytical data preparation.

Data processing is handled by:

`Pipeline_Inventra_Medalhao`

and orchestrated by:

`Job_Inventra_BI`

with the task:

`Executar_Pipeline_Inventra`

The Databricks source code is available in:

`pipeline-source/`

This README provides only a general overview. The files in this directory contain the actual source code and pipeline implementation.

## Security

Tokens, passwords, secrets, `.env` files, and other credentials must not be stored in this directory.