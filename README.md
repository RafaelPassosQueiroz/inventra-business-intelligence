________________pt-BR________________

# Inventra Business Intelligence

Projeto de **Engenharia de Dados e Business Intelligence** desenvolvido para transformar dados operacionais do Inventra em informações gerenciais sobre estoque, validade, compras, fornecedores e inventários.

![Dashboard Inventra](assets/images/qlik/visao-geral.png)

## Sobre o projeto

O Inventra é uma solução voltada à gestão de estoque de cozinhas industriais.

Este repositório apresenta a camada analítica do projeto, responsável por transformar dados do PostgreSQL em informações prontas para análise no Qlik Sense.

O fluxo implementado é:

```text
PostgreSQL
    ↓
Databricks
    ↓
Bronze → Silver → Gold
    ↓
Qlik Sense
    ↓
Dashboard Gerencial
```

## Arquitetura de Dados

O processamento utiliza a **Arquitetura Medalhão** no Databricks:

- **Bronze:** ingestão dos dados de origem;
- **Silver:** limpeza, padronização e transformação;
- **Gold:** dados preparados para consumo analítico.

Principais componentes:

- `Pipeline_Inventra_Medalhao`
- `Job_Inventra_BI`
- Task `Executar_Pipeline_Inventra`
- Catálogo `inventra`
- Schemas `inventra_bronze`, `inventra_silver` e `inventra_gold`

## Dashboard

O dashboard **Painel - Inventra**, desenvolvido no Qlik Sense, está organizado em quatro áreas:

- Visão geral;
- Estoque e validade;
- Compras e fornecedores;
- Inventário, alertas e controle.

Entre os indicadores analisados estão:

- valor atual do estoque;
- lotes com saldo;
- produtos abaixo do estoque mínimo;
- lotes vencidos;
- cobertura de estoque;
- distribuição por categorias e cozinhas;
- fornecedores e requisições;
- divergências de inventário.

## Tecnologias

- PostgreSQL
- Databricks
- SQL
- Arquitetura Medalhão
- Qlik Sense
- GitHub

## Estrutura do repositório

```text
inventra-bi/
├── databricks/
├── qlik/
├── sql/
├── docs/
├── assets/
│   └── images/
├── .gitignore
└── README.md
```

## Segurança

Credenciais, tokens, secrets, senhas e informações de conexão sensíveis não são versionados neste repositório.

## Contexto

O projeto faz parte do ecossistema **Inventra** e foi desenvolvido com finalidade acadêmica e de portfólio.

Os dados utilizados nas demonstrações são fictícios e têm como objetivo representar cenários de gestão de estoque industrial.


<hr>

________________en-US________________

# Inventra Business Intelligence

A **Data Engineering and Business Intelligence** project designed to transform Inventra's operational data into management insights about inventory, expiration dates, purchasing, suppliers, and inventory audits.

![Inventra Dashboard](assets/images/qlik/visao-geral.png)

## About the Project

Inventra is a solution focused on inventory management for industrial kitchens.

This repository contains the analytical layer of the project, transforming PostgreSQL operational data into information ready for analysis in Qlik Sense.

The implemented data flow is:

```text
PostgreSQL
    ↓
Databricks
    ↓
Bronze → Silver → Gold
    ↓
Qlik Sense
    ↓
Management Dashboard
```

## Data Architecture

Data processing follows the **Medallion Architecture** in Databricks:

- **Bronze:** raw data ingestion;
- **Silver:** data cleaning, standardization, and transformation;
- **Gold:** analytical datasets prepared for consumption.

Main components:

- `Pipeline_Inventra_Medalhao`
- `Job_Inventra_BI`
- Task `Executar_Pipeline_Inventra`
- Catalog `inventra`
- Schemas `inventra_bronze`, `inventra_silver`, and `inventra_gold`

## Dashboard

The **Painel - Inventra** dashboard was developed in Qlik Sense and is organized into four areas:

- Overview;
- Inventory and expiration;
- Purchasing and suppliers;
- Inventory audits, alerts, and control.

Main indicators include:

- current inventory value;
- stock batches with remaining quantity;
- products below minimum stock;
- expired batches;
- inventory coverage;
- distribution by category and kitchen;
- suppliers and requisitions;
- inventory discrepancies.

## Technologies

- PostgreSQL
- Databricks
- SQL
- Medallion Architecture
- Qlik Sense
- GitHub

## Repository Structure

```text
inventra-bi/
├── databricks/
├── qlik/
├── sql/
├── docs/
├── assets/
│   └── images/
├── .gitignore
└── README.md
```

## Security

Credentials, access tokens, secrets, passwords, and sensitive connection information are not stored in this repository.

## Context

This project is part of the **Inventra** ecosystem and was developed for academic and portfolio purposes.

The demonstration data is fictional and is intended to represent industrial inventory management scenarios.
