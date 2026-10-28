________________pt-BR________________

# Qlik Sense — Inventra BI

Esta pasta reúne os arquivos relacionados à camada de **Business Intelligence** do Inventra desenvolvida no Qlik Sense.

Aplicativo:

`Painel - Inventra`

O aplicativo utiliza a conexão:

`Databricks_Inventra`

para consumir dados da camada `inventra_gold` do Databricks.

O script de carga utilizado pelo aplicativo está disponível em:

`load-script/inventra_load_script.qvs`

O dashboard está organizado nas seguintes áreas:

- Visão geral;
- Estoque e validade;
- Compras e fornecedores;
- Inventário, alertas e controle.

Este README apresenta apenas uma visão geral do aplicativo. O script de carga, screenshots e demais arquivos deste diretório documentam a implementação real no Qlik Sense.

Caso o arquivo QVF seja disponibilizado, recomenda-se utilizar uma versão exportada sem dados em:

`app/Painel-Inventra-sem-dados.qvf`

## Segurança

Credenciais, tokens, secrets e informações sensíveis de conexão não devem ser versionados neste diretório.

<hr>

________________en-US________________

# Qlik Sense — Inventra BI

This folder contains the files related to the **Business Intelligence** layer of Inventra developed in Qlik Sense.

Application:

`Painel - Inventra`

The application uses the following connection:

`Databricks_Inventra`

to consume data from the Databricks `inventra_gold` layer.

The load script used by the application is available at:

`load-script/inventra_load_script.qvs`

The dashboard is organized into the following areas:

- Overview;
- Inventory and expiration;
- Purchasing and suppliers;
- Inventory audits, alerts, and control.

This README provides only a general overview of the application. The load script, screenshots, and other files in this directory document the actual Qlik Sense implementation.

If a QVF file is provided, it is recommended to store an exported version without data at:

`app/Painel-Inventra-sem-dados.qvf`

## Security

Credentials, tokens, secrets, and sensitive connection information must not be stored in this directory.