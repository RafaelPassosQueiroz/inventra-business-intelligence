# Databricks — Inventra BI

Esta pasta contém o código-fonte e a documentação da camada de Engenharia de Dados utilizada pelo Business Intelligence do Inventra.

## Estrutura

O projeto utiliza o Catalog:

`inventra`

com os schemas:

- `inventra_bronze`
- `inventra_silver`
- `inventra_gold`

## Arquitetura Medalhão

### Bronze

Camada responsável por receber os dados provenientes do PostgreSQL próximos ao formato de origem.

Foram identificadas 15 estruturas.

### Silver

Camada de tratamento e padronização.

Entre as estratégias documentadas estão:

- deduplicação por identificador;
- limpeza de textos;
- conversão de datas e quantidades;
- construção de chaves de relacionamento;
- regras de qualidade.

Foram identificadas 15 estruturas.

### Gold

Camada destinada ao consumo analítico.

Foram identificadas 17 estruturas:

- `gld_alertas`
- `gld_calendario_mestre`
- `gld_categorias`
- `gld_contagens_inventario`
- `gld_cozinhas`
- `gld_fornecedores`
- `gld_inventarios`
- `gld_itens_requisicoes`
- `gld_kpis_resumo`
- `gld_lotes_estoque`
- `gld_parametros_produto_cozinha`
- `gld_perfis`
- `gld_produtos`
- `gld_produtos_fornecedores`
- `gld_requisicoes`
- `gld_unidades_medida`
- `gld_usuarios`

## Pipeline

Pipeline identificado:

`Pipeline_Inventra_Medalhao`

O grafo demonstrado possui 47 estruturas distribuídas pelas três camadas.

![Pipeline](../assets/images/databricks/pipeline-dag.png)

## Orquestração

Job:

`Job_Inventra_BI`

Task:

`Executar_Pipeline_Inventra`

Tipo da task:

`Pipeline`

Pipeline executado:

`Pipeline_Inventra_Medalhao`

A task está configurada para executar uma atualização completa do pipeline.

As evidências demonstram execuções manuais e disparadas pelo scheduler concluídas com sucesso.

## Agendamento

O Job apresenta programação diária às 07:50 na interface analisada.

O fuso horário da configuração não foi comprovado pelos materiais e, portanto, não é afirmado nesta documentação.

## Código-fonte

Os arquivos em `pipeline-source/` correspondem ao código-fonte exportado do Databricks.

A estrutura original exportada deve ser preservada sempre que possível para manter a correspondência com o pipeline configurado na plataforma.

## Segurança

Este diretório não deve conter:

- tokens;
- senhas;
- arquivos `.env`;
- credenciais PostgreSQL;
- Databricks Personal Access Tokens;
- connection strings com credenciais;
- chaves privadas.