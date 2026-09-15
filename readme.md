# Projeto Indústria 4.0 — Pipeline de Dados Azure

> Documento de trabalho em progresso. Este arquivo vai sendo atualizado conforme novas seções (real-time, API, infraestrutura) são capturadas e documentadas.

## Visão geral da arquitetura

```
Batch: CSV (raw) → Data Factory → Data Lake (RAW/BRONZE/SILVER)
     → Synapse notebooks (PySpark) → gold.dim_* e gold.fact_producao (SQL)

Real-time: Script Python (iot_devices_industry.py) → IoT Hub → Data Explorer (modulo-fnal)
     → gold.condicao_maquina_diaria (SQL)

API: Azure Functions → 3 rotas HTTP consultando o SQL Gold
     (producao-resumo, condicao-maquina, saude-linha)
```

---

## Pipeline Batch: Raw → Bronze → Silver → Gold

O pipeline batch é responsável por ingerir dados históricos de produção (CSV) e transformá-los progressivamente até chegar num modelo dimensional pronto para consulta, seguindo a arquitetura medallion.

### Orquestração (Azure Data Factory)

O pipeline `pl_raw_to_bronze` copia os arquivos brutos para a camada Raw do Data Lake e dispara a primeira transformação. Abaixo, o histórico de execuções mostrando processamento bem-sucedido de ponta a ponta:

![Pipeline runs no Data Factory, 3 execuções com status Succeeded](docs/images/01-pipeline-runs-data-factory.png)

Além do Data Factory, o próprio Synapse orquestra um pipeline interno (`pipeline_batch_industry_4_0`) que encadeia visualmente cada etapa da transformação — leitura, metadados, processamento por linha (ForEach), e carga final via stored procedure:

![Visão gráfica do pipeline_batch_industry_4_0 no Synapse](docs/images/02-pipeline-grafico-synapse.png)

### Armazenamento em camadas (Azure Data Lake Storage)

Os dados são organizados fisicamente em três camadas dentro do container `inicial-datalake`, cada uma representando um estágio de maturidade dos dados:

![Estrutura de pastas RAW/BRONZE/SILVER no Data Lake](docs/images/03-datalake-estrutura-camadas.png)

### Transformação e modelagem dimensional (Azure Synapse Analytics)

A transformação final ocorre em notebooks PySpark no Synapse, organizados em três etapas sequenciais (`01_raw_to_bronze`, `02_bronze_to_silver`, `03_silver_to_gold`). A última etapa lê os dados validados da camada Silver:

![Notebook 03_silver_to_gold lendo a camada Silver](docs/images/04-notebook-silver-to-gold-leitura.png)

...e constrói o modelo final em Gold: dimensões carregadas via overwrite simples (baixa cardinalidade) e a tabela fato `gold.fact_producao` atualizada via upsert incremental, usando a chave de negócio (`sk_linha + sk_maquina + sk_data`) para garantir que reprocessamentos atrasados não dupliquem registros:

![DDL da tabela fato e confirmação de execução do notebook](docs/images/05-notebook-silver-to-gold-ddl-confirmacao.png)

### Resultado final consultável (Azure SQL Database)

Os dados transformados ficam disponíveis para consumo (dashboards, APIs) na tabela `gold.fact_producao`, com métricas de produção já calculadas:

![Query SELECT TOP 10 * FROM gold.fact_producao com resultado real](docs/images/06-sql-gold-fact-producao.png)

---

## Pipeline Real-time: IoT Hub → Data Explorer → SQL

O pipeline real-time captura telemetria simulada de sensores das máquinas da fábrica, ingere via IoT Hub, armazena em série temporal no Azure Data Explorer, e agrega diariamente para consumo no SQL Gold.

### Dispositivos IoT

8 devices simulando máquinas da fábrica (M01–M03 → Linha 1, M04–M06 → Linha 2, M07–M08 → Linha 3), registrados e habilitados no IoT Hub:

![Lista de devices M01-M08 no IoT Hub, todos Enabled](docs/images/07-iot-hub-devices.png)

### Telemetria bruta no Data Explorer (Azure Data Explorer / Kusto)

Os dados de sensor (temperatura, vibração, RPM) chegam na tabela `TelemetriaMaquinas`, com granularidade de série temporal (`id_maquina`, `linha_producao`, `timestamp`):

![Query KQL TelemetriaMaquinas com 10 registros reais de telemetria](docs/images/08-data-explorer-telemetria-kql.png)

> Nota operacional: o cluster Data Explorer cobra por tempo de compute mesmo ocioso, então fica parado (Stopped) por padrão e só é ligado sob demanda para ingestão/consulta — lição aprendida após um incidente de custo (ver seção "Decisões técnicas e lições aprendidas").

### Agregação diária consultável (Azure SQL Database)

A telemetria bruta é agregada diariamente por máquina e disponibilizada na tabela `gold.condicao_maquina_diaria`, com médias de temperatura, vibração e RPM prontas para consumo:

![Query SELECT TOP 10 * FROM gold.condicao_maquina_diaria com resultado real](docs/images/09-sql-gold-condicao-maquina-diaria.png)

---

## API: Azure Functions

*(código completo e testado localmente com dados reais — deploy na nuvem pendente, ver detalhes abaixo)*

### Bloqueio inicial e resolução

Durante a criação da Function App, o portal Azure não exibia mais a opção clássica de plano Consumption (Linux) como card visual — apenas "Flex Consumption" (não suportado em conta Free Trial) e "Consumption (Windows)" (sem suporte a runtime Python). Após diagnóstico com `az functionapp create --debug`, foi identificado que o erro de rede (`ConnectionResetError`) ocorria numa chamada específica (`functionAppStacks`) que baixa um catálogo grande, provavelmente por limitação de rede local.

A resolução definitiva veio do upgrade da assinatura de Free Trial para Pay-As-You-Go (mantendo o crédito promocional intacto até a data de expiração original) — isso removeu a restrição de "Flex Consumption não suportado em conta trial", permitindo criar a Function App pelo fluxo padrão do portal.

### Status atual

- ✅ Function App criada no portal (plano Flex Consumption, runtime Python 3.13, Linux)
- ✅ Código local com as 3 rotas HTTP (`producao-resumo`, `condicao-maquina`, `saude-linha`), credenciais via variável de ambiente (nunca hardcoded, com referência preparada para Key Vault)
- ✅ Testado localmente (`func start` + Azurite como emulador de storage) — as 3 rotas respondendo com dados reais do SQL Gold, incluindo join entre produção e condição de máquina na rota combinada
- ⬜ Deploy para a nuvem pendente — bloqueado por `ConnectionResetError` de rede ao tentar publicar via `az functionapp deployment source config-zip` (mesmo padrão de erro observado no bloqueio inicial), sugerindo interferência de rede local/antivírus/provedor, não um problema de código ou conta. Diagnóstico planejado: testar em rede diferente (hotspot) para confirmar a causa.

### Nota técnica: caminho de deploy

O plano original era publicar via extensão Azure Functions do VS Code, seguindo o mesmo fluxo do curso. Dois obstáculos técnicos levaram a uma rota alternativa:
1. A extensão do VS Code apresentou erro persistente ao listar assinaturas disponíveis (`select a subscription` vazio)
2. O login do Azure CLI travava silenciosamente (janela de seleção de conta fechava sem completar) — causa identificada como bug conhecido do broker WAM do Windows, resolvido com `az config set core.enable_broker_on_windows=false`

Com o login resolvido, o deploy foi tentado via `az functionapp deployment source config-zip` (empacotando o código em `.zip`), mas esbarrou no `ConnectionResetError` de rede mencionado acima.

`[PRINT 8 — quando o deploy for concluído: teste dos 3 endpoints públicos respondendo com dado real]`

---

## Infraestrutura como Código e CI/CD

*(planejado — módulos bônus do curso: Terraform, Docker, GitHub Actions)*

---

## Decisões técnicas e lições aprendidas

- Separação de responsabilidades: Storage Account dedicado (`stfuncindustria40`) para a Function App, separado do Data Lake principal (`dlcursoazure`)
- Segurança: uso de variáveis de ambiente / `.env` para todas as credenciais (nunca hardcoded), após incidente de exposição acidental
- Custo: incidente de ~R$170 causado pelo cluster Azure Data Explorer permanecendo em estado "Running" sem uso ativo — lição aprendida sobre monitoramento ativo de recursos de compute

### Credenciais do SQL nos notebooks Synapse: de texto plano para Azure Key Vault

Durante a preparação do repositório público, uma revisão de segurança nos notebooks do Synapse (etapa anterior a qualquer commit) identificou que a célula de conexão JDBC do notebook `03_silver_to_gold` continha usuário e senha do Azure SQL Database em texto plano — prática comum em ambiente de curso/aprendizado, mas inadequada para um repositório público.

Passos da correção:
1. Tentativa inicial com `mssparkutils.credentials.getFullConnectionString()` apontando para o linked service SQL já existente no workspace — abordagem descartada após erro (`Missing required property 'connectionstring'`), uma limitação documentada da função para linked services configurados com campos separados (servidor/usuário/senha) em vez de uma connection string única.
2. Solução definitiva: criação de um Azure Key Vault (`kv-curso-azure`) dedicado, com os segredos `sql-server-user` e `sql-server-password` cadastrados, conectado ao workspace Synapse como linked service, com permissão de acesso via Azure RBAC (role "Key Vault Secrets Officer").
3. O notebook passou a usar `mssparkutils.credentials.getSecretWithLS("kv_curso_azure", "sql-server-password")` para buscar as credenciais em tempo de execução — a senha nunca fica escrita em nenhum arquivo do repositório nem do notebook. Testado e confirmado funcionando no Synapse Studio após publicação do linked service (nota técnica: `getSecretWithLS` recebe o nome do *linked service*, não o nome do recurso Key Vault — usar `getSecret` com o nome do recurso resulta em erro de resolução de host).

Como precaução adicional, a senha do SQL será trocada, já que existiu em texto plano no histórico de execução do notebook por um período.
