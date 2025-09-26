# Plataforma de Dados com Apache Spark, Iceberg e PostgreSQL

Este projeto implementa uma plataforma de dados moderna para processamento e armazenamento de dados em um data lake, utilizando tecnologias de ponta como Apache Spark para processamento distribuído, Apache Iceberg como formato de tabela aberta para data lakes, e PostgreSQL como sistema de banco de dados relacional, simulando um ambiente de ERP.

## Visão Geral da Arquitetura

A arquitetura é orquestrada com Docker Compose e consiste nos seguintes serviços:

-   **Apache Spark Cluster**:
    -   `spark-master`: Nó mestre do Spark, responsável por coordenar a execução de tarefas.
    -   `spark-worker`: Nós de trabalho que executam as tarefas de processamento de dados.
    -   `spark-history-server`: Uma interface web para monitorar e analisar o histórico de execuções das aplicações Spark.
-   **PostgreSQL (`postgres_erp`)**: Um banco de dados PostgreSQL que atua como a fonte de dados, simulando um sistema transacional (ERP) com o banco de dados `northwind`.
-   **Data Lake Storage**: O armazenamento do data lake é simulado em um diretório local (`./dados`), que é montado como um volume nos contêineres Spark. As tabelas Iceberg são armazenadas neste diretório.

## Tecnologias Utilizadas

-   **Orquestração**: Docker e Docker Compose
-   **Processamento de Dados**: Apache Spark 3.3
-   **Formato de Tabela**: Apache Iceberg 1.6.1
-   **Banco de Dados de Origem**: PostgreSQL
-   **Linguagem**: Python 3 (PySpark)

## Pré-requisitos

-   [Docker](https://www.docker.com/get-started)
-   [Docker Compose](https://docs.docker.com/compose/install/)

## Estrutura de Diretórios

```
.
├── docker-compose.yml      # Arquivo de orquestração dos serviços Docker
├── dados/                  # Diretório do Data Lake (tabelas Iceberg)
├── db/
│   └── northwind.sql       # Script SQL para popular o banco de dados de origem
├── scripts/
│   └── ingest.py           # Script PySpark para ingestão de dados
├── spark/
│   ├── Dockerfile          # Dockerfile para a imagem customizada do Spark
│   ├── .env.spark          # Variáveis de ambiente para o Spark
│   └── entrypoint.sh       # Script de inicialização para os contêineres Spark
└── README.md
```

## Como Executar o Projeto

1.  **Clone o repositório:**
    ```bash
    git clone https://github.com/AleTavares/dados_Iceberg.git
    cd dados_Iceberg
    ```

2.  **Construa e inicie os contêineres:**
    Este comando irá construir a imagem Docker customizada para o Spark (se ainda não existir) e iniciar todos os serviços em segundo plano.
    ```bash
    docker-compose up -d --build
    ```

3.  **Verifique o status dos serviços:**
    Para garantir que todos os contêineres estão rodando corretamente:
    ```bash
    docker-compose ps
    ```

## Acessando os Serviços

-   **Spark Master UI**: [http://localhost:9090](http://localhost:9090)
-   **Spark History Server**: [http://localhost:18080](http://localhost:18080)
-   **PostgreSQL**: Acessível na porta `2001` da sua máquina local (`localhost`).

## Executando a Ingestão de Dados

O script `scripts/ingest.py` é responsável por ler os dados da tabela `customers` do PostgreSQL e ingeri-los em uma tabela Iceberg no data lake.

Para executar o script, você pode usar o comando `spark-submit` dentro do contêiner do Spark Master:

1.  **Acesse o contêiner do Spark Master:**
    ```bash
    docker exec -it spark_iceberg_master bash
    ```

2.  **Execute o `spark-submit`:**
    O diretório `scripts` do seu host é montado em `/opt/spark/apps` no contêiner.
    ```bash
    spark-submit /opt/spark/apps/ingest.py
    ```

Após a execução, você pode verificar os arquivos da tabela Iceberg no diretório `./dados/default/customers` do seu host.

## Como Parar a Plataforma

Para parar todos os serviços e remover os contêineres, execute:
```bash
docker-compose down
```

Se desejar remover também os volumes (isso apagará os dados do PostgreSQL e do data lake), utilize:
```bash
docker-compose down -v
```