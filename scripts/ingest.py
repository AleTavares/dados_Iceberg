import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col

spark = SparkSession.builder \
    .appName("IcebergWithSpark") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.6.1,org.postgresql:postgresql:42.3.1") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.hadoop_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.hadoop_catalog.type", "hadoop") \
    .config("spark.sql.catalog.hadoop_catalog.warehouse", "/datalake") \
    .config("spark.sql.default.catalog", "hadoop_catalog") \
    .getOrCreate()

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

spark.sql("DROP TABLE IF EXISTS hadoop_catalog.default.customers")

spark.sql("""
    CREATE TABLE hadoop_catalog.default.customers (
        customer_id string NOT NULL,
        company_name string NOT NULL,
        contact_name string,
        contact_title string,
        address string,
        city string,
        region string,
        postal_code string,
        country string,
        phone string,
        fax string
    )
    USING iceberg          
          """)

# Credenciais do PostgreSQL
jdbc_hostname = "postgres-erp"
jdbc_port = 5432
jdbc_database = "northwind"
jdbc_username = "postgres"
jdbc_password = "postgres"

# URL JDBC de conexão
jdbc_url = f"jdbc:postgresql://{jdbc_hostname}:{jdbc_port}/{jdbc_database}"

# Propriedades de conexão JDBC
connection_properties = {
    "user": jdbc_username,
    "password": jdbc_password,
    "driver": "org.postgresql.Driver"
}

def read_postgres_data():
    query = f"""
        (SELECT *
         FROM customers
        ) AS customers
    """
    df = spark.read.jdbc(
        url=jdbc_url,
        table=query,
        properties=connection_properties
    )
    return df

# Ler dados do PostgreSQL
df_postgres = read_postgres_data()

# Exibir os dados lidos
df_postgres.show()

# Criar uma visão temporária
df_postgres.createOrReplaceTempView("customers")

# Executar o MERGE INTO
spark.sql("""
    MERGE INTO hadoop_catalog.default.customers AS target
    USING customers AS source
    ON target.customer_id = source.customer_id
    WHEN MATCHED THEN
        UPDATE SET *
    WHEN NOT MATCHED THEN
        INSERT *
""")

row_count = spark.sql("SELECT COUNT(*) FROM hadoop_catalog.default.customers").collect()[0][0]
print(f"Total de linhas na tabela customers: {row_count}")
#26214

# Atualizar o timestamp da última execução
last_run_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print(f"Processo concluído. Novo timestamp da última execução: {last_run_timestamp}")