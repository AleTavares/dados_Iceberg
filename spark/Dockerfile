# Cria Imagem Spark

# Imagem do SO usada como base
FROM python:3.11-bullseye as spark-base

# Atualiza o SO e instala pacotes
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      sudo \
      curl \
      vim \
      nano \
      unzip \
      rsync \
      openjdk-11-jdk \
      build-essential \
      software-properties-common \
      ssh && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*


# Variáveis de ambiente
ENV SPARK_HOME=${SPARK_HOME:-"/opt/spark"}
ENV HADOOP_HOME=${HADOOP_HOME:-"/opt/hadoop"}

# Cria as pastas
RUN mkdir -p ${HADOOP_HOME} && mkdir -p ${SPARK_HOME}
WORKDIR ${SPARK_HOME}

# Download do arquivo de binários do Spark
RUN curl https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz -o spark-3.5.0-bin-hadoop3.tgz \
 && tar xvzf spark-3.5.0-bin-hadoop3.tgz --directory /opt/spark --strip-components 1 \
 && rm -rf spark-3.5.0-bin-hadoop3.tgz \
 && wget -q https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.6.1/iceberg-spark-runtime-3.5_2.12-1.6.1.jar -P /opt/spark/jars/ \
 && wget -q https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar -P /opt/spark/jars/

# Prepara o ambiente com PySpark
FROM spark-base as pyspark

# Instala as dependências Python
COPY ./spark/requirements/requirements.txt .
RUN pip3 install -r requirements.txt

# Mais variáveis de ambiente
ENV PATH="/opt/spark/sbin:/opt/spark/bin:${PATH}"
ENV SPARK_HOME="/opt/spark"
ENV SPARK_MASTER="spark://spark-master:7077"
ENV SPARK_MASTER_HOST spark-master
ENV SPARK_MASTER_PORT 7077
ENV PYSPARK_PYTHON python3

# Copia o arquivo de configuração do Spark para a imagem
COPY ./spark/conf/spark-defaults.conf "$SPARK_HOME/conf"

# Permissões
RUN chmod u+x /opt/spark/sbin/* && \
    chmod u+x /opt/spark/bin/*

# Variável PYTHONPATH
ENV PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH

# Copia o script de inicialização dos serviços para a imagem
COPY ./spark/entrypoint.sh .

# Ajusta o privilégio
RUN chmod +x entrypoint.sh
RUN mkdir -p /datalake

# Executa o script quando inicializar um container
ENTRYPOINT ["./entrypoint.sh"]

