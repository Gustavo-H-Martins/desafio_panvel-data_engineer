ARG SPARK_VERSION=v3.4.0
FROM apache/spark-py:${SPARK_VERSION}

LABEL mainteiner="gustavo.lopes <gustavojoana10@gmail.com>"

# Desabilita a interatividade dos comandos
ENV DEBIAN_FRONTEND=noninteractive

# Expõe as portas `4040`
EXPOSE 4040 

# Copia os arquivos jars 
COPY  ./jars/* /opt/spark/jars/

# Copia o arquivos requirements.txt
# pip install
USER root
COPY requirements.txt ./
RUN python3 -m pip install --no-cache-dir -U pip
RUN python3 -m pip install --no-cache-dir -r requirements.txt


# Definndo a versão do python no pyspark
ENV PYSPARK_PYTHON=/usr/bin/python3

# Define o usuário para o job
RUN useradd --create-home --home-dir /app appuser


# Define o diretório principal da aplicação
WORKDIR /app
COPY . /app

# Alterar permissões dos diretorios
USER root
RUN chown -R appuser:appuser /app
RUN chmod -R 755 /app
USER appuser

CMD ["python3", "main.py"]
