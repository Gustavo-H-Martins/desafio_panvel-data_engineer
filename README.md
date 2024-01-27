# 💾📀 Projeto de Engenharia de Dados - Grupo Panvel

## 🗄👨‍💼👩‍💼 Visão Geral do Projeto

O Grupo Panvel tem como objetivo entender o comportamento de compra dos clientes para oferecer um serviço personalizado.
Este projeto de engenharia de dados visa fornecer as bases necessárias para a análise do comportamento de compra dos clientes.

## ♻🎣 Requisitos de Dados

O pipeline de dados espera receber os seguintes campos de dados de vendas e clientes:

- Vendas: código, data, item, valor, quantidade, descontos, canais
- Clientes: código, data de nascimento, idade, gênero, localização, flags de privacidade

## ⚙💡 Requisitos do projeto

- [x] Versão 3.4.0 ou superior do Apache Spark
- [x] Versão 3.3.0 ou superior do Aoache Hadoop
- [x] Versão 3.6 até 3.10 do Python
- [x] Docker Engine ou Desktop
- [x] JDK11 ou JDK17 ou JDK21... ou outro Kit de Desenvolvimento Java equivalente ao Spark e Hadoop
- [x] Arquivos Jars: `delta-core_2.12-2.4.0.jar`, `delta-storage-2.4.0.jar` os dois são obrigatórios.
- [x] [Consultar Release](https://docs.delta.io/latest/releases.html) de referência Delta Lake e Spark 

## 🏁🏎  Pipeline de Dados

O processo de extração, transformação e carregamento dos dados necessários será realizado utilizando as seguintes etapas:

1. Extração: os dados foram entregues em um diretório com arquivos `parquet` nomeados.
2. Transformação: os dados serão limpos, validados e transformados em um formato adequado para análise.
3. Carregamento: os dados processados serão carregados em um repositório ou banco de dados para acesso e análise.

## 🔍🔎 Garantia da Qualidade dos Dados

Para garantir a qualidade dos dados, serão utilizados métodos de validação, limpeza e tratamentos necessários.
Isso incluirá a verificação de consistência, integridade e precisão dos dados.

## 📁📂 [Proposta de Arquitetura](./docs/proposta.md)

A arquitetura proposta abordará a eficiência no processamento, escalabilidade e fluxo de dados entre diferentes camadas do pipeline.
Será adotada uma arquitetura modular e distribuída para lidar com grandes volumes de dados.

## 🛠⚒ Ferramentas e Técnicas

As ferramentas e técnicas escolhidas serão baseadas nos requisitos do projeto, visando a eficiência e escalabilidade.
Serão utilizadas ferramentas de ETL (Extract, Transform, Load) para o processamento dos dados, bem como tecnologias de armazenamento e processamento distribuído.

## 👨🏽‍💼⛑👷🏽‍♂️👷🏽‍♀️ Como executar o projeto?:
Você pode copiar e  colar os comandos ou simplesmente executar em ordem.

* Execução local:
    ```Bash
    # Primeiro instalar as dependências Pipy em ambiente local:
    ## COM PIPENV ##
    python -m pip install pipenv # Após instalar o pipenv se já não tiver instalado localmente
    python -m pipenv install -r requirements.txt

    ## COM PIP PURO ##
    python -m pip upgrade pip
    python -m pip install -r requirements # A vantagem do pipenv é que ele cria um ambiente dedicado ao projeto.

    # Após instalar as dependências basta executar o script `main.py`
    python ./main.py
    ```
* Execução Docker
    ```bash
    # O Docker é uma engine que cria e orquestra uma imagem de uma pseudo-máquina virtual em linux 

    ## WSL ##
    docker build -t job_spark-i --build-arg BUILD_DATE=$(date -u +"%Y-%m-%dT%H:%M:%SZ") -f Dockerfile .

    ## PowerShell ##
    docker build -t job_spark-i --build-arg BUILD_DATE=$(Get-Date -Format "yyyy-MM-ddTHH:mm:ssZ") -f Dockerfile .

    #Docker Run - Cria e executa um container docker
    docker run -it  --name job_spark-c job_spark-i:latest
    #Docker Run - Cria e executa um container docker mapeando a porta
    docker run -p 4040:4040 -dit  --name job_spark-c job_spark-i:latest
    #Docker Run - Cria e executa um container docker mapeando a porta e o volume de execução
    docker run -p 4040:4040 -dit --name job_spark-c -v .:./app job_spark-i:latest
    ```
* Executando na nuvem AWS - exemplo
    ```bash
    ## WSL ##
    docker build -t job_spark-i --build-arg BUILD_DATE=$(date -u +"%Y-%m-%dT%H:%M:%SZ") -f Dockerfile .

    ## PowerShell ##
    docker build -t job_spark-i --build-arg BUILD_DATE=$(Get-Date -Format "yyyy-MM-ddTHH:mm:ssZ") -f Dockerfile .

    # Para fazer o upload da imagem para o ECR, siga estes passos:

    # Crie um repositório ECR na AWS Console ou usando o AWS CLI.
    # Faça login no ECR usando o comando: 
    aws ecr get-login-password --region sa-east-1 | docker login --username AWS --password-stdin id_da_conta.dkr.ecr.regiao.amazonaws.com. 

    # Substitua  "id_da_conta" pelo ID da sua conta AWS.
    # Marque a imagem que você construiu com o comando: 
    docker tag job_spark-i:`tag` `id_da_conta`.dkr.ecr.regiao.amazonaws.com/`nome_do_repositorio`:`tag`. 

    # Substitua "tag" pela tag da imagem, 
    # "id_da_conta.dkr.ecr.regiao.amazonaws.com" pelo endpoint do seu repositório ECR e "`nome_do_repositorio`:`tag`" pelo nome do repositório ECR e a tag desejada.
    # Faça o push da imagem para o ECR com o comando: 
    docker push `id_da_conta`.dkr.ecr.sa-east-1.amazonaws.com/`nome_do_repositorio`:`tag`.

    # Configure o ECS para consumir a imagem do ECR e depois crie uma Definição de Tareça vingulada ao container e a imagem
    # Após isso basta definir um CRON e aguardar a execução
    ```

## ℹ Considerações Adicionais

Para projetos futuros, será importante considerar a implementação de técnicas avançadas de análise de dados, como machine learning,
para oferecer insights mais precisos sobre o comportamento de compra dos clientes.

## 🧑🏽 Colaboradores

Este projeto foi criado por:

- Gustavo H Martins ([GitHub](https://github.com/Gustavo-H-Martins) | [LinkedIn](https://www.linkedin.com/in/gustavo-henrique-lopes-martins-361789192/))
