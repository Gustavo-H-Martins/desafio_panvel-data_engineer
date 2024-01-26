import os
import json

# Mapeia e carrega os parâmetros no dicionário json
file_json = os.path.abspath(os.path.join(os.path.dirname(__file__), ".." , "config/spark_jobs.json"))

with open(file=file_json, mode="r") as dict_file:
    config = json.loads(dict_file.read())
    dict_file.close()

# Parâmetros
upsert:bool = True
tables_config_dict:dict = config.get("tables_config_dict", {})
datalake_paths:dict = config.get("datalake_paths", {})
transform_data:dict = config.get("transform_data", {})
gold_operations:dict = config.get("gold_operations", {})
transient_config_dict:dict = config.get("transient_config_dict", {})
