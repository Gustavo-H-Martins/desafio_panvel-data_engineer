import os
import json

# Mapeia e carrega os parâmetros no dicionário json
file_json = os.path.abspath(os.path.join(os.path.dirname(__file__), ".." , "config/spark_jobs.json"))

with open(file=file_json, mode="r") as dict_file:
    config = json.loads(dict_file.read())
    dict_file.close()

# Parâmetros
upsert = True
tables_config_dict = config.get("tables_config_dict", {})
datalake_paths = config.get("datalake_paths", {})
transform_data = config.get("transform_data", {})
consumer_operations = config.get("consumer_operations", {})
transient_config_dict = config.get("transient_config_dict", {})

# Print para ver se tem algo
# print(datalake_paths)
