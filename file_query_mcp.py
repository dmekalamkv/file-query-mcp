from mcp.server.fastmcp import FastMCP
from typing import Any
import httpx
import json
import mcp
import os
import duckdb
import polars as pl
import gc
import pandas as pd
import pyarrow
import pydantic
from pydantic import BaseModel, Field
from typing import Dict


mcp = FastMCP("file_query_mcp")
con = duckdb.connect(database=':memory:')

#--------------input classes-------------------

class FileSchemaOverride(BaseModel):  
    file_name: str
    schema_override_input: Dict[str,str]  # Example: "column1: int, column2: str"

#-------------------------------


@mcp.tool()
def list_data_files(path:str) -> str:
    """List all data files available for querying given a folder path"""
    data_files = {}
    for root, dirs, files in os.walk(path):
        for file in files:
            if file.endswith(('.csv', '.json', '.xlsx', '.paraquet')):
                file_name = file.replace('.', '_').replace('-', '_')
                file_table_name = f"_{file_name}"
                data_files[file] = {"path":os.path.join(root, file), "table_name": file_table_name}


    if not data_files:
        json.dump({"error": "No data files found"}, open("data_files.json", "w"), indent=4)
        
    else:
        json.dump(data_files, open("data_files.json", "w"), indent=4)

    names_list = list(data_files.keys())
    all_file_names_str = "\n".join(names_list)
    return all_file_names_str

@mcp.tool()
def list_file_schema(file_names_list: list) -> str:
    """List the schema of all given data files. this tool also loads the data files into memory for querying."""
    # Check if schema cache exists and load it
    if os.path.exists("schema_descriptions.json"):
        with open("schema_descriptions.json", "r") as f:
            schema_descriptions = json.load(f)
    else:
        schema_descriptions = {}
    if os.path.exists("data_files.json"):
        with open("data_files.json", "r") as f:
            data_files = json.load(f)
    for file in file_names_list:
        if file not in data_files.keys():
            schema_descriptions[file] = f"File {file} not found.\n"
            continue
        path = data_files[file]["path"]
        table_name = data_files[file]['table_name']
        try:
            if file not in schema_descriptions.keys():
                if file.endswith('.csv'):
                    df = pl.read_csv(path)
                    con.register(table_name, df)
                elif file.endswith('.json'):
                    df = pl.read_json(path)
                    con.register(table_name, df)
                elif file.endswith('.xlsx'):
                    df = pl.read_excel(path)
                    con.register(table_name, df)
                elif file.endswith('.parquet'):
                    df = pl.read_parquet(path)
                    con.register(table_name, df)

                else:
                    schema_descriptions["file"] = f"Unsupported file format for {file}.\n"
                    continue
                schema_descriptions[file] = f"""
                Schema for {file}:\n{df.schema}\n\n
                Descriptive statistics for {file}:\n{df.describe()}\n\n
                Top 5 rows of {file}:\n{df.head(5).to_pandas().to_string()}\n
                --- \n"""
                del df
                gc.collect()
            json.dump(schema_descriptions, open("schema_descriptions.json", "w"), indent=4)
        except Exception as e:
            schema_descriptions[file] = f"Error reading {file}: {str(e)}\n"
            with open(data_files[file]["path"], "r") as f:
                first_500 = f.read(500)
            schema_descriptions[file] += f"First 500 characters of the file:\n{first_500}\n---\n to help with schema inference and override"
            
    output_string =  "\n".join([schema_descriptions[f] for f in file_names_list if f in schema_descriptions.keys()])
    return output_string

@mcp.tool()
def load_override_schema(schema_json: FileSchemaOverride) -> str:
    """Load data files with an override schema. schema_overide is a list of strings like "column1: int, column2: str" (and other polars dataframes) use this to force certain columns to be certain types when the list_file_schema tool fails to infer the correct types.
    The follwing type keywords are supported: int, float, str/string, bool, date, datetime
    """
    if os.path.exists("data_files.json"):
        with open("data_files.json", "r") as f:
            data_files = json.load(f)   
    if os.path.exists("schema_descriptions.json"):
        with open("schema_descriptions.json", "r") as f:
            schema_descriptions = json.load(f)
    else:
        schema_descriptions = {}
    if schema_json.file_name not in data_files.keys():
        return f"Error: File {schema_json.file_name} not found in loaded data files."
    path = data_files[schema_json.file_name]["path"]
    table_name = data_files[schema_json.file_name]['table_name']
    schema_override = {}
    for col, dtype in schema_json.schema_override_input.items():
        if dtype.lower() == "int":
            schema_override[col] = pl.Int64
        elif dtype.lower() == "float":
            schema_override[col] = pl.Float64
        elif dtype.lower() == "str" or dtype.lower() == "string":
            schema_override[col] = pl.Utf8
        elif dtype.lower() == "bool":
            schema_override[col] = pl.Boolean
        elif dtype.lower() == "date":
            schema_override[col] = pl.Date
        elif dtype.lower() == "datetime":
            schema_override[col] = pl.Datetime
        else:
            return f"Error: Unsupported data type {dtype} for column {col}."
    try:
        if schema_json.file_name.endswith('.csv'):
            df = pl.read_csv(path, dtypes=schema_override)
            con.register(table_name, df)
        elif schema_json.file_name.endswith('.json'):
            df = pl.read_json(path, dtypes=schema_override)
            con.register(table_name, df)
        elif schema_json.file_name.endswith('.xlsx'):
            df = pl.read_excel(path, dtypes=schema_override)
            con.register(table_name, df)
        elif schema_json.file_name.endswith('.parquet'):
            df = pl.read_parquet(path, dtypes=schema_override)
            con.register(table_name, df)
        else:
            return f"Error: Unsupported file format for {schema_json.file_name}."
        schema_descriptions[schema_json.file_name] = f"""
        Schema for {schema_json.file_name} with override:\n{df.schema}\n\n
        Descriptive statistics for {schema_json.file_name}:\n{df.describe()}\n\n
        Top 5 rows of {schema_json.file_name}:\n{df.head(5).to_pandas().to_string()}\n
        --- \n"""
        json.dump(schema_descriptions, open("schema_descriptions.json", "w"), indent=4)
        del df
        gc.collect()
        return f"""Successfully loaded {schema_json.file_name} with override schema.
        Here is the schema:\n{schema_descriptions[schema_json.file_name]}
        """
    except Exception as e:
        return f"Error loading {schema_json.file_name} with override schema: {str(e)}"
    

@mcp.tool()
def query_files(raw_query: str) -> str:
    """Run a SQL query against the loaded data files. The table names are just the file names so just use them directly.
    example: SELECT * FROM abcd.csv WHERE column1 > 100
    """
    words = raw_query.split()
    if os.path.exists("data_files.json"):
        with open("data_files.json", "r") as f:
            data_files = json.load(f)
    for file_name in data_files.keys():
        if file_name in words:
            raw_query = raw_query.replace(file_name, data_files[file_name]['table_name'])
            continue
        path = data_files[file_name]["path"]
        if path in words:
            if not os.path.exists(path):
                return f"Error: File {path} does not exist."
            raw_query = raw_query.replace(f"{path}", data_files[file_name]['table_name'])
            continue
        

    query = raw_query
    try:
        result_df = con.execute(query).df()
        result_str = result_df.to_string()
        return result_str
    except Exception as e:
        return f"Error executing query: {str(e)}"


if __name__ == "__main__":
    # Initialize and run the server
    mcp.run(transport='stdio')