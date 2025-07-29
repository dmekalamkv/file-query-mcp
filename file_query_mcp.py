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


mcp = FastMCP("file_query_mcp")
con = duckdb.connect(database=':memory:')




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
    """List the schema of all given data files."""
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
                    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{path}')")
                elif file.endswith('.json'):
                    df = pl.read_json(path)
                    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_json_auto('{path}')")
                elif file.endswith('.xlsx'):
                    df = pl.read_excel(path)
                    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_excel_auto('{path}')")
                elif file.endswith('.parquet'):
                    df = pl.read_parquet(path)
                    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{path}')")
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
    output_string =  "\n".join([schema_descriptions[f] for f in file_names_list if f in schema_descriptions.keys()])
    return output_string


@mcp.tool()
def query_files(raw_query: str) -> str:
    """Run a SQL query against the loaded data files."""
    words = raw_query.split()
    if os.path.exists("data_files.json"):
        with open("data_files.json", "r") as f:
            data_files = json.load(f)
    for file_name in data_files.keys():
        if file_name in words:
            raw_query = raw_query.replace(file_name, data_files[file_name]['table_name'])
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