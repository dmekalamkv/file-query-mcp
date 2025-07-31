# MCP (Model Context Protocol) Server for File Query Operations
# This server provides tools to discover, load, and query data files using SQL
# Supports CSV, JSON, Excel, and Parquet file formats

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

# Initialize MCP server and in-memory DuckDB connection
mcp = FastMCP("file_query_mcp")
con = duckdb.connect(database=':memory:')  # In-memory database for fast querying

#--------------Input Classes-------------------
# Pydantic models for type validation and structure

class FileSchemaOverride(BaseModel):  
    """Model for overriding column data types when loading files
    
    Attributes:
        file_name: Name of the file to apply schema override
        schema_override_input: Dictionary mapping column names to data types
                              Example: {"column1": "int", "column2": "str"}
    """
    file_name: str
    schema_override_input: Dict[str,str]  # Example: "column1: int, column2: str"

#-------------------------------

@mcp.tool()
def list_data_files(path: str) -> str:
    """Discover and catalog all data files in a directory tree
    
    Recursively walks through the given path to find supported data files
    and creates a mapping of file names to their paths and table names.
    
    Args:
        path: Root directory path to search for data files
        
    Returns:
        String containing newline-separated list of discovered file names
        
    Side Effects:
        Creates/updates 'data_files.json' with file catalog information
    """
    data_files = {}
    
    # Walk through directory tree to find data files
    for root, dirs, files in os.walk(path):
        for file in files:
            # Check for supported file extensions
            if file.endswith(('.csv', '.json', '.xlsx', '.paraquet')):
                # Create sanitized table name (replace special chars with underscores)
                file_name = file.replace('.', '_').replace('-', '_')
                file_table_name = f"_{file_name}"
                
                # Store file metadata
                data_files[file] = {
                    "path": os.path.join(root, file), 
                    "table_name": file_table_name
                }

    # Handle case when no files are found
    if not data_files:
        json.dump({"error": "No data files found"}, open("data_files.json", "w"), indent=4)
    else:
        # Save catalog to JSON file for persistence
        json.dump(data_files, open("data_files.json", "w"), indent=4)

    # Return user-friendly list of file names
    names_list = list(data_files.keys())
    all_file_names_str = "\n".join(names_list)
    return all_file_names_str

@mcp.tool()
def list_file_schema(file_names_list: list) -> str:
    """Analyze file schemas and load data into memory for querying
    
    For each file, extracts schema information, descriptive statistics,
    and sample data. Also registers the data with DuckDB for SQL querying.
    
    Args:
        file_names_list: List of file names to analyze
        
    Returns:
        String containing formatted schema information for all files
        

    """
    # Side Effects:
    # - Loads data files into DuckDB memory
    # - Creates/updates 'schema_descriptions.json' cache
    # Load existing schema cache if available
    if os.path.exists("schema_descriptions.json"):
        with open("schema_descriptions.json", "r") as f:
            schema_descriptions = json.load(f)
    else:
        schema_descriptions = {}
    
    # Load file catalog
    if os.path.exists("data_files.json"):
        with open("data_files.json", "r") as f:
            data_files = json.load(f)
    
    # Process each requested file
    for file in file_names_list:
        # Check if file exists in catalog
        if file not in data_files.keys():
            schema_descriptions[file] = f"File {file} not found.\n"
            continue
            
        path = data_files[file]["path"]
        table_name = data_files[file]['table_name']
        
        try:
            # Skip if schema already cached
            if file not in schema_descriptions.keys():
                # Load file based on extension using Polars
                if file.endswith('.csv'):
                    df = pl.read_csv(path)
                elif file.endswith('.json'):
                    df = pl.read_json(path)
                elif file.endswith('.xlsx'):
                    df = pl.read_excel(path)
                elif file.endswith('.parquet'):
                    df = pl.read_parquet(path)
                else:
                    schema_descriptions["file"] = f"Unsupported file format for {file}.\n"
                    continue
                
                # Register dataframe with DuckDB for SQL querying
                con.register(table_name, df)
                
                # Generate comprehensive schema description
                schema_descriptions[file] = f"""
                Schema for {file}:\n{df.schema}\n\n
                Descriptive statistics for {file}:\n{df.describe()}\n\n
                Top 5 rows of {file}:\n{df.head(5).to_pandas().to_string()}\n
                --- \n"""
                
                # Clean up memory
                del df
                gc.collect()
                
            # Update schema cache
            json.dump(schema_descriptions, open("schema_descriptions.json", "w"), indent=4)
            
        except Exception as e:
            # Handle file reading errors gracefully
            schema_descriptions[file] = f"Error reading {file}: {str(e)}\n"
            
            # Provide file preview for troubleshooting
            try:
                with open(data_files[file]["path"], "r") as f:
                    first_500 = f.read(500)
                schema_descriptions[file] += f"First 500 characters of the file:\n{first_500}\n---\n to help with schema inference and override"
            except:
                pass
            
    # Compile output for requested files
    output_string = "\n".join([schema_descriptions[f] for f in file_names_list if f in schema_descriptions.keys()])
    return output_string

@mcp.tool()
def load_override_schema(schema_json: FileSchemaOverride) -> str:
    """Load data file with custom column data types
    
    When automatic schema inference fails or produces incorrect types,
    this tool allows manual specification of column data types.
    
    Args:
        schema_json: FileSchemaOverride object containing file name and type mappings
        
    Returns:
        String with success message and schema information, or error details
        
    Supported Type mapping strings:
        int, float, str/string, bool, date, datetime
    """
    # Load file catalog and schema cache
    if os.path.exists("data_files.json"):
        with open("data_files.json", "r") as f:
            data_files = json.load(f)   
    if os.path.exists("schema_descriptions.json"):
        with open("schema_descriptions.json", "r") as f:
            schema_descriptions = json.load(f)
    else:
        schema_descriptions = {}
    
    # Validate file exists
    if schema_json.file_name not in data_files.keys():
        return f"Error: File {schema_json.file_name} not found in loaded data files."
    
    path = data_files[schema_json.file_name]["path"]
    table_name = data_files[schema_json.file_name]['table_name']
    
    # Convert string type names to Polars data types
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
        # Load file with custom schema based on file type
        if schema_json.file_name.endswith('.csv'):
            df = pl.read_csv(path, dtypes=schema_override)
        elif schema_json.file_name.endswith('.json'):
            df = pl.read_json(path, dtypes=schema_override)
        elif schema_json.file_name.endswith('.xlsx'):
            df = pl.read_excel(path, dtypes=schema_override)
        elif schema_json.file_name.endswith('.parquet'):
            df = pl.read_parquet(path, dtypes=schema_override)
        else:
            return f"Error: Unsupported file format for {schema_json.file_name}."
        
        # Register with DuckDB
        con.register(table_name, df)
        
        # Update schema description with override information
        schema_descriptions[schema_json.file_name] = f"""
        Schema for {schema_json.file_name} with override:\n{df.schema}\n\n
        Descriptive statistics for {schema_json.file_name}:\n{df.describe()}\n\n
        Top 5 rows of {schema_json.file_name}:\n{df.head(5).to_pandas().to_string()}\n
        --- \n"""
        
        # Save updated schema cache
        json.dump(schema_descriptions, open("schema_descriptions.json", "w"), indent=4)
        
        # Clean up memory
        del df
        gc.collect()
        
        return f"""Successfully loaded {schema_json.file_name} with override schema.
        Here is the schema:\n{schema_descriptions[schema_json.file_name]}
        """
        
    except Exception as e:
        return f"Error loading {schema_json.file_name} with override schema: {str(e)}"

@mcp.tool()
def query_files(raw_query: str) -> str:
    """Execute SQL queries against loaded data files
        
    Args:
        raw_query: SQL query string with file names or paths as table references
                  Example: "SELECT * FROM mydata.csv WHERE column1 > 100"
        
    Returns:
        String representation of query results or error message
        
    """
    # Translates file names in SQL queries to their corresponding table names
    # and executes the query using DuckDB.
    # Note:
    # File names and paths in queries are automatically converted to table names
    words = raw_query.split()
    
    # Load file catalog for name translation
    if os.path.exists("data_files.json"):
        with open("data_files.json", "r") as f:
            data_files = json.load(f)
    
    # Replace file names and paths with table names in query
    for file_name in data_files.keys():
        # Replace direct file name references
        if file_name in words:
            raw_query = raw_query.replace(file_name, data_files[file_name]['table_name'])
            continue
            
        # Replace file path references
        path = data_files[file_name]["path"]
        if path in words:
            if not os.path.exists(path):
                return f"Error: File {path} does not exist."
            raw_query = raw_query.replace(f"{path}", data_files[file_name]['table_name'])
            continue

    query = raw_query
    
    try:
        # Execute query and return results as formatted string
        result_df = con.execute(query).df()
        result_str = result_df.to_string()
        return result_str
    except Exception as e:
        return f"Error executing query: {str(e)}"

if __name__ == "__main__":
    # Entry point: Initialize and start the MCP server
    # Uses stdio transport for communication with MCP clients
    mcp.run(transport='stdio')