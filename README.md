# file-query-mcp

this is a MCP sever that can connect to any data sources like csv, excel, paraquet or any other data format and query them on requirement

## Overview

`file-query-mcp` is an MCP (Model Context Protocol) server designed to address the challenges faced by data scientists and developers who work with large datasets spread across multiple projects. This tool allows users to query their datasets, perform joins, and extract insights using natural language queries. It seamlessly integrates with your development environment, making it a powerful addition to your data analysis workflow.

### Problem Statement

As a data scientist, managing and querying data files across different projects can be cumbersome. Often, datasets are stored in various formats like CSV, Excel, or Parquet, and answering questions about these datasets requires significant effort to load, process, and query them. `file-query-mcp` solves this problem by providing a unified interface to query and join datasets on demand, using natural language queries.

### Features

- Connects to various data sources, including CSV, Excel, Parquet, and more.
- Allows querying and joining datasets using natural language.
- Automatically detects available data sources in your project.
- Integrates easily with your development environment.

### How to Use

1. **Clone the Repository**

   ```bash
   git clone <repository-url>
   cd file-query-mcp
   ```

2. **Install Dependencies**
   Ensure you have `uv` and other required dependencies installed. If not, install them using:

   ```bash
   pip install -r requirements.txt
   ```

3. **Run the MCP Server**
   Use the following command to start the server:

   ```bash
   uv --directory <folder-location-that-holds-file_query_mcp.py> run file_query_mcp.py
   ```

4. **Integrate with GitHub Copilot Chat Agent**
   - Add the MCP tool to your GitHub Copilot agent tools list as an MCP server.
   - Once added, the server will automatically detect all available data sources in your repository.
   - You can then query and join tables on demand to get your results.

Your final setting should look like this

```settings.json <vscode>
"mcp": {
    "servers": {
      "file_query_mcp_v0": {
        "type": "stdio",
        "command": "uv",
        "args": [
          "--directory",
          "<location of local file-query-mcp repo>",
          "run",
          "file_query_mcp.py"
        ]
      }
    }
```

### Example Use Case

Suppose you have multiple datasets in your project folder, such as `sales.csv`, `customers.xlsx`, and `products.parquet`. With `file-query-mcp`, you can:

- Query individual datasets to extract specific information.
- Perform joins between datasets (e.g., join `sales` and `customers` on `customer_id`).
- Use natural language queries like "Show me the total sales by product category."

### Why Use `file-query-mcp`?

- **Efficiency**: Save time by querying datasets directly without manual preprocessing.
- **Flexibility**: Supports multiple data formats and natural language queries.
- **Integration**: Works seamlessly with your development environment and GitHub Copilot.

`file-query-mcp` is your go-to solution for managing and querying datasets effortlessly. Start using it today to simplify your data analysis workflow!
