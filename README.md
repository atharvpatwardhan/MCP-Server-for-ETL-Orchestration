# 🧠 MCP Server for ETL Orchestration

> **Natural Language-Powered ETL Workflows using Airflow, AWS Glue, Athena, and S3**

This project implements a **Model Context Protocol (MCP)**-compliant server that exposes a powerful set of ETL orchestration tools to LLM agents (like Claude or GPT), enabling them to control, monitor, and interact with real-world data infrastructure using natural language.

---

## 🚀 Features

- 🛰️ **Airflow Integration**  
  Trigger DAGs, monitor their status, and list available workflows.

- 🪣 **S3 Tools**  
  Create buckets, upload files, delete buckets — programmatically or via LLM prompts.

- 🧬 **AWS Glue Integration**  
  Start jobs, track job runs, fetch logs, and view available ETL scripts.

- 🔍 **Athena Query Engine**  
  Execute SQL queries on S3 data, poll for status, fetch results, and list catalog metadata.

- 🧠 **LLM-Native Tool Interface**  
  Fully MCP-compliant interface for Claude, GPT, and other AI assistants to programmatically operate the stack using natural language.

---

## 🛠️ Available Tools

### 📌 Airflow

- Trigger DAGs
- Check DAG status
- List available DAGs with status

### 📌 S3

- Create an S3 bucket
- Upload a file to a bucket
- Delete an S3 bucket (with optional object cleanup)

### 📌 Glue

- Run a Glue job with optional arguments
- Check Glue job run status
- Fetch Glue job logs
- List all available Glue jobs

### 📌 Athena

- Run SQL queries on Athena with configurable output location
- Check query execution status
- Fetch query results
- List available databases
- List tables in a specific database

---

## ⚙️ Setup

### 1. Clone the Repository and Install Dependencies

```bash
git clone https://github.com/atharvpatwardhan/mcp-etl-orchestrator.git
cd mcp-etl-orchestrator
pip install -r requirements.txt
```

### 2. Configure Environment Variables

Create a `.env` file in the root directory and populate it with your AWS credentials:

```dotenv
# AWS Credentials
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
AWS_DEFAULT_REGION=your-aws-region
```

### 3. Update Airflow Credentials in tools/airflow_config.py (optional)

# Airflow API Configuration

```
AIRFLOW_API_BASE=http://localhost:8080/api/v1
AIRFLOW_USERNAME=admin
AIRFLOW_PASSWORD=admin
```

### 4. Start the MCP Server

```bash
python main.py
```

Once the server is running, connect your Claude Desktop or any MCP-compatible client to the server and begin using the tools with natural language commands!
