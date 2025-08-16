# OC2PG
Oracle Database to PostgresSQL database migration tool for UQCS Hackathon 2025.

By **Youssef Hassan** and **Abdallah Azazy**.

---

## Overview
OC2PG is a prototype migration tool that automates the process of moving database schemas and data from Oracle to PostgreSQL.  
It handles:
- Oracle schema introspection  
- DDL translation and application in PostgreSQL  
- Bulk data migration
- Row-count validation  

This tool is designed to be lightweight and easy to extend for production use.

---

## Installation

### Prerequisites
  - Python 3.10+
  - [Oracle Instant Client](https://www.oracle.com/database/technologies/instant-client.html)
  - PostgreSQL 14+ running locally or remotely
### Setup
Clone the repository and set up a virtual environment:

```bash
git clone https://github.com/<your-username>/oc2pg.git
cd oc2pg
python3 -m venv .venv
source .venv/bin/activate
```

---
## Configuration

Database connection details are provided via a simple config file or CLI parameters.
### Postgres Example:
```bash
host: localhost
port: 5432
user: postgres
pass: postgres
database: migration_target
```
### Oracle Example:
```bash
host: localhost
port: 1521
service: XE
user: system
pass: oracle
```
---
## Usage

Run the migration with:
```bash
python3 src/cli.py migrate \
  --owner HR \
  --oracle-dsn "localhost:1521/XE" \
  --oracle-user system \
  --oracle-password oracle \
  --pg-dsn "postgresql://postgres:postgres@localhost:5432/migration_target" \
  --pg-schema public
```
and for more specific commands run the help command:
```bash
python3 src/cli.py --help
```

The steps include:
- Discover Oracle schema
- Emit Postgres DDL
- Apply DDL on Postgres
- Bulk copy table data
- Validate row counts

To run the demo program, execute the command:
```bash
python3 -m streamlit run demo/demo.py
```
in the [oc2pg] directory of the project. This should start a localhost server, which you can connect to via a web browser, allowing for GUI interaction with the tool.

---
## AI-usage

This progam's testing is written partially by the LLM ChatGPT-5. Namely, these are the files under the [test](https://github.com/GuardianCoding/oc2pg/tree/main/test) directory of the project using the promt:
"test this function in the same format example that i have above..."

This program's demo program is partially written by the LLM ChatGPT-5. Namely, these are the files under the [demo](https://github.com/GuardianCoding/oc2pg/tree/main/demo) directory of the project.

This program's example bash scripts that produce dockers with sample Oracle and PostgresSQL databases are partially written by the LLM ChatGPT-5. Namely, these are the files under the [examples] (https://github.com/GuardianCoding/oc2pg/tree/main/examples) directory of the project. 




