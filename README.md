
# **RemoteERPConnector**

## **Project Overview**
The **RemoteERPConnector** is an ETL (Extract, Transform, Load) pipeline designed to connect to a remote ERP system, extract data, process it, and load it into a target database or system for further analysis and reporting. The project uses SSH tunneling to securely connect to a remote MariaDB ERP database and processes the data using PySpark.

---

## **Features**
- Secure SSH tunneling for connecting to a remote ERP database.
- Data extraction from the ERP system (MariaDB) using SQL queries.
- Data transformation and processing using PySpark.
- Ability to save the processed data to various formats (e.g., CSV, database).

---

## **Technologies Used**
- **Python**: Programming language used for the ETL logic and automation.
- **PySpark**: Framework for large-scale data processing and transformation.
- **MariaDB**: Database system for the ERP.
- **SSHTunnel**: Library for creating an SSH tunnel to securely access the remote database.
- **pymysql**: Library to interact with the MariaDB database.

---

## **Prerequisites**
Before using the project, ensure the following dependencies are installed:

- Python 3.x
- PySpark
- pymysql
- sshtunnel

---

## **Setup**
### 1. **SSH Tunnel Configuration**
Update the SSH and database configuration with your credentials and connection details:
```python
remote_host = ""                             # Your remote server IP
remote_port = 3306                           # MariaDB port
local_port = 3309                            # Local port to forward
ssh_username = "root"                        # Replace with your SSH username
ssh_private_key = "/path/to/your/private/key" # Path to your private key file
db_config = {
    "host": "127.0.0.1",                 # Use localhost for the tunnel
    "user": "root",                     # Your MariaDB username
    "password": "your_password",        # Your MariaDB password
    "database": "      ",              # The database you want to connect to
    "port": local_port
}
```

### 2. **PySpark Setup**
Ensure that PySpark is correctly installed and configured. If you're using a local setup, Spark will run locally. If you're using a Spark cluster, adjust the `SparkSession` configuration.


## **License**
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
