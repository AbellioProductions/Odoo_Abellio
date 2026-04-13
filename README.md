# Odoo MES / ERP Core

A Manufacturing Execution System (MES) built on Odoo 17, designed to aggregate machine telemetry, generate PQO (Product Quality Output) reports, and automate maintenance workflows via MaintainX.

##  Core Objectives:
* **Data Consolidation:** Merging high-frequency machine telemetry with manual operator packing reports into a single Odoo environment.
* **Operator Workspace:** Optimized interface for production task management and real-time equipment monitoring.
* **Hybrid Storage Architecture:** Separation of ERP transactional data (PostgreSQL) and telemetry time-series data (TimescaleDB) to ensure system stability under high load.

##  Technical Stack & Architecture

### 1. Telemetry & TimescaleDB
Machine data collection is handled via IPC + pyads, writing directly to TimescaleDB.
* **PostgreSQL FDW:** Odoo accesses telemetry data using Foreign Data Wrappers, allowing it to treat telemetry as standard Odoo tables without taxing the primary ERP database.
* **Language:** Python 3.10, XML
* **CSV Import:** Integrated wizards support manual raw data uploads to TimescaleDB to maintain historical integrity when automated streams are interrupted.

### 2. Legacy Systems Integration (Gemba OEE)
During the migration phase, the system maintains a direct connection to external MS SQL (Impact Connect) databases using pyodbc. This ensures seamless synchronization of shifts and alarm events from the legacy environment.

### 3. MaintainX Automation
Bi-directional Work Order synchronization via REST API.

* **OCA Queue Job:** All API interactions are handled asynchronously to prevent UI lag and ensure reliable data delivery regardless of order volume.
* **Traffic Prioritization:** Dedicated job channels manage MaintainX traffic to prevent bottlenecks.

---

##    Installation & Setup
This project utilizes a microservices architecture with full network isolation for internal components. Only ports 80 and 443 (Nginx) are exposed to the outside. Access to the databases and Odoo is strictly internal via the Docker network.

### Host Server Preparation
* Ensure Docker and Docker Compose are installed on the host.
* VPN (Crucial): Configure the VPN client directly on the host machine (server) so it has routing and access to the IP addresses of the PLC equipment (TwinCAT). The telemetry_worker container will use the host's network interface to poll the machines.

###  Cloning & Environment Setup
Clone the repository and navigate to its directory. 
```bash
git clone --recursive https://github.com/ConstantineManychev/MyOdooMES_ERP.git
cd MyOdooMES_ERP
```
Create the working configuration file based on the provided template:
```bash
cp ".env temp" .env
```
Open the .env file and set secure passwords for the databases (Odoo PostgreSQL and TimescaleDB), as well as system variables.

If deploying in production with a bound domain:
Open the init-letsencrypt.sh file and set the domain and email variables to your actual data.
In the nginx/nginx.conf file, uncomment the server block for port 443.
Run the automated SSL certificate retrieval script:
```bash
chmod +x init-letsencrypt.sh
sudo ./init-letsencrypt.sh
```

Launch the architecture. The --build flag is required on the first run or after any code changes in twincat_poller to ensure Docker builds the image with dependencies for the telemetry_worker:
```bash
docker-compose up -d --build
```

For the telemetry_worker to write telemetry and Odoo to read it, you must initialize the hypertables and configuration dictionaries.
Apply the database schema from the migration (replace timescaledb_container_name with your actual container name from docker-compose ps):
* Using the script from the repository:
```bash
bash init_db/timescale_db_setup.sh
```
* OR applying the SQL file directly:
```bash
cat db_migrations/V1__init_schema.sql | docker exec -i timescaledb_container_name psql -U <DB_USER from .env> -d <DB_NAME from .env>
```

###  Install the Module


```bash
docker-compose exec odoo odoo -i mes_core -d Odoo --db_host=db --db_user=odoo --db_password=odoo --stop-after-init
```
(Note: If you are reinstalling after a refactor, you may need to update the module list first: docker-compose exec odoo odoo -u base ...)


##    Project Structure
Plaintext

```text
custom_addons/
└── mes_core/               # Main Module
    ├── models/
    │   ├── mes_machine_performance.py  # OEE Logic
    │   ├── mes_production_report.py    # Packing/QC Logic
    │   ├── mes_tasks.py                # Task Logic
    │   └── mes_dictionaries.py         # Shifts, Defects, etc.
    ├── views/
    │   ├── mes_menus.xml               # Menu Hierarchy
    │   └── ...                         # UI Definitions
    ├── wizard/
    │   └── external_import_wizard.py   # SQL Import Logic
    └── security/                       # Access Rights (User/Manager)
```

##    License
LGPL-3
