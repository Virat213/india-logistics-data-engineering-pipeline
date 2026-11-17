# Indian Logistics Data Pipeline: Cloud & Container Deployment Guide

**Target Environment:** Local Docker Compose or Low-Cost Cloud VM (AWS EC2 / GCP Compute Engine / Azure VM)  
**Estimated Cloud Cost:** Free-Tier Eligible or < \$15–\$20/month  

---

## 1. Architecture Overview

```
                      +---------------------------------------+
                      |       Host System / Cloud VM         |
                      |   (Ubuntu 22.04 LTS or Windows 11)   |
                      +-------------------+-------------------+
                                          |
        +---------------------------------+---------------------------------+
        |                                 |                                 |
+-------v-------+                 +-------v-------+                 +-------v-------+
|  Apache Kafka |                 |  Apache Spark |                 | Apache Airflow|
|  & Zookeeper  |                 | Master & Node |                 | Web & Sched   |
| (Ports: 9092) |                 | (Ports: 8081) |                 | (Ports: 8085) |
+---------------+                 +---------------+                 +---------------+
        |                                 |                                 |
        +---------------------------------+---------------------------------+
                                          |
                      +-------------------v-------------------+
                      |      Persistent Storage Volumes       |
                      |   ./data/bronze   ./data/silver       |
                      |   ./data/gold     ./hive/warehouse    |
                      +---------------------------------------+
```

---

## 2. Option A: Local Docker Deployment

### Prerequisites:
- Docker Desktop (Windows / macOS) or Docker Engine (Linux).
- At least 4GB RAM allocated to Docker.

### Execution Steps:
1. **Clone the repository and navigate to root:**
   ```bash
   cd india-logistics-pipeline
   ```
2. **Create environment file from template:**
   ```powershell
   Copy-Item .env.example .env
   # or on Linux/macOS:
   cp .env.example .env
   ```
3. **Build and launch containerized services in detached mode:**
   ```bash
   docker compose up -d --build
   ```
4. **Verify container health:**
   ```bash
   docker compose ps
   ```
   You should see:
   - `logistics-zookeeper` (Running)
   - `logistics-kafka` (Running)
   - `logistics-spark-master` (Running)
   - `logistics-spark-worker` (Running)
   - `logistics-postgres` (Running)
   - `logistics-airflow-webserver` (Running on port 8085)
   - `logistics-airflow-scheduler` (Running)

5. **Access Service Interfaces:**
   - **Airflow Web UI:** Navigate to `http://localhost:8085`  
     *Credentials:* Username: `admin` | Password: `admin`
   - **Spark Master Web UI:** Navigate to `http://localhost:8081`

6. **Trigger the Pipeline DAG in Airflow:**
   - On the Airflow dashboard, locate `india_logistics_batch_etl`.
   - Toggle the DAG status to **Active** (blue).
   - Click the **Trigger DAG** (Play button) to run the batch ETL pipeline.

7. **Shutdown & Cleanup:**
   ```bash
   # Stop containers while preserving data volumes:
   docker compose down

   # Stop and wipe persistent volumes:
   docker compose down -v
   ```

---

## 3. Option B: Low-Cost Cloud VM Deployment (AWS / GCP / Azure)

For a university final-year project, you can host the pipeline on an affordable single-node cloud VM without needing Kubernetes or paid managed services.

### Recommended VM Sizing:
- **AWS:** `t3.large` or `t3a.large` (2 vCPU, 8 GB RAM) — Spot instance: ~\$0.025/hr (~\$18/month).
- **Google Cloud Platform:** `e2-standard-2` (2 vCPU, 8 GB RAM) — Eligible for \$300 free credits.
- **Azure:** `Standard_B2ms` (2 vCPU, 8 GB RAM).
- **Storage:** 40 GB General Purpose SSD (gp3/pd-balanced).
- **OS:** Ubuntu 22.04 LTS (Jammy Jellyfish).

### Cloud Security Group / Inbound Firewall Rules:
Configure the following inbound port rules in your cloud firewall:
| Port | Protocol | Purpose | Source |
| :--- | :--- | :--- | :--- |
| `22` | TCP | SSH Administration | My IP only |
| `8085` | TCP | Apache Airflow Webserver | My IP or Restricted |
| `8081` | TCP | Apache Spark Master UI | My IP or Restricted |
| `9092` | TCP | Kafka Client Listener | VPC / localhost |

### Step-by-Step Cloud VM Setup:
```bash
# 1. Connect to VM via SSH
ssh -i "your-key.pem" ubuntu@<YOUR_VM_PUBLIC_IP>

# 2. Update packages and install Docker Engine & Docker Compose
sudo apt-get update && sudo apt-get upgrade -y
sudo apt-get install -y ca-certificates curl gnupg lsb-release
sudo mkdir -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

# 3. Add ubuntu user to docker group
sudo usermod -aG docker $USER
newgrp docker

# 4. Clone project repository
git clone <YOUR_GITHUB_REPOSITORY_URL>
cd india-logistics-pipeline

# 5. Configure environment
cp .env.example .env
sed -i 's/localhost:9092/kafka:29092/g' .env

# 6. Start the pipeline
docker compose up -d --build

# 7. Check logs
docker compose logs -f airflow-webserver
```

---

## 4. Monitoring & Troubleshooting

### Viewing Service Logs:
```bash
# View Airflow scheduler logs:
docker compose logs -f airflow-scheduler

# View Kafka event logs:
docker compose logs -f kafka

# View Spark master logs:
docker compose logs -f spark-master
```

### Inspecting Kafka Topics inside Container:
```bash
# List Kafka topics:
docker exec -it logistics-kafka kafka-topics --bootstrap-server localhost:9092 --list

# Consume live messages from the shipment events topic:
docker exec -it logistics-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic india.logistics.shipment_events \
  --from-beginning \
  --max-messages 10
```

### Common Troubleshooting Scenarios:
1. **Port 8080 Conflict:**  
   The Airflow webserver host port is explicitly remapped to **`8085`** in `docker-compose.yml` to avoid conflicting with existing local web servers or Jenkins.
2. **Out-of-Memory (OOM) on Small VMs:**  
   If deploying on a 4 GB RAM VM, add a 4 GB swap file:
   ```bash
   sudo fallocate -l 4G /swapfile
   sudo chmod 600 /swapfile
   sudo mkswap /swapfile
   sudo swapon /swapfile
   ```
3. **Resetting Airflow Database:**  
   If DAG definitions fail to update, trigger `docker compose restart airflow-scheduler airflow-webserver`.
