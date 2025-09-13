# 📊 RabbitMQ Consumer: PostgreSQL Insights → CSV → S3

This project implements a **RabbitMQ consumer service** that:

1. **Consumes jobs** from a RabbitMQ queue.  
2. **Pulls data** from a PostgreSQL database based on the job payload.  
3. **Generates insights** and transforms the results into a structured **CSV file**.  
4. **Uploads the CSV file to Amazon S3**.  
5. Provides a **download link (presigned URL)** so clients can securely retrieve the file.

---

## ⚙️ Architecture

- **Producer** publishes a job (JSON payload) → `generate_csv` queue.  
- **Consumer** receives job, runs a query against **PostgreSQL**, generates insights.  
- Results are saved as `.csv` and uploaded to **S3**.  
- A **presigned S3 URL** is returned to the client for download.  

---

## 📂 Project Structure

.
├── Config/
│   ├── Client.py
│   ├── RabbitMQ.py
│   └── PostgresClient.py
├── Parser/
│   ├── StudentParser.py
│   ├── TutorParser.py
│   └── test
├── S3/
│   └── main.py
├── main.py  
├── Dockerfile
├── Makefile
├── Requirements.txt 
└── README.md


---

## 🔧 Setup

### 1. Requirements

- Python 3.9+
- RabbitMQ server
- PostgreSQL
- AWS S3 bucket

Install dependencies:

```bash
pip install -r requirements.txt