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

```


### 2. Env 
# RabbitMQ
RABBITMQ_HOST=localhost
RABBITMQ_QUEUE=generate_materials

# PostgreSQL
DB_HOST=localhost
DB_PORT=5432
DB_NAME=assessments_db
DB_USER=myuser
DB_PASS=mypassword

# AWS
AWS_ACCESS_KEY_ID=your-key
AWS_SECRET_ACCESS_KEY=your-secret
AWS_REGION=us-east-1
S3_BUCKET=assessment-materials

## Running
```bash
    python main.py
```
or
OPTIONAL:
include postgreSQL image, RabbitMQ image in docker file to build and run
```bash
    docker run build .
```


## Example payload from rabbitMQ
{
    LocationID  *int64    `json:"location_id"`
    ProgramID   *int64    `json:"program_id"`
    SubjectID   *int64    `json:"subject_id"`
    SemesterID  *int64    `json:"semester_id"`
    DateStart   time.Time `json:"date"`
    DateEnd     time.Time `json:"date_end"`
    SortKey     string    `json:"sort_key"`
    Entity      *string   `json:"entity"`
    S3OutputKey *string   `json:"s3_output_key"`
    DataType    *string   `json:"data_type"`
}
