# STEDI Step Trainer Data Lakehouse Project

## 📌 Project Overview
This project builds a **Lakehouse solution** using **AWS Glue, S3, Athena, and PySpark** to process **STEDI Step Trainer** sensor data. The data is cleaned, transformed, and stored in different zones (**Landing, Trusted, and Curated**) for machine learning.


## 🛠️ **Workflow**
1. **Landing Zone**: Raw data is stored in S3.
2. **Trusted Zone**: Filtered customer and accelerometer data.
3. **Curated Zone**: Final dataset for machine learning.

### **✅ AWS Glue Jobs**
| Job Name                      | Description |
|--------------------------------|-------------|
| `customer_trusted.py`          | Filters customers who agreed to share data. |
| `accelerometer_trusted.py`     | Filters accelerometer data for approved customers. |
| `customers_curated.py`         | Keeps only customers with accelerometer data. |
| `step_trainer_trusted.py`      | Filters step trainer data for approved customers. |
| `machine_learning_curated.py`  | Joins accelerometer & step trainer data for ML. |

### **✅ SQL Queries**
| File | Purpose |
|------|---------|
| `customer_landing.sql` | Creates `customer_landing` table. |
| `accelerometer_landing.sql` | Creates `accelerometer_landing` table. |
| `step_trainer_landing.sql` | Creates `step_trainer_landing` table. |

## 🚀 **How to Run**
1. Upload raw JSON data to **S3**.
2. Create AWS Glue tables using **SQL queries**.
3. Execute **Glue Jobs** step by step.
4. Verify row counts in **Athena**.

---

🎯 **Goal**: Create a AWS Spark data pipeline to provide a clean dataset for **step detection ML models**.  
📌 **Tech Stack**: **PySpark, Glue, AWS S3, Athena, **.  
📌 **Author**: Seulgie 
