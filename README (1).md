# 🚦 Traffic Data ETL Pipeline using Apache Airflow

## 📚 Overview

This project demonstrates the creation and deployment of an automated ETL (Extract, Transform, Load) data pipeline using **Apache Airflow**. The pipeline is designed to fetch, process, and transform traffic data from a remote source into a clean, structured CSV file for downstream analysis.

- **Tool Used**: Apache Airflow
- **Language**: Python
- **Author**: Gurdarshan Singh
- **Course**: Data Acquisition and Management (PGDM Predictive Analytics)

## 🔗 Data Source

Data is downloaded from the following public link:

```
https://elasticbeanstalk-us-east-2-340729127361.s3.us-east-2.amazonaws.com/trafficdata.tgz
```

## 🔧 Pipeline Tasks (DAG: `traffic_data_ETL`)

1. Create Directory
2. Download Data
3. Extract Data
4. Extract CSV
5. Extract TSV
6. Extract Fixed Width
7. Combine Files
8. Transform Data
9. Define Dependencies

## 🗂 Output

Final transformed data is saved at:
```
/tmp/traffic_data/transformed_data.csv
```

## 🖥 Airflow Web UI

- Access DAG at: `http://localhost:8080`
- Views Used:
  - Graph View
  - Gantt Chart
  - Task Logs
  - Task Duration / Run Duration Charts

## 📝 Report & Observations

Detailed report available in `Assignment_4.pdf` including:
- Execution trends
- Gantt chart observations
- Task bottlenecks
- Screenshots for DAG tracking

## 📂 Repository Structure

```
├── traffic_data_etl.py
├── README.md
├── Assignment_4.pdf
├── screenshots/
├── requirements.txt
└── .gitignore
```

## 🧾 Requirements

To run the project locally:
```
pip install apache-airflow pandas requests
```

To start Airflow:
```
airflow standalone
```

## ✅ Conclusion

This project highlights how Apache Airflow can automate complex ETL workflows involving mixed file formats, structured scheduling, and monitoring through its UI.

## 📧 Contact

**Gurdarshan Singh**  
PGDM – Predictive Analytics  
University of Winnipeg