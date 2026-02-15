# ⚽ Soccer Match Analytics ETL Pipeline

End-to-end ETL pipeline processing 25,000+ soccer matches using PySpark and Databricks Medallion Architecture.

![Python](https://img.shields.io/badge/Python-3.10-blue)
![PySpark](https://img.shields.io/badge/PySpark-3.5-orange)
![Databricks](https://img.shields.io/badge/Databricks-Serverless-red)
![License](https://img.shields.io/badge/License-MIT-green)

## 📊 Project Overview

This project demonstrates production-grade data engineering practices by building a scalable ETL pipeline that:
- Extracts data from SQLite database (250K+ records)
- Transforms using PySpark with Medallion Architecture (Bronze → Silver → Gold)
- Implements Data Quality Engineering (DQE) with automated validation
- Optimizes performance using broadcast joins and partitioning
- Delivers analytical datasets for team performance analysis

## 🏗️ Architecture
```
SQLite (Bronze)
    ↓
Parquet Partitioned (Silver)
    ↓
Aggregated Analytics (Gold)
```

### Medallion Layers:
- **Bronze**: Raw data from SQLite (`Player`, `Match`, `Team`, `Player_Attributes`)
- **Silver**: Cleaned Parquet files with partitioning by `birth_year`
- **Gold**: Business-ready aggregations (player performance, team analytics)

## 🔧 Tech Stack

- **Compute**: Databricks Serverless
- **Processing**: PySpark 3.5, Pandas
- **Storage**: Databricks Volumes, Parquet format
- **Quality**: Custom DQE framework with assertions
- **Optimization**: Broadcast joins, partition pruning

## 🚀 Quick Start

### Prerequisites
- Databricks Workspace (Community Edition OK)
- Python 3.10+
- Access to European Soccer Database

### Installation
```bash
# Clone repository
git clone https://github.com/YOUR_USERNAME/soccer-etl-databricks.git
cd soccer-etl-databricks

# Install dependencies
pip install -r requirements.txt
```

### Setup in Databricks

1. Upload `database.sqlite` to Databricks Volumes:
```
   /Volumes/workspace/default/soccer_data/soccer.zip
```

2. Import notebook:
   - Workspace → Import → Select `Soccer_ETL_Project.py`

3. Run all cells (Runtime: ~5 minutes)

## 📈 Key Features

### 1. Data Quality Engineering (DQE)
```python
✅ Uniqueness validation (player_api_id)
✅ NOT NULL constraints (player_name)
✅ Temporal validation (no future dates)
✅ Completeness metrics (80%+ threshold)
```

### 2. Performance Optimizations

- **Broadcast Joins**: 3-5x faster for dimension tables
- **Partitioning**: Queries scan only relevant `birth_year` partitions
- **Parquet Format**: 75% storage reduction vs CSV

### 3. Advanced Transformations
```python
# Unpivoting 22 players per match
stack_expression = "stack(22, 'home_1', home_player_1, ...)"

# Dynamic team assignment
when(col("player_role").contains("home"), col("home_team_api_id"))
```

## 📊 Sample Analysis: Inter Milan Top Players
```python
# Output example
+-------------------+------------+------------+
|player_name        |appearances |skill_level |
+-------------------+------------+------------+
|Javier Zanetti     |143         |75.8        |
|Esteban Cambiasso  |128         |73.2        |
|Maicon             |115         |76.5        |
+-------------------+------------+------------+
```

## 🧪 Data Quality Report
```
--- RAPORT JAKOŚCI DANYCH (DQE) ---
Całkowita liczba zawodników: 247
Zawodnicy z poprawnym ratingiem: 203
Wskaźnik kompletności: 82.19%
✅ STATUS: JAKOŚĆ DANYCH ZAAKCEPTOWANA
```

## 📚 Learning Outcomes

This project demonstrates:
- ✅ Medallion Architecture implementation
- ✅ PySpark optimization techniques (broadcast, partitioning)
- ✅ Data Quality Engineering (DQE) framework
- ✅ Complex joins across multiple tables
- ✅ Handling NULL values and data imputation
- ✅ Production-ready code structure

## 🔄 Future Enhancements

- [ ] Incremental loading with Delta Lake
- [ ] Orchestration with Databricks Workflows
- [ ] ML model: Match outcome prediction
- [ ] Real-time streaming with Structured Streaming
- [ ] Power BI/Tableau dashboard
- [ ] REST API for Gold tables

## 📄 License

MIT License - see [LICENSE](LICENSE) file

## 🤝 Contributing

Contributions welcome! Please open an issue first to discuss changes.

---

⭐ If you found this project helpful, please give it a star!
