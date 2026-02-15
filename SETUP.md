# Setup Instructions

## Step 1: Databricks Workspace Setup

1. Create free Databricks Community account: https://community.cloud.databricks.com/
2. Create new cluster (Runtime 14.3 LTS or higher)

## Step 2: Data Preparation

1. Download European Soccer Database:
   - Source: https://www.kaggle.com/datasets/hugomathien/soccer
   
2. Upload to Databricks Volumes:
```
   /Volumes/workspace/default/soccer_data/
```

## Step 3: Run Pipeline

1. Import notebook to Databricks
2. Attach to cluster
3. Run all cells

**Expected runtime**: 5-7 minutes
**Output**: Silver and Gold tables in Volumes
