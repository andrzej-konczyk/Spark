# Databricks notebook source
# MAGIC %sh
# MAGIC rm -rf /tmp/soccer_work
# MAGIC mkdir -p /tmp/soccer_work
# MAGIC cd /tmp/soccer_work
# MAGIC
# MAGIC cp /Volumes/workspace/default/soccer_data/soccer.zip .
# MAGIC
# MAGIC echo "📦 Extracting soccer.zip..."
# MAGIC unzip -o soccer.zip
# MAGIC
# MAGIC echo ""
# MAGIC echo "========================================="
# MAGIC echo "✓ Contents of temporary folder:"
# MAGIC ls -lh
# MAGIC
# MAGIC if [ -f "database.sqlite" ]; then
# MAGIC     echo "✅ Found database.sqlite. Copying to Volumes..."
# MAGIC     cp database.sqlite /Volumes/workspace/default/soccer_data/
# MAGIC     echo "Done!"
# MAGIC else
# MAGIC     echo "❌ Error: database.sqlite not found in ZIP."
# MAGIC fi

# COMMAND ----------

import sqlite3
import pandas as pd

db_path = "/Volumes/workspace/default/soccer_data/database.sqlite"
conn = sqlite3.connect(db_path)

tables = pd.read_sql("""SELECT name FROM sqlite_master WHERE type='table';""", conn)
display(tables)

conn.close()

# COMMAND ----------

# DBTITLE 1,Extract and Validate Player Data
import pandas as pd
import sqlite3
import decimal
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
from pyspark.sql.functions import col

conn = sqlite3.connect("/Volumes/workspace/default/soccer_data/database.sqlite")

pdf_player = pd.read_sql("SELECT * FROM Player", conn)

def data_quality_check(pdf_player):
    if len(pdf_player['player_api_id'].unique()) == len(pdf_player):
        print("✓ Test unique id passed")
    else:
        print("✗ Test unique id failed")
    if pdf_player['player_name'].isnull().sum() == 0:
        print("✓ Test not-null player name passed")
    else:
        print("✗ Test not-null player name failed")

data_quality_check(pdf_player)

pdf_player['birthday'] = pd.to_datetime(pdf_player['birthday']).dt.strftime('%Y-%m-%d')
pdf_player['height'] = pdf_player['height'].apply(lambda x: None if pd.isnull(x) else decimal.Decimal(str(x)))

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("player_api_id", IntegerType(), True),
    StructField("player_name", StringType(), True),
    StructField("player_fifa_api_id", IntegerType(), True),
    StructField("birthday", StringType(), True),
    StructField("height", DecimalType(10,2), True),
    StructField("weight", IntegerType(), True)
])

df_player = spark.createDataFrame(pdf_player, schema=schema)
df_player.printSchema()

# COMMAND ----------

# DBTITLE 1,Transform and Write to Silver Layer
from pyspark.sql.functions import year, col

df_player_enhanced = df_player.withColumn("birth_year", year(col("birthday")))

(df_player_enhanced.write
  .mode("overwrite")
  .format("parquet")
  .partitionBy("birth_year")
  .save("/Volumes/workspace/default/soccer_data/silver/players"))

print("✅ Silver layer saved with partitioning!")

# COMMAND ----------

display(dbutils.fs.ls("/Volumes/workspace/default/soccer_data/silver/players"))

# COMMAND ----------

# DBTITLE 1,Extract and Validate Match Data
import pandas as pd
import sqlite3
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

conn = sqlite3.connect("/Volumes/workspace/default/soccer_data/database.sqlite")

query = """
SELECT id, country_id, league_id, season, stage, date, 
       match_api_id, home_team_api_id, away_team_api_id 
FROM Match
"""
pdf_match = pd.read_sql(query, conn)
pdf_match['date'] = pd.to_datetime(pdf_match['date'])

def check_future_date(pdf_match):
    max_date = pdf_match['date'].max()
    today = pd.Timestamp.today()
    if max_date > today:
        print("✗ Test future date failed")
    else:
        print("✓ Test future date passed")

check_future_date(pdf_match)

schema_match = StructType([
    StructField("id", IntegerType(), True),
    StructField("country_id", IntegerType(), True),
    StructField("league_id", IntegerType(), True),
    StructField("season", StringType(), True),
    StructField("stage", IntegerType(), True),
    StructField("date", TimestampType(), True),
    StructField("match_api_id", IntegerType(), True),
    StructField("home_team_api_id", IntegerType(), True),
    StructField("away_team_api_id", IntegerType(), True)
])

df_match = spark.createDataFrame(pdf_match, schema=schema_match)

print("✅ Match table loaded to Spark!")
df_match.show(5)

# COMMAND ----------

# DBTITLE 1,Unpivot Match Player Data
import pandas as pd
import sqlite3
import decimal
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
from pyspark.sql.functions import col, expr, broadcast

conn = sqlite3.connect("/Volumes/workspace/default/soccer_data/database.sqlite")

pdf_player = pd.read_sql("SELECT * FROM Player", conn)

pdf_player['birthday'] = pd.to_datetime(pdf_player['birthday']).dt.strftime('%Y-%m-%d')
pdf_player['height'] = pdf_player['height'].apply(lambda x: None if pd.isnull(x) else decimal.Decimal(str(x)))

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("player_api_id", IntegerType(), True),
    StructField("player_name", StringType(), True),
    StructField("player_fifa_api_id", IntegerType(), True),
    StructField("birthday", StringType(), True),
    StructField("height", DecimalType(10,2), True),
    StructField("weight", IntegerType(), True)
])

df_player = spark.createDataFrame(pdf_player, schema=schema)

home_players = [f"home_player_{i}" for i in range(1, 12)]
away_players = [f"away_player_{i}" for i in range(1, 12)]
player_cols = ", ".join(home_players + away_players)

query_match_full = f"""
SELECT match_api_id, {player_cols}
FROM Match
"""
pdf_match_players = pd.read_sql(query_match_full, conn)
df_match_players = spark.createDataFrame(pdf_match_players)

stack_cols = ", ".join([f"'{col}', {col}" for col in home_players + away_players])
stack_expression = f"stack(22, {stack_cols}) as (player_role, player_api_id)"

df_match_long = df_match_players.selectExpr("match_api_id", stack_expression) \
    .where("player_api_id IS NOT NULL")

df_match_long.show(10)

gold_player_performance = df_match_long.join(
    broadcast(df_player),
    "player_api_id",
    "left"
)

display(gold_player_performance.select("match_api_id", "player_name", "player_role"))

# COMMAND ----------

# DBTITLE 1,Join with Team Names
from pyspark.sql.functions import when, col, broadcast

pdf_team = pd.read_sql("SELECT team_api_id, team_long_name FROM Team", conn)
df_team = spark.createDataFrame(pdf_team)

gold_with_team_ids = gold_player_performance.join(
    broadcast(df_match.select("match_api_id", "home_team_api_id", "away_team_api_id")),
    "match_api_id"
).withColumn("current_team_id", 
    when(col("player_role").contains("home"), col("home_team_api_id"))
    .otherwise(col("away_team_api_id"))
)

final_player_performance = gold_with_team_ids.join(
    broadcast(df_team),
    gold_with_team_ids.current_team_id == df_team.team_api_id,
    "left"
)

result = final_player_performance.select(
    "match_api_id", "player_name", "player_role", "team_long_name"
)

display(result)

# COMMAND ----------

# DBTITLE 1,Analyze Inter Milan Players
from pyspark.sql.functions import col, count

inter_analysis = result.filter(col("team_long_name").contains("Inter")) \
    .groupBy("player_name") \
    .agg(count("match_api_id").alias("appearances")) \
    .orderBy(col("appearances").desc())

print("📊 Top 10 Inter Milan players in database:")
inter_analysis.show(10)

# COMMAND ----------

# DBTITLE 1,Join with Player Attributes and Calculate Skill Level
from pyspark.sql.functions import coalesce, lit, round, avg, desc, count, broadcast

pdf_attrs = pd.read_sql("""
    SELECT player_api_id, 
           AVG(overall_rating) as avg_rating
    FROM Player_Attributes
    GROUP BY player_api_id
""", conn)

df_attrs = spark.createDataFrame(pdf_attrs)

result_with_id = final_player_performance.select(
    "match_api_id", 
    "player_api_id",
    "player_name", 
    "player_role", 
    "team_long_name"
)

inter_final_report = result_with_id.filter(col("team_long_name").contains("Inter")) \
    .join(broadcast(df_attrs), "player_api_id", "left") \
    .groupBy("player_name") \
    .agg(
        count("match_api_id").alias("appearances"),
        round(avg(coalesce(col("avg_rating"), lit(0))), 2).alias("skill_level")
    ) \
    .orderBy(desc("appearances"))

print("🏆 Inter Milan Report (with skill level):")
inter_final_report.show(15)

# COMMAND ----------

# DBTITLE 1,Data Quality: Completeness Check
from pyspark.sql.functions import col

def check_skill_level_completeness(df):
    total_count = df.count()
    valid_count = df.filter((col("skill_level").isNotNull()) & (col("skill_level") > 0)).count()
    
    completeness_pct = (valid_count / total_count) * 100
    
    print(f"--- DATA QUALITY REPORT (DQE) ---")
    print(f"Total players: {total_count}")
    print(f"Players with valid rating: {valid_count}")
    print(f"Completeness metric: {completeness_pct:.2f}%")
    
    if completeness_pct < 80:
        print("❌ STATUS: DATA NOT SUITABLE FOR ANALYSIS (Below 80% threshold)")
    else:
        print("✅ STATUS: DATA QUALITY ACCEPTED")

check_skill_level_completeness(inter_final_report)

# COMMAND ----------

# DBTITLE 1,Data Quality: Uniqueness Validation
def validate_uniqueness(df, column_name):
    total_rows = df.count()
    unique_rows = df.select(column_name).distinct().count()
    
    if total_rows == unique_rows:
        print(f"✅ UNIQUENESS TEST: PASSED. Each row is a unique {column_name}.")
    else:
        duplicates = total_rows - unique_rows
        print(f"❌ UNIQUENESS TEST: FAILED! Found {duplicates} duplicate(s) in column {column_name}.")
        df.groupby(column_name).count().filter("count > 1").show()

validate_uniqueness(inter_final_report, "player_name")

# COMMAND ----------
