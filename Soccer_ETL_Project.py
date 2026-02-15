# Databricks notebook source
# MAGIC %sh
# MAGIC # 1. Wyczyść i przygotuj folder roboczy w lokalnym systemie plików
# MAGIC rm -rf /tmp/soccer_work
# MAGIC mkdir -p /tmp/soccer_work
# MAGIC cd /tmp/soccer_work
# MAGIC
# MAGIC # 2. Skopiuj plik z Volumes do lokalnego folderu /tmp/
# MAGIC # Używamy ścieżki /Volumes/... bez przedrostka /dbfs/ jeśli to możliwe
# MAGIC cp /Volumes/workspace/default/soccer_data/soccer.zip .
# MAGIC
# MAGIC # 3. Rozpakuj
# MAGIC echo "📦 Rozpakowuję soccer.zip..."
# MAGIC unzip -o soccer.zip
# MAGIC
# MAGIC # 4. Pokaż co udało się wypakować
# MAGIC echo ""
# MAGIC echo "========================================="
# MAGIC echo "✓ Zawartość folderu tymczasowego:"
# MAGIC ls -lh
# MAGIC
# MAGIC # 5. Opcjonalnie: Skopiuj rozpakowany plik z powrotem do Volumes, aby go zachować na stałe
# MAGIC if [ -f "database.sqlite" ]; then
# MAGIC     echo "✅ Znaleziono database.sqlite. Kopiuję do Volumes..."
# MAGIC     cp database.sqlite /Volumes/workspace/default/soccer_data/
# MAGIC     echo "Done!"
# MAGIC else
# MAGIC     echo "❌ Błąd: Nie znaleziono database.sqlite wewnątrz ZIPa."
# MAGIC fi

# COMMAND ----------

import sqlite3
import pandas as pd

# Ścieżka do Twojego pliku w Volumes
db_path = "/Volumes/workspace/default/soccer_data/database.sqlite"

# Połączenie z bazą
conn = sqlite3.connect(db_path)

# Sprawdzenie dostępnych tabel
tables = pd.read_sql("""SELECT name FROM sqlite_master WHERE type='table';""", conn)
display(tables)

# Zamknięcie połączenia po sprawdzeniu
conn.close()

# COMMAND ----------

# DBTITLE 1,Cell 3
import pandas as pd
import sqlite3
import decimal
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DecimalType
from pyspark.sql.functions import col

# Połączenie
conn = sqlite3.connect("/Volumes/workspace/default/soccer_data/database.sqlite")

# 1. Wczytanie
pdf_player = pd.read_sql("SELECT * FROM Player", conn)

# 2. DATA QUALITY CHECK 
def data_quality_check(pdf_player):
  if len(pdf_player['player_api_id'].unique()) == len(pdf_player):
    print("Test unique id passed")
  else:
    print("Test unique id failed")
  if pdf_player['player_name'].isnull().sum() == 0:
    print("Test not-null player name passed")
  else:
    print("Test not-null player name failed")

data_quality_check(pdf_player)

# KONWERSJA
pdf_player['birthday'] = pd.to_datetime(pdf_player['birthday']).dt.strftime('%Y-%m-%d')
pdf_player['height'] = pdf_player['height'].apply(lambda x: None if pd.isnull(x) else decimal.Decimal(str(x)))

# 3. Konwersja do Sparka (jeśli testy OK)
# Define schema
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

# Wyświetl schemat
df_player.printSchema()

# COMMAND ----------

# DBTITLE 1,Cell 4
from pyspark.sql.functions import year, col

# 1. Transformacja: Dodaj kolumnę birth_year na potrzeby partycjonowania
df_player_enhanced = df_player.withColumn("birth_year", year(col("birthday")))

# 2. OPTYMALIZACJA: Cache'owanie
# Powiedz Sparkowi, że będziesz często używał tej ramki
# df_player_enhanced.cache()  # Usunięte, nieobsługiwane na serverless

# 3. ZAPIS DO WARSTWY SILVER (Parquet)
# Ścieżka: "/Volumes/workspace/default/soccer_data/silver/players"
# 3. ZAPIS DO WARSTWY SILVER
(df_player_enhanced.write
  .mode("overwrite")              # Jeśli folder istnieje, nadpisz go (bezpieczne przy testach)
  .format("parquet")              # Ustawiamy format na Parquet (standard Big Data)
  .partitionBy("birth_year")      # Fizyczny podział danych na podfoldery wg roku
  .save("/Volumes/workspace/default/soccer_data/silver/players"))

print("Zapisano warstwę Silver z partycjonowaniem!")

# COMMAND ----------

display(dbutils.fs.ls("/Volumes/workspace/default/soccer_data/silver/players"))

# COMMAND ----------

# DBTITLE 1,SQL Query and Data Quality Check for Match Table
import pandas as pd
import sqlite3
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# 0. Połączenie z bazą
conn = sqlite3.connect("/Volumes/workspace/default/soccer_data/database.sqlite")

# 1. SQL Query - ograniczamy dane na wejściu (Optymalizacja!)
query = """
SELECT id, country_id, league_id, season, stage, date, 
       match_api_id, home_team_api_id, away_team_api_id 
FROM Match
"""
pdf_match = pd.read_sql(query, conn)

pdf_match['date'] = pd.to_datetime(pdf_match['date'])

# 2. Data Quality Check
# Sprawdź, czy pdf_match['date'] nie zawiera dat z przyszłości
# Podpowiedź: pd.to_datetime(pdf_match['date']).max()
def check_future_date(pdf_match):
  max_date = pdf_match['date'].max()
  today = pd.Timestamp.today()
  if max_date > today:
    print("Test future date failed")
  else:
    print("Test future date passed")

check_future_date(pdf_match)

# 3. Definicja schematu
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

# Tworzenie Spark DataFrame
df_match = spark.createDataFrame(pdf_match, schema=schema_match)

# Usunięto cache() - nieobsługiwane na serverless

print("Tabela Match wczytana do Sparka!")
df_match.show(5)


# COMMAND ----------

# DBTITLE 1,Cell 7
import pandas as pd
import sqlite3
import decimal
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DecimalType
from pyspark.sql.functions import col, expr, broadcast

# Połączenie
conn = sqlite3.connect("/Volumes/workspace/default/soccer_data/database.sqlite")

# 1. Wczytanie
pdf_player = pd.read_sql("SELECT * FROM Player", conn)

# KONWERSJA
pdf_player['birthday'] = pd.to_datetime(pdf_player['birthday']).dt.strftime('%Y-%m-%d')
pdf_player['height'] = pdf_player['height'].apply(lambda x: None if pd.isnull(x) else decimal.Decimal(str(x)))

# Define schema
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

# --- Original user code ---
# 1. Pobierzemy ID meczu i wszystkie kolumny zawodników (musisz je dodać do swojego wcześniejszego SELECTa w Match lub wczytać teraz)
# Załóżmy, że wczytujemy match_api_id oraz kolumny home_player_1...11 i away_player_1...11

# Lista kolumn zawodników do wyciągnięcia
home_players = [f"home_player_{i}" for i in range(1, 12)]
away_players = [f"away_player_{i}" for i in range(1, 12)]
player_cols = ", ".join(home_players + away_players)

query_match_full = f"""
SELECT match_api_id, {player_cols}
FROM Match
"""
pdf_match_players = pd.read_sql(query_match_full, conn)
df_match_players = spark.createDataFrame(pdf_match_players)

# To jest przykład jak "odpiwotować" dane w Sparku:

# Tworzymy string dla funkcji stack: "stack(22, 'home_1', home_player_1, 'home_2', ...)"
stack_cols = ", ".join([f"'{col}', {col}" for col in home_players + away_players])
stack_expression = f"stack(22, {stack_cols}) as (player_role, player_api_id)"

df_match_long = df_match_players.selectExpr("match_api_id", stack_expression) \
    .where("player_api_id IS NOT NULL") # DQ: ignorujemy puste składy

df_match_long.show(10)

# 2. Teraz czas na Broadcast Join z tabelą Player, którą zrobiłeś wcześniej!
# Chcemy do każdego występu dołączyć imię i nazwisko gracza.

# Łączymy występy z danymi personalnymi zawodników
gold_player_performance = df_match_long.join(
    broadcast(df_player), # df_player jest mały, więc broadcastujemy!
    "player_api_id",
    "left"
)

display(gold_player_performance.select("match_api_id", "player_name", "player_role"))

# COMMAND ----------

# 1. Wczytaj Team i zrób z niego Broadcast
pdf_team = pd.read_sql("SELECT team_api_id, team_long_name FROM Team", conn)
df_team = spark.createDataFrame(pdf_team)

# 2. Musimy dołączyć nazwę drużyny do gold_player_performance
# Wykorzystaj fakt, że player_role zaczyna się od 'home' lub 'away'
from pyspark.sql.functions import when, col

# Dodajmy pomocniczą kolumnę, która powie nam, które ID drużyny przypisać do gracza
gold_with_team_ids = gold_player_performance.join(
    broadcast(df_match.select("match_api_id", "home_team_api_id", "away_team_api_id")),
    "match_api_id"
).withColumn("current_team_id", 
    when(col("player_role").contains("home"), col("home_team_api_id"))
    .otherwise(col("away_team_api_id"))
)

# 3. Teraz Twój ruch: Zrób finałowy Join z df_team, aby zamienić current_team_id na team_long_name.
# Finałowy join: zamień current_team_id na team_long_name
final_player_performance = gold_with_team_ids.join(
    broadcast(df_team),
    gold_with_team_ids.current_team_id == df_team.team_api_id,
    "left"
)

# Wybierz kluczowe kolumny do prezentacji
result = final_player_performance.select(
    "match_api_id", "player_name", "player_role", "team_long_name"
)

display(result)

# COMMAND ----------

# DBTITLE 1,Step 3: Join with Team Names
from pyspark.sql.functions import col, count

# 1. Filtrujemy dane dla Interu
# Używamy ilike, żeby nie martwić się o wielkość liter
inter_analysis = result.filter(col("team_long_name").contains("Inter")) \
    .groupBy("player_name") \
    .agg(count("match_api_id").alias("appearances")) \
    .orderBy(col("appearances").desc())

print("📊 Top 10 zawodników Interu Mediolan w bazie:")
inter_analysis.show(10)

# COMMAND ----------

# DBTITLE 1,Cell 10
from pyspark.sql.functions import coalesce, lit, round, avg, desc, count, broadcast

# 1. Poprawiamy wcześniejszy krok (result musi mieć ID!)
result_with_id = final_player_performance.select(
    "match_api_id", 
    "player_api_id",  # <--- TO BYŁ BRAKUJĄCY ELEMENT
    "player_name", 
    "player_role", 
    "team_long_name"
)

# 2. Finałowy raport z obsługą NULL (Ważne dla DQE!)
inter_final_report = result_with_id.filter(col("team_long_name").contains("Inter")) \
    .join(broadcast(df_attrs), "player_api_id", "left") \
    .groupBy("player_name") \
    .agg(
    count("match_api_id").alias("appearances"),
    round(avg(coalesce(col("avg_rating"), lit(0))), 2).alias("skill_level")
) \
    .orderBy(desc("appearances"))

print("🏆 Raport Interu (Wersja DQE - stabilna i kompletna):")
inter_final_report.show(15)


# COMMAND ----------

# DBTITLE 1,Check Skill Level Assertions
from pyspark.sql.functions import col

def check_skill_level_pro(df):
    total_count = df.count()
    # Liczymy tych, którzy mają poprawne dane (nie null i nie 0)
    valid_count = df.filter((col("skill_level").isNotNull()) & (col("skill_level") > 0)).count()
    
    # Obliczamy metrykę "Completeness"
    completeness_pct = (valid_count / total_count) * 100
    
    print(f"--- RAPORT JAKOŚCI DANYCH (DQE) ---")
    print(f"Całkowita liczba zawodników: {total_count}")
    print(f"Zawodnicy z poprawnym ratingiem: {valid_count}")
    print(f"Wskaźnik kompletności (Completeness): {completeness_pct:.2f}%")
    
    if completeness_pct < 80:
        print("❌ STATUS: DANE NIEPRZYDATNE DO ANALIZY (Poniżej progu 80%)")
    else:
        print("✅ STATUS: JAKOŚĆ DANYCH ZAAKCEPTOWANA")

check_skill_level_pro(inter_final_report)

# COMMAND ----------

def validate_uniqueness(df, column_name):
    total_rows = df.count()
    unique_rows = df.select(column_name).distinct().count()
    
    if total_rows == unique_rows:
        print(f"✅ TEST UNIKALNOŚCI: PASSED. Każdy wiersz to unikalny {column_name}.")
    else:
        duplicates = total_rows - unique_rows
        print(f"❌ TEST UNIKALNOŚCI: FAILED! Znaleziono {duplicates} duplikaty/ów w kolumnie {column_name}.")
        # DQE Action: Pokaż duplikaty, żeby DE mógł je naprawić
        df.groupby(column_name).count().filter("count > 1").show()

validate_uniqueness(inter_final_report, "player_name")

# COMMAND ----------

