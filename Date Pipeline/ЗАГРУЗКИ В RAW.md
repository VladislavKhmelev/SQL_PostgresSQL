~~~
ЗАГРУЗКИ В RAW:


1. CSV → PostgreSQL RAW через SQLAlchemy

# CSV
#  ↓
# Python open()
#  ↓
# чтение файла  (with open(file, "r", encoding="utf-8") as file:)
#  ↓
# SQLAlchemy connection.execute(query, file)
#  ↓
# PostgreSQL RAW

То есть SQLAlchemy здесь отвечает за загрузку в БД, а Python — за чтение CSV.



1/2. CSV → Pandas → техническая подготовка → SQLAlchemy → PostgreSQL

# CSV
#  ↓
# pd.read_csv()     
#  ↓
# DataFrame    
#  ↓
# техническая подготовка
#  ↓
# file= df.to_dict(orient="records")
#  ↓
# list[dict]
#  ↓
# SQLAlchemy  connection.execute(query, file)
#  ↓
# PostgreSQL



2/2. CSV → Pandas → техническая подготовка → SQLAlchemy → PostgreSQL

# CSV
#  ↓
# pd.read_csv()     
#  ↓
# DataFrame    
#  ↓
# техническая подготовка
#  ↓
# SQLAlchemy 
#  ↓
# df.to_sql("raw_orders",con=engine,if_exists="append",index=False)
#  ↓
# PostgreSQL RAW



3. CSV → PostgreSQL COPY через Psycopg2
прямая загрузка 

# CSV
#  ↓
# Python open()  (with open(file, "r", encoding="utf-8") as file:)
#  ↓
# Psycopg2   cursor.copy_expert + COPY raw_orders + FROM STDIN WITH CSV HEADER   , file
#  ↓
# COPY
#  ↓
# PostgreSQL RAW


COPY предназначен именно для массовой загрузки.



4. psql \copy
Она похожа на COPY, но файл читается клиентом, а не сервером.

\copy raw_orders FROM 'orders.csv' WITH CSV HEADER
~~~
