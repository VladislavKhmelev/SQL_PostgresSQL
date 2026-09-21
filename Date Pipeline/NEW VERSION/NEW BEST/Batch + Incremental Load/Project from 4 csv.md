~~~
from pathlib import Path
import pandas as pd


files = list(Path(r"C:\Users\VladK\OneDrive\Desktop\proj\ver10\data").glob("*.csv"))

#
print(files,'\n')
print('сколько файлов:', len(files), '\n')


dataframes = []



for file in files:
    print("=" * 80)
    print("ФАЙЛ:", file.name)
    print("=" * 80)

    df = pd.read_csv(file)

    print(df,'\n')

    dataframes.append(df)



print('обьединение таблиц:', '\n')
df = pd.concat(dataframes, ignore_index=True)

print(df.head(2), '\n')
print('сколько общих строк после обьединения:', len(df), '\n')


print(df.dtypes)



df.to_csv(
    r"C:\Users\VladK\OneDrive\Desktop\proj\ver10\data\raw_sales_all_batch.csv",
    index=False
)


далее загрузка в RAW (text) + VALIDATED (CORE + ERROR наш bacth_csv) + UPSERT в существующий CORE (Increment load: INSERT + DO UPDATE + WHERE EXCLUDED.updated_at )



DDL
--------------------------
CREATE TABLE raw_sales_all_batch (
sale_id text,
product_id text,
quantity text,
amount text,
sale_date  text,
updated_at text


);




\copy raw_sales_all_batch FROM 'C:\Users\VladK\OneDrive\Desktop\proj\ver10\data\обработка через pandas file\products.csv' WITH (FORMAT csv, HEADER true);



# тут приводим типы и error_reason (как обычно)

DDL + DQL
-----------------------

create table validated_sales_batch as

SELECT

.....


NULLIF(
    CONCAT_WS(
        '; ',
        CASE
            WHEN  NULLIF(trim(customer_id)     ,'') is null
                THEN 'customer_id is NULL'

....




DDL (структура таблиц):
----------------------
create table error_sales_batch
create table core_sales_batch


--далее

DML (как обычно)
------------------------
1. INSERT INTO error_sales_batch ... SELECT

2. Incremental Load (UPSERT: INSERT....ON COFLICT...WHERE):
    INSERT INTO core_sales_batch ... SELECT








~~~
