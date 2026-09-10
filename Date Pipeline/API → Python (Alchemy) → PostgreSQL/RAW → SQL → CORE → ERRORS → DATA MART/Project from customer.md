~~~
import requests
from sqlalchemy import create_engine
from sqlalchemy import text
import os
from dotenv import load_dotenv


load_dotenv(r"C:\Users\Vladislav X\Desktop\proj\ver6\1.env")

user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
database = os.getenv("DB_NAME")


engine = create_engine(
    f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
)



#-----------------------------------

with engine.connect() as connection:
    result = connection.execute(text("SELECT 1"))

    if result.fetchone() == (1,):
        print("Подключен")

# -----------------------------------


url = "https://dummyjson.com/users"

response = requests.get(url)

print(response.status_code)
# print(response.json())


data = response.json()


users = data["users"]

# print(data)
# print(users[0])


user = users[0]

# print(user)

customer = {
    "customer_id": user["id"],
    "name": user["firstName"] + " " + user["lastName"],
    "email": user["email"],
    "age": user["age"],
    "city": user["address"]["city"],
    "phone": user["phone"]
}

# print(customer)

customers = []
for user in users:
    customer = {
        "customer_id": user["id"],
        "name": user["firstName"] + " " + user["lastName"],
        "email": user["email"],
        "age": user["age"],
        "city": user["address"]["city"],
        "phone": user["phone"]
    }

    customers.append(customer)

# print(customers)

# print("Количество клиентов:", len(customers))
print("Поля:", customers[0].keys())
print("Первая запись:", customers[0])



valid_customers = []
error_customers = []

for customer in customers:

    error_reason = None

    if customer["customer_id"] is None:
        error_reason = "customer_id is NULL"

    elif customer["name"] is None:
        error_reason = "name is NULL"

    elif customer["email"] is None:
        error_reason = "email is NULL"

    elif customer["age"] < 18 or customer["age"] > 100:
        error_reason = "age is out of range"

    elif customer["city"] is None:
        error_reason = "city is NULL"

    if error_reason:
        error_customers.append({
            **customer,
            "error_reason": error_reason
        })
    else:
        valid_customers.append(customer)

print("Всего:", len(customers))
print("Корректных:", len(valid_customers))
print("Ошибочных:", len(error_customers))



try:
    with engine.begin() as connection:

        connection.execute(
            text("TRUNCATE TABLE raw_customers")
        )

        for customer in customers:
            connection.execute(
                text("""
                        INSERT INTO raw_customers
                            (customer_id, name, email, age, city, phone)
                        VALUES
                            (:customer_id, :name, :email, :age, :city, :phone)
                    """),
                customer
            )

        result = connection.execute(
            text("SELECT COUNT(*) FROM raw_customers")
        )

        print("Строк в raw_customers:", result.scalar())

except Exception as e:
    print("Ошибка:", e)
    raise



#
# Что произошло?
# CREATE TABLE customers AS SELECT ... означает:
# создать новую таблицу customers и положить в неё результат SELECT.

# ┌────────────────┐
# │ raw_customers  │
# │ ВСЕ данные     │
# └───────┬────────┘
#         ↓
#       SQL
#     валидация
#         ↓
# ┌────────────────┐
# │   customers    │
# │ только valid   │
# └────────────────┘

# Важный момент
# Мы не удаляем ошибочные строки из RAW.
# RAW остаётся как был.
# Например:

# RAW
# ├── Иван   → корректный
# ├── Пётр   → возраст 15 ❌
# └── Анна   → city NULL ❌
#
# CORE customers
# └── Иван   → только корректный

# А ошибочные записи мы сохраним отдельно в error_customers.

# CREATE TABLE customers AS

#
# SELECT
#     customer_id,
#     name,
#     email,
#     age,
#     city,
#     phone
#
# FROM raw_customers
#
# WHERE customer_id IS NOT NULL
#   AND name IS NOT NULL
#   AND email IS NOT NULL
#   AND age BETWEEN 18 AND 100
#   AND city IS NOT NULL;



# Создаём error_customers
# Теперь сохраняем ошибки отдельно:

#
# CREATE TABLE error_customers AS
#
# SELECT
#     customer_id,
#     name,
#     email,
#     age,
#     city,
#     phone,
#
#     CASE
#         WHEN customer_id IS NULL THEN 'customer_id is NULL'
#         WHEN name IS NULL THEN 'name is NULL'
#         WHEN email IS NULL THEN 'email is NULL'
#         WHEN age < 18 OR age > 100 THEN 'age is out of range'
#         WHEN city IS NULL THEN 'city is NULL'
#     END AS error_reason
#
# FROM raw_customers
#
# WHERE customer_id IS NULL
#    OR name IS NULL
#    OR email IS NULL
#    OR age < 18
#    OR age > 100
#    OR city IS NULL;
#
#



~~~
