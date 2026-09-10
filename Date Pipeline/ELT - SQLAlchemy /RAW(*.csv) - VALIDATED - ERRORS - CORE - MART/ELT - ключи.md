~~~
# ┌──────────────────────────────────────────────────────────────────────┐
# │ SQLAlchemy                                                           │
# │      +                                                               │
# │ query = text("""INSERT INTO raw_orders ... VALUES (:order_id, ...)   │
# │      +                                                               │
# │ df = pd.read_csv(...)                                                │
# │      +                                                               │
# │ file = df.to_dict(orient="records")                                  │
# │      +                                                               │
# │ connection.execute(query, file)                                      │
# │      +                                                               │
# │ query1 = text("""                                                    │
# │     CREATE TABLE validated_orders AS                                 │
# │     SELECT                                                           │
# │         order_id::int,                                               │
# │         customer_id::int,                                            │
# │         ...                                                           │
# │                                                                       │
# │         CASE                                                         │
# │             WHEN customer_id NOT IN (                                 │
# │                 SELECT customer_id                                     │
# │                 FROM customers                                        │
# │             )                                                         │
# │             THEN 'customer_id not found'                              │
# │      +                                                                │
# │ connection.execute(                                                  │
# │     text("DROP TABLE IF EXISTS validated_orders")                     │
# │ )                                                                     │
# │      +                                                                │
# │ query = text("""                                                      │
# │     INSERT INTO orders                                                │
# │     SELECT ...                                                        │
# │     FROM validated_orders                                             │
# │     WHERE error_reason IS NULL                                        │
# │     ON CONFLICT (order_id)                                            │
# │     DO UPDATE SET                                                     │
# │         ...                                                            │
# │      +                                                                │
# │ query = text("""                                                      │
# │     CREATE TABLE error_orders                                         │
# │     ...                                                               │
# │      +                                                                │
# │ query = text("""                                                      │
# │     CREATE TABLE dm_customer_sales AS                                 │
# │     SELECT ...                                                        │
# └──────────────────────────────────────────────────────────────────────┘

~~~
