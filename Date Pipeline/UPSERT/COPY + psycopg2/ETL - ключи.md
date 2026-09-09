 COPY + psycopg2 + TRUNCATE TABLE staging_customers + FROM STDIN with open (*.csv) + TEMP TABLE validated_tables --> error_customers(error_reason) + customers (ON CONFLICT (id))
