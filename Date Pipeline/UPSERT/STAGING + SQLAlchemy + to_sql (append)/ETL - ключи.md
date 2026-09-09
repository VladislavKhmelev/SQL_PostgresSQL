STAGING + SQLAlchemy + error_df.to_csv + TRUNCATE TABLE staging_customers + clean_df.to_sql(staging_customers через SQLAlchemy и режим append добавляем в таблицу) + query = text("""
    INSERT INTO ...тут в запросе берем из таблицы staging_customers .....и вставляем ее в .....insert into customers (ON CONFLICT (id) DO UPDATE)
