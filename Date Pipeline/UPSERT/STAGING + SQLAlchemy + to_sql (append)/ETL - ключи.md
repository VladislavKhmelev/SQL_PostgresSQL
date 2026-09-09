STAGING + SQLAlchemy + error_df.to_csv + TRUNCATE TABLE staging_customers + clean_df.to_sql(staging_customers) + query = text("""
    INSERT INTO + from staging_customers insert into customers (ON CONFLICT (id) DO UPDATE)
