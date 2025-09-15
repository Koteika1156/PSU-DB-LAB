import psycopg2
import settings

def create_tables():
    with open('setup_normalized_db.sql', encoding='utf-8') as f:
        sql_script = f.read()

    conn = psycopg2.connect(**settings.config)
    cur = conn.cursor()
    cur.execute(sql_script)
    conn.commit()
    cur.close()
    conn.close()

create_tables()
