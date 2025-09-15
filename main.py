import sqlite3
import psycopg2
import settings
import numpy as np


def get_data(cursor, table, column, value):
    cursor.execute(f"SELECT id FROM {table} WHERE {column} = %s", (value,))
    result = cursor.fetchone()
    if result:
        return result[0]
    else:
        cursor.execute(f"INSERT INTO {table} ({column}) VALUES (%s) RETURNING id", (value,))
        return cursor.fetchone()[0]


def get_data_multi(cursor, table, search_fields: dict, insert_fields: dict):
    where_clause = " AND ".join([f"{col} = %s" for col in search_fields.keys()])
    values = tuple(search_fields.values())
    cursor.execute(f"SELECT id FROM {table} WHERE {where_clause}", values)
    result = cursor.fetchone()

    if result:
        return result[0]
    else:
        cols = ", ".join(insert_fields.keys())
        placeholders = ", ".join(["%s"] * len(insert_fields))
        cursor.execute(
            f"INSERT INTO {table} ({cols}) VALUES ({placeholders}) RETURNING id",
            tuple(insert_fields.values())
        )
        return cursor.fetchone()[0]


def import_data():
    sqlite_con = sqlite3.connect('hospital_denormalized.db')
    sqlite_cur = sqlite_con.cursor()

    pg_con = psycopg2.connect(**settings.config)
    pg_cur = pg_con.cursor()

    sqlite_cur.execute("SELECT * FROM hospital_records")
    records = sqlite_cur.fetchall()
    structured_records = np.array(records, dtype=settings.dtype)

    for record in structured_records:
        department_id = get_data(pg_cur, 'departments', 'name', record["department_name"])
        patient_id = get_data_multi(
            pg_cur,
            "patients",
            {"full_name": record["patient_name"]},
            {"full_name": record["patient_name"], "birth_date": record["patient_dob"]}
        )
        diagnosis_id = get_data(pg_cur, 'diagnoses', 'name', record["diagnosis_name"])
        doctor_id = get_data_multi(
            pg_cur,
            "doctors",
            {"full_name": record["doctor_name"]},
            {
                "full_name": record["doctor_name"],
                "specialization": record["doctor_spec"],
                "department_id": department_id,
            }
        )
        pg_cur.execute(
            "SELECT id FROM appointments WHERE patient_id = %s AND doctor_id = %s AND appointment_date = %s",
            (patient_id, doctor_id, record["app_date"])
        )
        appointment_res = pg_cur.fetchone()

        if appointment_res:
            appointment_id = appointment_res[0]
            pg_cur.execute(
                "UPDATE appointments SET complaints = %s WHERE id = %s",
                (record["complaints"], appointment_id)
            )
        else:
            pg_cur.execute(
                "INSERT INTO appointments (patient_id, doctor_id, appointment_date, complaints) VALUES (%s, %s, %s, %s) RETURNING id",
                (patient_id, doctor_id, record["app_date"], record["complaints"])
            )
            appointment_id = pg_cur.fetchone()[0]

        pg_cur.execute(
            "SELECT 1 FROM appointment_diagnoses WHERE appointment_id = %s AND diagnosis_id = %s",
            (appointment_id, diagnosis_id)
        )
        if not pg_cur.fetchone():
            pg_cur.execute(
                "INSERT INTO appointment_diagnoses (appointment_id, diagnosis_id) VALUES (%s, %s)",
                (appointment_id, diagnosis_id)
            )

    pg_con.commit()
    pg_cur.close()
    pg_con.close()
    sqlite_cur.close()
    sqlite_con.close()


import_data()
