import psycopg2
from openpyxl import Workbook
import settings

def create_full_report(filename="report.xlsx", department=None, doctor=None, patient=None, appointment_date=None):
    conn = psycopg2.connect(**settings.config)
    cur = conn.cursor()

    query = """
        SELECT p.full_name,
               TO_CHAR(p.birth_date,'YYYY-MM-DD'),
               d.full_name,
               d.specialization,
               dep.name,
               TO_CHAR(a.appointment_date,'YYYY-MM-DD HH24:MI'),
               a.complaints
        FROM appointments a
        JOIN patients p ON a.patient_id=p.id
        JOIN doctors d ON a.doctor_id=d.id
        JOIN departments dep ON d.department_id=dep.id
    """

    conditions = []
    params = []

    if department:
        conditions.append("dep.name = %s")
        params.append(department)
    if doctor:
        conditions.append("d.full_name = %s")
        params.append(doctor)
    if patient:
        conditions.append("p.full_name = %s")
        params.append(patient)
    if appointment_date:
        conditions.append("TO_CHAR(a.appointment_date,'YYYY-MM-DD') = %s")
        params.append(appointment_date)

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    query += " ORDER BY a.appointment_date;"

    cur.execute(query, params)
    data = cur.fetchall()
    cur.close()
    conn.close()

    wb = Workbook()
    ws = wb.active
    ws.append(["Пациент","Дата рождения","Врач","Специализация","Отделение","Дата приёма","Жалобы"])
    for row in data:
        ws.append(row)
    wb.save(filename)


if __name__ == "__main__":
    create_full_report(
        filename="report_filtered.xlsx",
        department="Терапевтическое отделение",
        doctor="Сидоров Сергей Петрович",
        patient="Иванов Иван Иванович",
        appointment_date="2025-09-11"
    )
