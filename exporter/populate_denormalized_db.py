import sqlite3

records = [
    ('Иванов Иван Иванович', '1985-04-12', 'Сидоров Сергей Петрович', 'Терапевт', 'Терапевтическое отделение', '2025-09-10 10:00', 'Кашель, температура', 'ОРВИ'),
    ('Смирнов Иван Иванович', '1992-07-21', 'Кузнецова Елена Васильевна', 'Хирург', 'Хирургическое отделение', '2025-09-10 12:30', 'Боль в правом боку', 'Аппендицит'),
    ('Иванов Иван Иванович', '1985-04-12', 'Сидоров Сергей Петрович', 'Терапевт', 'Терапевтическое отделение', '2025-09-15 11:00', 'Плановый осмотр', 'Здоров'),
    ('Васильев Евгений Семёнович', '1978-11-30', 'Орлова Анна Михайловна', 'Кардиолог', 'Кардиологическое отделение', '2025-09-11 09:00', 'Боль в груди', 'Стенокардия'),
    ('Петрова Анна Игоревна', '1992-07-21', 'Кузнецова Елена Васильевна', 'Хирург', 'Хирургическое отделение', '2025-09-17 15:00', 'Послеоперационный осмотр', 'Восстановление')
]

con = sqlite3.connect('hospital_denormalized.db')
cur = con.cursor()

cur.execute('DROP TABLE IF EXISTS hospital_records')

cur.execute('''
    CREATE TABLE hospital_records (
        patient_full_name TEXT,
        patient_birth_date TEXT,
        doctor_full_name TEXT,
        doctor_specialization TEXT,
        department_name TEXT,
        appointment_date TEXT,
        complaints TEXT,
        diagnosis_name TEXT
    )
''')

cur.executemany(
    'INSERT INTO hospital_records VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
    records
)

con.commit()
con.close()
