config = {
    "dbname": "psu",
    "user": "postgres",
    "password": "123",
    "host": "localhost",
    "port": "5432"
}

dtype = [
    ('patient_name', 'U50'), ('patient_dob', 'U10'), ('doctor_name', 'U50'),
    ('doctor_spec', 'U50'), ('department_name', 'U50'), ('app_date', 'U20'),
    ('complaints', 'U100'), ('diagnosis_name', 'U50')
]