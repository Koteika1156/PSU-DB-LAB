DROP TABLE IF EXISTS appointment_diagnoses;
DROP TABLE IF EXISTS appointments;
DROP TABLE IF EXISTS doctors;
DROP TABLE IF EXISTS departments;
DROP TABLE IF EXISTS patients;
DROP TABLE IF EXISTS diagnoses;

-- 1. Отделения
CREATE TABLE departments (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL -- Уже есть UNIQUE для get_or_create_department
);

-- 2. Врачи
CREATE TABLE doctors (
    id SERIAL PRIMARY KEY,
    full_name VARCHAR(255) NOT NULL,
    specialization VARCHAR(255),
    department_id INTEGER REFERENCES departments(id),
    -- Добавлено ограничение для get_or_create_doctor в importer.py
    CONSTRAINT unique_doctor_spec UNIQUE (full_name, specialization)
);

-- 3. Пациенты
CREATE TABLE patients (
    id SERIAL PRIMARY KEY,
    full_name VARCHAR(255) NOT NULL,
    birth_date DATE,
    -- Добавлено ограничение для get_or_create_patient в importer.py
    CONSTRAINT unique_patient_name_birth UNIQUE (full_name, birth_date)
);

-- 4. Диагнозы
CREATE TABLE diagnoses (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL -- Уже есть UNIQUE для get_or_create_diagnosis
);

-- 5. Приемы
CREATE TABLE appointments (
    id SERIAL PRIMARY KEY,
    patient_id INTEGER REFERENCES patients(id),
    doctor_id INTEGER REFERENCES doctors(id),
    department_id INTEGER REFERENCES departments(id), -- Добавлено поле, так как оно есть в коде
    appointment_date TIMESTAMP,
    complaints TEXT,
    CONSTRAINT unique_appointment_key UNIQUE (patient_id, doctor_id, appointment_date)
);

CREATE TABLE appointment_diagnoses (
    appointment_id INTEGER REFERENCES appointments(id),
    diagnosis_id INTEGER REFERENCES diagnoses(id),
    PRIMARY KEY (appointment_id, diagnosis_id)
);