import argparse
import json
import socket
import ssl
import base64
import yaml
import threading
from datetime import datetime

import pika
import psycopg2

from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

def load_config(path="config.yaml"):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def load_rsa_privkey(path):
    with open(path, "rb") as f:
        return serialization.load_pem_private_key(f.read(), password=None)

def rsa_decrypt(privkey, ciphertext: bytes) -> bytes:
    return privkey.decrypt(
        ciphertext,
        padding.OAEP(
            mgf=padding.MGF1(algorithm=hashes.SHA256()),
            algorithm=hashes.SHA256(),
            label=None
        )
    )

def decrypt_custom(privkey_path, payload):
    enc_key = base64.b64decode(payload["encrypted_key_b64"])
    iv = base64.b64decode(payload["iv_b64"])
    ct = base64.b64decode(payload["ciphertext_b64"])
    priv = load_rsa_privkey(privkey_path)
    aes_key = rsa_decrypt(priv, enc_key)
    aesgcm = AESGCM(aes_key)
    pt = aesgcm.decrypt(iv, ct, None)
    return pt

def process_message(raw_bytes, cfg):
    try:
        obj = json.loads(raw_bytes.decode("utf-8"))
    except Exception as e:
        print("Invalid JSON:", e)
        return None
    scheme = obj.get("scheme")
    payload = obj.get("payload")
    if scheme == "custom":
        privkey_path = cfg.get("crypto", {}).get("importer_privkey_path")
        if not privkey_path:
            raise RuntimeError("No importer_privkey_path configured")
        plaintext = decrypt_custom(privkey_path, payload)
    elif scheme == "tls" or scheme == "plain":
        b64 = payload.get("plaintext_b64")
        plaintext = base64.b64decode(b64)
    else:
        raise RuntimeError("Unknown scheme: %s" % scheme)
    try:
        data = json.loads(plaintext.decode("utf-8"))
    except Exception as e:
        print("Failed to parse inner JSON:", e)
        return None
    return data

def get_db_conn(cfg):
    pg = cfg.get("postgres", {})
    conn = psycopg2.connect(
        dbname=pg["dbname"],
        user=pg["user"],
        password=pg["password"],
        host=pg.get("host", "127.0.0.1"),
        port=pg.get("port", 5432)
    )
    conn.autocommit = True
    return conn

def parse_datetime(s):
    if s is None:
        return None
    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            pass
    return None

def get_or_create_patient(cur, full_name, birth_date):
    cur.execute(
        """
        INSERT INTO patients (full_name, birth_date)
        VALUES (%s, %s)
        ON CONFLICT (full_name, birth_date) DO UPDATE SET full_name = EXCLUDED.full_name
        RETURNING id
        """,
        (full_name, birth_date)
    )
    r = cur.fetchone()
    return r[0] if r else None

def get_or_create_doctor(cur, full_name, specialization):
    cur.execute(
        """
        INSERT INTO doctors (full_name, specialization)
        VALUES (%s, %s)
        ON CONFLICT (full_name, specialization) DO UPDATE SET full_name = EXCLUDED.full_name
        RETURNING id
        """,
        (full_name, specialization)
    )
    r = cur.fetchone()
    return r[0] if r else None

def get_or_create_department(cur, name):
    if not name:
        return None
    cur.execute(
        "INSERT INTO departments (name) VALUES (%s) ON CONFLICT (name) DO NOTHING RETURNING id",
        (name,)
    )
    r = cur.fetchone()
    if r:
        return r[0]
    cur.execute("SELECT id FROM departments WHERE name = %s", (name,))
    rr = cur.fetchone()
    return rr[0] if rr else None

def get_or_create_diagnosis(cur, name):
    if not name:
        return None
    cur.execute(
        "INSERT INTO diagnoses (name) VALUES (%s) ON CONFLICT (name) DO NOTHING RETURNING id",
        (name,)
    )
    r = cur.fetchone()
    if r:
        return r[0]
    cur.execute("SELECT id FROM diagnoses WHERE name = %s", (name,))
    rr = cur.fetchone()
    return rr[0] if rr else None

def get_or_create_appointment(cur, patient_id, doctor_id, department_id, appointment_datetime, complaints, diagnosis_id):
    cur.execute(
        """
        INSERT INTO appointments (patient_id, doctor_id, department_id, appointment_date, complaints)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (patient_id, doctor_id, appointment_date) DO NOTHING
        RETURNING id
        """,
        (patient_id, doctor_id, department_id, appointment_datetime, complaints)
    )
    r = cur.fetchone()
    if r:
        return r[0]
    cur.execute(
        "SELECT id FROM appointments WHERE patient_id = %s AND doctor_id = %s AND appointment_date = %s",
        (patient_id, doctor_id, appointment_datetime)
    )
    rr = cur.fetchone()
    return rr[0] if rr else None

def apply_normalization_and_insert(conn, row):
    with conn.cursor() as cur:
        try:
            patient_id = get_or_create_patient(cur, row.get("patient_full_name"), row.get("patient_birth_date"))
            doctor_id = get_or_create_doctor(cur, row.get("doctor_full_name"), row.get("doctor_specialization"))
            dept_id = get_or_create_department(cur, row.get("department_name"))
            diag_id = get_or_create_diagnosis(cur, row.get("diagnosis_name"))
            appt_dt = parse_datetime(row.get("appointment_date"))
            complaints = row.get("complaints")
            appt_id = get_or_create_appointment(cur, patient_id, doctor_id, dept_id, appt_dt, complaints, diag_id)

            if appt_id and diag_id:
                cur.execute("INSERT INTO appointment_diagnoses (appointment_id, diagnosis_id) VALUES (%s, %s) ON CONFLICT DO NOTHING", (appt_id, diag_id))
            return appt_id
        except Exception as e:
            print("Normalization DB error:", e)
            conn.rollback()
            return None

def socket_worker(client_sock, addr, cfg, db_conn):
    try:
        buf = b""
        while True:
            data = client_sock.recv(4096)
            if not data:
                break
            buf += data
            while b"\n" in buf:
                line, buf = buf.split(b"\n", 1)
                if not line.strip():
                    continue
                row = process_message(line, cfg)
                if row is not None:
                    apply_normalization_and_insert(db_conn, row)
    except Exception as e:
        print("Connection handling error:", e)
    finally:
        client_sock.close()

def run_socket_server(cfg):
    host = cfg.get("socket", {}).get("host", "0.0.0.0")
    port = cfg.get("socket", {}).get("port", 9000)
    use_tls = cfg.get("use_tls", False)
    if use_tls:
        certfile = cfg.get("tls", {}).get("certfile")
        keyfile = cfg.get("tls", {}).get("keyfile")
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(certfile=certfile, keyfile=keyfile)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind((host, port))
    sock.listen(5)
    print(f"Listening on {host}:{port} (tls={use_tls})")
    db_conn = get_db_conn(cfg)
    try:
        while True:
            client, addr = sock.accept()
            if use_tls:
                try:
                    client = context.wrap_socket(client, server_side=True)
                except Exception as e:
                    print("TLS wrap failed:", e)
                    client.close()
                    continue
            t = threading.Thread(target=socket_worker, args=(client, addr, cfg, db_conn), daemon=True)
            t.start()
    finally:
        db_conn.close()
        sock.close()

def on_rabbit_message(ch, method, properties, body, cfg, db_conn):
    row = process_message(body, cfg)
    if row is not None:
        apply_normalization_and_insert(db_conn, row)
    ch.basic_ack(delivery_tag=method.delivery_tag)

def run_rabbit_consumer(cfg):
    url = cfg.get("rabbitmq", {}).get("url")
    queue = cfg.get("rabbitmq", {}).get("queue", "psu_lab_queue")
    if not url:
        raise RuntimeError("RabbitMQ URL not configured")
    params = pika.URLParameters(url)
    conn = pika.BlockingConnection(params)
    channel = conn.channel()
    channel.queue_declare(queue=queue, durable=True)
    db_conn = get_db_conn(cfg)
    on_message = lambda ch, method, properties, body: on_rabbit_message(ch, method, properties, body, cfg, db_conn)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=queue, on_message_callback=on_message)
    print("RabbitMQ consumer started, queue=", queue)
    try:
        channel.start_consuming()
    finally:
        db_conn.close()
        conn.close()

# ---------------- main ----------------
def main():
    parser = argparse.ArgumentParser(description="Importer: accept messages and insert into normalized DB")
    parser.add_argument("--config", default="importer_config.yaml")
    args = parser.parse_args()
    cfg = load_config(args.config)
    mode = cfg.get("mode", "socket")
    if mode == "socket":
        run_socket_server(cfg)
    elif mode == "rabbitmq":
        run_rabbit_consumer(cfg)
    else:
        raise RuntimeError("Unknown mode: %s" % mode)

if __name__ == "__main__":
    main()