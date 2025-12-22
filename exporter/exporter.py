import argparse
import json
import socket
import ssl
import base64
import sqlite3
import time
import os

import yaml
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding
import pika

def load_config(path="config.yaml"):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def load_rsa_pubkey(path):
    with open(path, "rb") as f:
        data = f.read()
        return serialization.load_pem_public_key(data)

def rsa_encrypt(pubkey, plaintext: bytes) -> bytes:
    return pubkey.encrypt(
        plaintext,
        padding.OAEP(
            mgf=padding.MGF1(algorithm=hashes.SHA256()),
            algorithm=hashes.SHA256(),
            label=None
        )
    )

def encrypt_custom(pubkey_path, plaintext_bytes: bytes):
    aes_key = AESGCM.generate_key(bit_length=256)
    aesgcm = AESGCM(aes_key)
    iv = os.urandom(12)
    ct = aesgcm.encrypt(iv, plaintext_bytes, None)
    pubkey = load_rsa_pubkey(pubkey_path)
    enc_key = rsa_encrypt(pubkey, aes_key)
    return {
        "encrypted_key_b64": base64.b64encode(enc_key).decode(),
        "iv_b64": base64.b64encode(iv).decode(),
        "ciphertext_b64": base64.b64encode(ct).decode()
    }

def send_via_socket(host, port, message_bytes, config):
    use_tls = config.get("use_tls", False)
    sock = socket.create_connection((host, port), timeout=10)
    if use_tls:
        context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        ca_cert = config.get("tls", {}).get("ca_cert")
        if ca_cert:
            context.load_verify_locations(cafile=ca_cert)
        context.check_hostname = True
        context.verify_mode = ssl.CERT_REQUIRED
        wrapped = context.wrap_socket(sock, server_hostname=host)
        conn = wrapped
    else:
        conn = sock

    conn.sendall(message_bytes + b"\n")
    conn.close()

def publish_rabbitmq(url, queue, message_bytes, config):
    params = pika.URLParameters(url)
    if config.get("rabbitmq", {}).get("use_tls"):
        pass

    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue=queue, durable=True)
    channel.basic_publish(
        exchange='',
        routing_key=queue,
        body=message_bytes,
        properties=pika.BasicProperties(delivery_mode=2)  # persistent
    )
    connection.close()

def iter_rows_from_sqlite(path, table):
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table}")
    for row in cur:
        yield dict(row)
    conn.close()

def main():
    parser = argparse.ArgumentParser(description="Exporter: send denormalized rows to importer")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--source-table", default="hospital_records")
    parser.add_argument("--sqlite", default="hospital_denormalized.db")
    args = parser.parse_args()
    cfg = load_config(args.config)

    mode = cfg.get("mode", "socket")
    host = cfg.get("socket", {}).get("host", "127.0.0.1")
    port = cfg.get("socket", {}).get("port", 9000)
    rabbit_url = cfg.get("rabbitmq", {}).get("url")
    rabbit_queue = cfg.get("rabbitmq", {}).get("queue", "psu_lab_queue")

    use_custom = cfg.get("use_custom_crypto", False)
    rsa_pub = cfg.get("crypto", {}).get("importer_pubkey_path")

    use_tls = cfg.get("use_tls", False)

    for row in iter_rows_from_sqlite(args.sqlite, args.source_table):
        plaintext = json.dumps(row, ensure_ascii=False).encode("utf-8")
        if use_custom:
            payload = encrypt_custom(rsa_pub, plaintext)
            message = {
                "scheme": "custom",
                "payload": payload,
                "meta": {"source_table": args.source_table}
            }
        else:
            message = {
                "scheme": "tls" if use_tls else "plain",
                "payload": {"plaintext_b64": base64.b64encode(plaintext).decode()},
                "meta": {"source_table": args.source_table}
            }
        message_bytes = json.dumps(message, ensure_ascii=False).encode("utf-8")

        if mode == "socket":
            print(host, port)
            send_via_socket(host, port, message_bytes, cfg)
        elif mode == "rabbitmq":
            if not rabbit_url:
                raise RuntimeError("RabbitMQ URL not configured")
            publish_rabbitmq(rabbit_url, rabbit_queue, message_bytes, cfg)
        else:
            raise RuntimeError("Unknown mode: %s" % mode)

        time.sleep(cfg.get("send_interval_sec", 0.1))

if __name__ == "__main__":
    main()