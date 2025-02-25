import time
import random
from datetime import datetime
from kafka import KafkaProducer

# Configuration du producteur Kafka
producer = KafkaProducer(bootstrap_servers="kafka:9092")

with open('web_server.log', 'r') as file:
    logs = file.readlines()
log_index = 0
def generate_log():
    """Génère un log Apache simulé"""
    global log_index

    if log_index < len(logs):
        log=logs[log_index].strip()
        log_index += 1
        return log
    else:
        return None 

# Boucle infinie pour générer les logs
while True:
    log = generate_log()
    if log:
        producer.send("logs", log.encode("utf-8"))
        print(f"Sent log: {log}")
    else:
        print("No more logs to send")
        break
    time.sleep(random.uniform(0.5, 2.0))  # Envoi des logs avec un délai aléatoire entre 0.5s et 2s
