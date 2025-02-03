from pyspark import SparkContext
import re
import json

# Initialiser le contexte Spark
sc = SparkContext(appName="AnalyseLogsApacheRDD")

# Charger les logs depuis HDFS
log_file = "hdfs://sparkhdfs-namenode-1:9000/logs/web_server.log"
logs_rdd = sc.textFile(log_file)

# APicher les 10 premières lignes
print("Exemple de lignes du fichier de logs :")
for line in logs_rdd.take(10):
    print(line)


# Définir le pattern regex pour parser les logs
log_pattern = r'(\d+\.\d+\.\d+\.\d+) - - \[(.*?)\] "(.*?) (.*?) HTTP.*" (\d+) (\d+)'
# Fonction pour parser une ligne de log
def parse_log_line(line):
    match = re.match(log_pattern, line)
    if match:
        return (
            match.group(1), # IP
            match.group(2), # Timestamp
            match.group(3), # HTTP Method
            match.group(4), # URL
            int(match.group(5)), # HTTP Status
            int(match.group(6)) # Response Size
        )
    else:
        return None

# Parser les logs
parsed_logs_rdd = logs_rdd.map(parse_log_line).filter(lambda x: x is not
None)
'''
# Afficher 10 logs parsés
print("Exemple de logs parsés :")
for log in parsed_logs_rdd.take(10):
    print(log)

total_requests = parsed_logs_rdd.count()
print(f"Nombre total de requêtes : {total_requests}")

top_urls = parsed_logs_rdd.map(lambda x: (x[3], 1)).reduceByKey(lambda a, b: a + b).takeOrdered(5,
key=lambda x: -x[1])
print("Les 5 URLs les plus demandées :")
for url, count in top_urls:
 print(f"{url}: {count}")
'''
'''
nombre de requete pour http
la taille moyenne des reponses par code http
adresse ip la plus active 
'''
most_asked_page = parsed_logs_rdd.map(lambda x: (x[4], 1)).reduceByKey(lambda a, b: a + b).takeOrdered(5,
key=lambda x: -x[1])
print("Les 5 URLs les plus demandées :")
for url, count in most_asked_page:
 print(f"{url}: {count}")

 commonhttpcode = parsed_logs_rdd.map(lambda x: (x[5], 1)).reduceByKey(lambda a, b: a + b).takeOrdered (10,
key=lambda x: -x[1])
print("Les 10 code http les plus frequent :")
for url, count in most_asked_page:
 print(f"{[5]}: {count}")

i = 1
result = {}
with open('web_server') as f:
    lines = f.readlines()
    for line in lines:
        r = line.split('\t\t')
        result[i] = {'timestamp': r[0], 'monitorip': r[1], 'monitorhost': r[2], 'monitorstatus': r[3], 'monitorid': r[4], 'resolveip': r[5]}
        i += 1 
print(result) 
with open('data.json', 'w') as fp:
    json.dump(result, fp)