import datetime
import os

def log(message, log_file='logs/logfile.txt'):
    # Creamos la carpeta logs si no existe
    os.makedirs('logs', exist_ok=True)
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(log_file, "a") as f:
        f.write(f"{timestamp} - {message}\n")