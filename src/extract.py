import glob
import pandas as pd
from src.utils import log

#presion","diametro","tipogas","fechapuesta","estado","profundidad","codtramant","material","calificacion","propiedad","empresa","longitud","geometria_wkt"

def extract_from_csv(file_to_process):
    return pd.read_csv(file_to_process)

def extract():
    # Definimos las columnas que esperamos
    cols = ["presion","diametro","tipogas","fechapuesta","estado","profundidad",
            "codtramant","material","calificacion","propiedad","empresa","longitud","geometria_wkt"]
    
    extracted_data = pd.DataFrame(columns=cols)
    
    # Buscamos archivos CSV en la carpeta data/
    # Es mejor que tus archivos originales estén en una carpeta llamada data
    archivos = glob.glob("data/*.csv")
    
    for csvfile in archivos:
        data = extract_from_csv(csvfile)
        extracted_data = pd.concat([extracted_data, data], ignore_index=True)
        log(f"Data extraida correctamente de: {csvfile}")
            
    return extracted_data