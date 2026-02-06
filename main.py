import pandas as pd 
import os 
from src.extract import extract
from src.transform import pipeline_limpieza
from src.utils import log 


def run_pipeline():
    #
  #creamos carpetas de salida si no existen 
  os.makedirs('output', exist_ok=True)
  os.makedirs('logs', exist_ok=True)

  try :
    print("Iniciamos el Proceso ETL ...")
    data_cruda= extract()

    if data_cruda.empty:
        print("No se encontraron datso apara porcesar.")
        return
    data_final=pipeline_limpieza(data_cruda)

    output_phat="output/tranformed_data.csv"
    data_final.to_csv(output_phat,index=False,encoding="utf-8")
    print(f"Exito ,datos guradados en: {output_phat}")
    print(f"Total de registros procesados: {len(data_final)}")
  
  except Exception as e:
    print(f"Error Critico : {e}")
    log(f"ERROR EN MAIN : {str(e)}")

    

if __name__ == "__main__":
    run_pipeline()