import pandas as pd
import numpy as np 
import re 
from datetime import datetime 
from fractions import Fraction
from src.utils import log



def  cleanPresion(data_presion,fail_fast=False):
    if "presion" not in data_presion.columns:
        msg = "La columna 'presion' no existe en los datos extraídos."
        log(msg)
        if fail_fast:
            raise KeyError(msg)
        else:
            return data_presion
        
    try:
        data_presion["presion_str"] = data_presion["presion"].astype(str).str.strip()
        data_presion["presion_str"] = data_presion["presion_str"].str.lower().str.replace(r"\s+", " ", regex=True)
        data_presion["presion_valor"] = data_presion["presion_str"].str.extract(r"(\d+\.?\d*)")[0]
        data_presion["presion_valor"] = pd.to_numeric(data_presion["presion_valor"],errors="coerce")


        invalid_rows= data_presion[data_presion["presion_valor"].isna()]
        invalid_count= len(invalid_rows)

        if invalid_count>0:
            invalid_rows.head(1000).to_csv("errores_presion.csv",index=False)
            log(f"Datos invalidos Guardados en un scv")
        
        data_presion["presion"]= data_presion["presion_valor"]

        data_presion= data_presion.drop(columns=["presion_valor","presion_str"])

        log(f"Limpieza de 'presion' completada. Filas totales: {len(data_presion):,}")

        return data_presion

    except Exception as e:
        msg = f"ERROR al limpiar columna 'presion': {str(e)}"
        log(msg)
        if fail_fast:
            raise
        else:
            print(msg)
            return data_presion


def cleanDiametro(data, fail_fast=False):
    if 'diametro' not in data.columns:
        msg = "La columna 'diametro' no existe."
        log(msg)
        if fail_fast:
            raise KeyError(msg)
        else:
            return data

    try:
        log("Iniciando limpieza de columna 'diametro'")

        # Copia de seguridad
        data['diametro_raw'] = data['diametro'].astype(str).str.strip()

        # Normaliza texto
        data['diametro_str'] = data['diametro_raw'].str.lower().str.replace(r"\s+", " ", regex=True)

        # Extrae número
        data['valor_raw'] = data['diametro_str'].str.extract(r"(\d+\/?\d*\.?\d*)")[0]

        # Convierte fracciones o decimales
        def convertir_a_decimal(x):
            try:
                return float(Fraction(str(x)))
            except:
                try:
                    return float(x)
                except:
                    return np.nan

        data['valor_num'] = data['valor_raw'].apply(convertir_a_decimal)

        data['unidad_raw'] = data['diametro_str'].str.extract(r"(mm|cm|m|pulg|pulgadas|in|\"|pies|ft)")[0]
        mapa_unidades = {
            'pulg': 'pulgadas',
            'in': 'pulgadas',
            '"': 'pulgadas',
            'pies': 'ft'
        }
        data['unidad'] = data['unidad_raw'].replace(mapa_unidades).infer_objects(copy=False)

        # Convierte a mm
        def convertir_a_mm(valor, unidad):
            if pd.isna(valor):
                return np.nan
            if isinstance(unidad, str):
                unidad = unidad.strip().lower()
            else:
                return valor
            if unidad in ['pulg', 'pulgada', 'pulgadas', 'in', '"']:
                return valor * 25.4
            elif unidad in ['mm', 'milimetro', 'milímetros']:
                return valor
            elif unidad in ['cm', 'centimetro', 'centímetros']:
                return valor * 10
            elif unidad in ['m', 'metro', 'metros']:
                return valor * 1000
            elif pd.isna(unidad):
                return valor
            else:
                return np.nan

        data['diametro_mm'] = data.apply(lambda x: convertir_a_mm(x['valor_num'], x['unidad']), axis=1)

        # Detecta errores
        invalid_rows = data[data['diametro_mm'].isna()]
        invalid_count = len(invalid_rows)

        if invalid_count > 0:
            invalid_rows.to_csv("errores_diametro.csv", index=False, encoding="utf-8")
            log(f" {invalid_count:,} registros inválidos guardados en errores_diametro.csv")
        else:
            log(" No se detectaron registros inválidos en 'diametro'")

        # Limpieza final
        data['diametro'] = data['diametro_mm']
        data = data.drop(columns=['diametro_raw', 'diametro_str', 'valor_raw', 'valor_num', 'unidad_raw', 'unidad', 'diametro_mm'])
        

        log(f" Limpieza de 'diametro' completada. Total filas: {len(data):,}")

        return data

    except Exception as e:
        msg = f" ERROR al limpiar 'diametro': {str(e)}"
        log(msg)
        if fail_fast:
            raise
        else:
            print(msg)
            return data


def cleanTipoGas(data,fail_fast=False):
    try:
        if 'tipogas' not in data.columns:
            msg="Columna 'tipogas' no encontrada en el dataframe"
            log(msg)
            if fail_fast:
                raise KeyError(msg)
            return data
        
        data['tipogas']=(
            data['tipogas']
            .astype(str)
            .str.strip()
            .str.lower()
            .str.replace(r"\s+", " ",regex=True)
        )
        mapa_gases={
            "tub":"tuberia",
            "duc":"gasoducto",
            "tuberia":'tuberia',
            'gasoducto':'gasoducto'

        }

        data['tipogas']=data['tipogas'].replace(mapa_gases)


        no_reconocidos=data[~data['tipogas'].isin(mapa_gases.keys())]['tipogas'].unique()
        if len(no_reconocidos)>0:
            msg=f"Valores no reconocidos en 'Tipogas' :{no_reconocidos}"
            log(msg)
            errores=data[data['tipogas'].isin(no_reconocidos)]
            errores.to_csv('errores_tipogas.csv',index=False)
        msg="limpieza de Tipogas completa correctamente"
        log(msg)
        return data
    
    except Exception as e :
        msg= f"Error al limpiar 'tipogas':{str(e)}"

    
def cleanFechaPuesta(data,columnas_fecha,formatos=None,fail_fast=False):

    if formatos is None:
        formatos = [
            "%Y-%m-%d",    # 2024-05-03
            "%d/%m/%Y",    # 03/05/2024
            "%Y%m%d",      # 20240503
            "%d-%m-%Y",    # 03-05-2024
            "%Y/%m/%d"     # 2024/05/03
           ]
    try:
        for col in columnas_fecha:
             if col not in data.columns:
                 msg=f"Columna {col} no encontrada"
                 log(msg)
                 continue
             log(f"Iniciando limpieza de columna: {col}")

            
             data[col] = data[col].astype(str).str.strip()

            # Intentar convertir con los formatos definidos
             def try_parse_date(x):
                for fmt in formatos:
                    try:
                        return datetime.strptime(x, fmt)
                    except Exception:
                        continue
                return np.nan

             data[col + "_limpia"] = data[col].apply(try_parse_date)

            # Calcular registros inválidos
             invalidos = data[data[col + "_limpia"].isna()]
             if len(invalidos) > 0:
                msg = f" {len(invalidos):,} registros inválidos en '{col}'"
                log(msg)
                invalidos.to_csv(f"errores_{col}.csv", index=False, encoding="utf-8")

            # Reemplazar columna original por la limpia
             data[col] = data[col + "_limpia"]
             data = data.drop(columns=[col + "_limpia"])

             log(f"Limpieza de '{col}' completada. Registros válidos: {data[col].notna().sum():,}")

        return data

    except Exception as e:
        msg = f" Error durante la limpieza de fechas: {str(e)}"
        log(msg)
        if fail_fast:
            raise
        else:
            return data


def cleanEstado(data,fail_fast=False):
    try:
        if 'estado' not in data.columns:
            msg=f"Columna 'estado' no encontrada en el DataFrame"
            log(msg)
            if fail_fast:
                raise KeyError(msg)
            return data
        log("iniciando limpieza de 'Estado'")
        data['estado']=(data['estado']
                        .astype(str)
                        .str.strip()
                        .str.lower()
                        .str.replace(r"\s+", " ",regex=True))
        prew={
            'alta':'alta',
            'existente':'existente',
            'modificado':'modificado',
            'baja':'baja',
            'removido':'removido'

        }
       
        no_reconocidos=data[~data['estado'].isin(prew.keys())]['estado'].unique()

        if len(no_reconocidos)>0:
            msg=f"Valores no reconocidos en 'estado' :{no_reconocidos}"
            log(msg)
            errores=data[data['estado'].isin(no_reconocidos)]
            errores.to_csv('errores_esatdo',index=False)


        msg="limpieza de estado completa correctamente"
        log(msg)
        return data
     
    except Exception as e:
        msg=f"ERROR al limpiar 'estado': {str(e)}" 



def cleanProfundidad(data, fail_fast=False):
    if "profundidad" not in data.columns:
        msg = "La columna 'profundidad' no existe."
        log(msg)
        if fail_fast:
            raise KeyError(msg)
        else:
            return data

    try:
        log("Iniciando limpieza de columna 'profundidad'")

        # Copia de seguridad
        data['profundidad_raw'] = data['profundidad'].astype(str).str.strip()

        # Normaliza texto
        data['profundidad_str'] = data['profundidad_raw'].str.lower().str.replace(r"\s+", " ", regex=True)

        # Extrae número
        data['valor_raw'] = data['profundidad_str'].str.extract(r"(\d+\/?\d*\.?\d*)")[0]

        # Convierte  o decimales
        def convertir_a_decimal(x):
            if pd.isna(x):
                return np.nan
            try:
                x_str=str(x).replace(",",".")
                return float(x_str)
            except ValueError:
                return np.nan
            



        invalid_rows = data[data['valor_raw'].isna()]
        invalid_count = len(invalid_rows)

        if invalid_count > 0:
            invalid_rows.to_csv("errores_profundidad.csv", index=False, encoding="utf-8")
            log(f" {invalid_count:,} registros inválidos guardados en errores_profundidad.csv")
        else:
            log(" No se detectaron registros inválidos en profundidad")

    
        data['valor_num'] = data['valor_raw'].apply(convertir_a_decimal)
        data['profundidad']=data['valor_num']

        data=data.drop(columns=['valor_num','valor_raw','profundidad_str','profundidad_raw'])


        log(f" Limpieza de 'diametro' completada. Total filas: {len(data):,}")
        return data

    except Exception as e:
        msg = f" ERROR al limpiar 'profundida': {str(e)}"
        log(msg)
        if fail_fast:
            raise
        else:
            print(msg)
            return data


def limpiar_codigos_unicos(data,fail_fast=False):
   
    try:
        if 'codtramant' not in data.columns:
            msg = "Columna 'codtramant' no encontrada en el DataFrame."
            log(msg)
            if fail_fast:
                raise KeyError(msg)
            else:
                return data

        log(" Iniciando limpieza de la columna única: 'codtramant'")

        
        data['codtramant'] = (
            data['codtramant']
            .astype(str)
            .str.strip()
            .str.lower() 
            .str.replace(r"[^A-Z0-9]", "", regex=True)
        )

        # Reemplazar vacíos por NaN
        data['codtramant'] = data['codtramant'].replace("", np.nan)

        
        duplicados = data[data['codtramant'].duplicated(keep=False)]
        total = len(data)

        if len(duplicados) > 0:
            log(f"Total registros: {total}")
            log(f"Se encontraron {len(duplicados):,} duplicados en 'codtramant'")
            duplicados.to_csv("duplicados_codtramant.csv", index=False, encoding="utf-8")

            log("Eliminando duplicados de 'codtramant'")
            if 'fechapuesta' in data.columns:
                # Conservar el registro más reciente según 'fechapuesta'
                data = (
                    data.sort_values('fechapuesta', ascending=False)
                        .drop_duplicates(subset='codtramant', keep='first')
                )
            else:
                # Conservar la fila con menos nulos si no hay fecha
                data['nulos'] = data.isna().sum(axis=1)
                data = (
                    data.sort_values('nulos')
                        .drop_duplicates(subset='codtramant', keep='first')
                        .drop(columns='nulos')
                )

   
        nulos = data[data['codtramant'].isna()]
        if len(nulos) > 0:
            log(f"Se encontraron {len(nulos):,} valores nulos en 'codtramant'")
            nulos.to_csv("nulos_codtramant.csv", index=False, encoding="utf-8")

    
        data = data.dropna(subset=['codtramant'])
        data = data.drop_duplicates(subset=['codtramant'], keep='first')

        log(f"Limpieza de 'codtramant' completada. Total registros únicos: {data['codtramant'].nunique():,}")

        return data

    except Exception as e:
        msg = f" Error al limpiar codtramant: {str(e)}"
        log(msg)
        if fail_fast:
            raise
        else:
            return data

def cleanMaterial(data,fail_fast=False):
    try:
        if 'material' not in data.columns:
            msg="Columna 'material' no encontrada en el dataframe"
            log(msg)
            if fail_fast:
                raise KeyError(msg)
            return data
        log("iniciando limpieza de 'Material'")
        data['material']=(
            data['material']
            .astype(str)
            .str.strip()
            .str.lower()
            .str.replace(r"\s+", " ",regex=True)
        )
        mapa_gases={
            "polietileno":"polietileno",
            "acero":"acero",
            "pe comp dh":"pol_doble_hoja",
            "pe comp dv":"pol_doble_balbula",
            "pol_doble_hoja":"pol_doble_hoja",
            "pol_doble_balbula":"pol_doble_balbula"


        }

        data['material']=data['material'].replace(mapa_gases)
        data['material'] = data['material'].astype(str).str.strip().str.lower()
        

        no_reconocidos=data[~data['material'].isin(mapa_gases.keys())]['material'].unique()
        if len(no_reconocidos)>0:
            msg=f"Valores no reconocidos en 'material' :{no_reconocidos}"
            log(msg)
            errores=data[data['material'].isin(no_reconocidos)]
            errores.to_csv('errores_material.csv',index=False)

        
        msg="limpieza de material completa correctamente"
        log(msg)
        return data
    
    except Exception as e :
        msg= f"Error al limpiar 'material':{str(e)}"
        return data


def cleanCalificacion(data,fail_fast=False):
    try:
        if 'calificacion' not in data.columns:
            msg=f"Columna 'calificacion' no encontrada en el DataFrame"
            log(msg)
            if fail_fast:
                raise KeyError(msg)
            return data
        msg("Inicio de limpieza de 'calificacion '")

        data['calificacion']=(data['calificacion']
                        .astype(str)
                        .str.strip()
                        .str.lower()
                        .str.replace(r"\s+", " ",regex=True))
        prew={
            'red única':'red unica',
            'otras redes':'otras redes',
            'red principal':'red principal'
            
        }

        data['calificacion']=data['calificacion'].replace(prew)
       
        no_reconocidos=data[~data['calificacion'].isin(prew.keys())]['calificacion'].unique()
        data['calificacion'] = data['calificacion'].astype(str).str.strip().str.lower()
        if len(no_reconocidos)>0:
            msg=f"Valores no reconocidos en 'calificacion' :{no_reconocidos}"
            log(msg)
            errores=data[data['calificacion'].isin(no_reconocidos)]
            errores.to_csv('errores_calificacion.csv',index=False)


        msg="limpieza de clasificacion completa correctamente"
        log(msg)
        return data
    
    except Exception as e:
        msg = f" Error durante la limpieza de calificacion: {str(e)}"
        log(msg)
        if fail_fast:
            raise
        else:
            return data



def cleanPropiedad(data,fail_fast=False):
    try:
        if 'propiedad' not in data.columns:
            msg=f"Columna 'propiedad' no encontrada en el DataFrame"
            log(msg)
            if fail_fast:
                raise KeyError(msg)
            return data
        
        data['propiedad']=(data['propiedad']
                        .astype(str)
                        .str.strip()
                        .str.lower()
                        .str.replace(r"\s+", " ",regex=True))
        prew={
            'distribuidora':'distribuidora',
            'fise':'fise'
          
        }
       
        no_reconocidos=data[~data['propiedad'].isin(prew.keys())]['propiedad'].unique()

        if len(no_reconocidos)>0:
            msg=f"Valores no reconocidos en 'propiedad' :{no_reconocidos}"
            log(msg)
            errores=data[data['propiedad'].isin(no_reconocidos)]
            errores.to_csv('errores_propiedad.csv',index=False)


        msg="limpieza de propiedad completa correctamente"
        log(msg)
        return data
    
    except Exception as e:
        msg = f" Error durante la limpieza de propiedad: {str(e)}"
        log(msg)
        if fail_fast:
            raise
        else:
            return data


def cleanEmpresa(data,fail_fast=False):
    try:
        if 'empresa' not in data.columns:
            msg=f"Columna 'empresa' no encontrada en el DataFrame"
            log(msg)
            if fail_fast:
                raise KeyError(msg)
            return data
        
        data['empresa']=(data['empresa']
                        .astype(str)
                        .str.strip()
                        .str.lower()
                        .str.replace(r"\s+", " ",regex=True))
        prew={
            'gnlc':'gnlc',
            'cont': 'cont',
            'gapa': 'gapa',
            'gnor':'gnor',
            'ptpe':'ptpe'
          
        }
       
        no_reconocidos=data[~data['empresa'].isin(prew.keys())]['empresa'].unique()

        if len(no_reconocidos)>0:
            msg=f"Valores no reconocidos en 'empresa' :{no_reconocidos}"
            log(msg)
            errores=data[data['empresa'].isin(no_reconocidos)]
            errores.to_csv('errores_empresa.csv',index=False)


        msg="limpieza de empresa completa correctamente"
        log(msg)
        return data
    
    except Exception as e:
        msg = f" Error durante la limpieza de empresa: {str(e)}"
        log(msg)
        if fail_fast:
            raise
        else:
            return data


def cleanlongitud(data,fail_fast=False):
    if "longitud" not in data.columns:
        msg = "La columna 'longitud' no existe."
        log(msg)
        if fail_fast:
            raise KeyError(msg)
        else:
            return data

    try:
        log("Iniciando limpieza de columna 'longitud'")

        # Copia de seguridad
        data['longitud_raw'] = data['longitud'].astype(str).str.strip()

        # Normaliza texto
        data['longitud_str'] = data['longitud_raw'].str.lower().str.replace(r"\s+", " ", regex=True)

        # Extrae número
        data['valor_raw'] = data['longitud_str'].str.extract(r"(\d+\/?\d*\.?\d*)")[0]

        # Convierte  decimales
        def convertir_a_decimal(x):
            if pd.isna(x):
                return np.nan
            try:
                x_str=str(x).replace(",",".")
                return float(x_str)
            except ValueError:
                return np.nan


        data['valor_num'] = data['valor_raw'].apply(convertir_a_decimal)
        data['longitud']=data['valor_num']

        data=data.drop(columns=['valor_num','valor_raw','longitud_str','longitud_raw'])
        log("limpieza de longitud correctamente ")
        return data
    
        

    except Exception as e:
        msg = f" ERROR al limpiar 'longitud': {str(e)}"
        log(msg)
        if fail_fast:
            raise
        else:
            print(msg)
            return data


def cleanGeo(wkt):
    if pd.isna(wkt):
        return pd.Series({
            'lon_inicio': np.nan,
            'lat_inicio': np.nan,
            'lon_fin': np.nan,
            'lat_fin': np.nan
        })
    
    # Limpieza básica
    wkt = str(wkt).strip()
    wkt = re.sub(r"\s+", " ", wkt)
    
    # Verificar formato LINESTRING
    match = re.match(r"^LINESTRING\s*\((.+)\)$", wkt)
    if not match:
        return pd.Series({
            'lon_inicio': np.nan,
            'lat_inicio': np.nan,
            'lon_fin': np.nan,
            'lat_fin': np.nan
        })
    
    try:
        # Extraer lista de coordenadas
        coords_str = match.group(1)
        puntos = [p.strip() for p in coords_str.split(",")]
        
        # Primer punto
        lon_ini, lat_ini = map(float, puntos[0].split())
        
        # Último punto
        lon_fin, lat_fin = map(float, puntos[-1].split())
        
        return pd.Series({
            'lon_inicio': lon_ini,
            'lat_inicio': lat_ini,
            'lon_fin': lon_fin,
            'lat_fin': lat_fin
        })
    except:
        return pd.Series({
            'lon_inicio': np.nan,
            'lat_inicio': np.nan,
            'lon_fin': np.nan,
            'lat_fin': np.nan
        })




def pipeline_limpieza(data):
    log("Iniciando el pipeline de limpieza  ...")
    data = cleanPresion(data)
    data = cleanDiametro(data)
    data = cleanTipoGas(data)
    data = cleanFechaPuesta(data, columnas_fecha=["fechapuesta"])
    data = cleanEstado(data)
    data = cleanProfundidad(data)
    data = limpiar_codigos_unicos(data)
    data = cleanMaterial(data)
    data = cleanCalificacion(data)
    data = cleanPropiedad(data)
    data = cleanEmpresa(data)
    data = cleanlongitud(data)

    #aplicar limpieza geografica 
    log("Extrayendo coordenadas de Geometria WKT")
    geo_df = data['geometria_wkt'].apply(cleanGeo)
    data = pd.concat([data, geo_df], axis=1)

    # Eliminar columnas que ya no sirven
    cols_drop = ['geometria_wkt','fecha_corte','fecha_emision','codtramo']
    data = data.drop(columns=[c for c in cols_drop if c in data.columns])

    log("Pipeline de limpieza completado.")
    return data