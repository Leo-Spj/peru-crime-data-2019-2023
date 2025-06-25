import pandas as pd
import os
import glob
from datetime import datetime

def normalizar_datos_delitos(ruta_csv_original):
    print(f"Cargando datos desde: {ruta_csv_original}")
    df_original = pd.read_csv(ruta_csv_original)
    
    # Debug: Mostrar columnas
    print("Columnas detectadas en el CSV:")
    print(df_original.columns.tolist())
    
    # Valores por defecto para columnas críticas
    columnas_criticas = {
        'Fecha_descarga': datetime(1900, 1, 1),
        'anio_denuncia': 0,
        'periodo_denuncia': 'DESCONOCIDO',
        'fecha_corte': datetime(1900, 1, 1),
        'generico': 'DESCONOCIDO',
        'subgenerico': 'DESCONOCIDO',
        'articulo': 'DESCONOCIDO',
        'des_articulo': 'DESCONOCIDO',
        'ubigeo_pjfs': '000000',
        'distrito_fiscal': 'DESCONOCIDO',
        'dpto_pjfs': 'DESCONOCIDO',
        'prov_pjfs': 'DESCONOCIDO',
        'dist_pjfs': 'DESCONOCIDO',
        'tipo_caso': 'DESCONOCIDO',
        'especialidad': 'DESCONOCIDO',
        'cantidad': 0
    }
    
    # Asegurar existencia de columnas
    for col, default in columnas_criticas.items():
        if col not in df_original.columns:
            print(f"Advertencia: Columna '{col}' no encontrada. Se crea con valores por defecto.")
            df_original[col] = default
    
    # Convertir fechas y manejar nulos
    for col in ['Fecha_descarga', 'fecha_corte']:
        df_original[col] = pd.to_datetime(df_original[col], format='%d/%m/%Y', errors='coerce')
        df_original[col] = df_original[col].fillna(datetime(1900, 1, 1))
    
    # Rellenar nulos en otras columnas
    for col in ['generico', 'subgenerico', 'articulo', 'des_articulo', 
                'tipo_caso', 'especialidad', 'distrito_fiscal', 
                'dpto_pjfs', 'prov_pjfs', 'dist_pjfs']:
        df_original[col] = df_original[col].fillna('DESCONOCIDO')
    
    df_original['ubigeo_pjfs'] = df_original['ubigeo_pjfs'].fillna('000000').astype(str).str.zfill(6)
    df_original['cantidad'] = df_original['cantidad'].fillna(0)
    
    print(f"Preprocesamiento completado. {len(df_original)} filas.")
    return df_original

if __name__ == "__main__":
    output_dir = 'normalizados'
    os.makedirs(output_dir, exist_ok=True)
    
    # Mapeos globales y almacenamiento
    tiempo_map = {}
    delito_map = {}
    ubicacion_map = {}
    tipocaso_map = {}
    
    # Contadores globales
    next_tiempo_id = 1
    next_delito_id = 1
    next_ubicacion_id = 1
    next_tipocaso_id = 1
    next_denuncia_id = 1
    
    # Listas para almacenar datos finales
    all_dim_tiempo = []
    all_dim_delito = []
    all_dim_ubicacion = []
    all_dim_tipocaso = []
    all_fact_denuncias = []
    
    # Procesar todos los archivos
    for csv_input_file in sorted(glob.glob('./[0-9][0-9][0-9][0-9].csv')):
        print(f"\nProcesando archivo: {csv_input_file}")
        try:
            df = normalizar_datos_delitos(csv_input_file)
            
            for _, row in df.iterrows():
                # Procesar Dim_Tiempo
                tiempo_key = (
                    row['Fecha_descarga'],
                    row['anio_denuncia'],
                    row['periodo_denuncia'],
                    row['fecha_corte']
                )
                if tiempo_key not in tiempo_map:
                    tiempo_map[tiempo_key] = next_tiempo_id
                    all_dim_tiempo.append({
                        'id_tiempo': next_tiempo_id,
                        'Fecha_descarga': row['Fecha_descarga'],
                        'anio_denuncia': row['anio_denuncia'],
                        'periodo_denuncia': row['periodo_denuncia'],
                        'fecha_corte': row['fecha_corte']
                    })
                    next_tiempo_id += 1
                tiempo_id = tiempo_map[tiempo_key]
                
                # Procesar Dim_Delito
                delito_key = (
                    row['generico'],
                    row['subgenerico'],
                    row['articulo'],
                    row['des_articulo']
                )
                if delito_key not in delito_map:
                    delito_map[delito_key] = next_delito_id
                    all_dim_delito.append({
                        'id_delito': next_delito_id,
                        'generico': row['generico'],
                        'subgenerico': row['subgenerico'],
                        'articulo': row['articulo'],
                        'des_articulo': row['des_articulo']
                    })
                    next_delito_id += 1
                delito_id = delito_map[delito_key]
                
                # Procesar Dim_Ubicacion
                ubicacion_key = (
                    row['ubigeo_pjfs'],
                    row['distrito_fiscal'],
                    row['dpto_pjfs'],
                    row['prov_pjfs'],
                    row['dist_pjfs']
                )
                if ubicacion_key not in ubicacion_map:
                    ubicacion_map[ubicacion_key] = next_ubicacion_id
                    all_dim_ubicacion.append({
                        'id_ubicacion': next_ubicacion_id,
                        'ubigeo_pjfs': row['ubigeo_pjfs'],
                        'distrito_fiscal': row['distrito_fiscal'],
                        'dpto_pjfs': row['dpto_pjfs'],
                        'prov_pjfs': row['prov_pjfs'],
                        'dist_pjfs': row['dist_pjfs']
                    })
                    next_ubicacion_id += 1
                ubicacion_id = ubicacion_map[ubicacion_key]
                
                # Procesar Dim_TipoCaso
                tipocaso_key = (
                    row['tipo_caso'],
                    row['especialidad']
                )
                if tipocaso_key not in tipocaso_map:
                    tipocaso_map[tipocaso_key] = next_tipocaso_id
                    all_dim_tipocaso.append({
                        'id_tipo_caso': next_tipocaso_id,
                        'tipo_caso': row['tipo_caso'],
                        'especialidad': row['especialidad']
                    })
                    next_tipocaso_id += 1
                tipocaso_id = tipocaso_map[tipocaso_key]
                
                # Crear registro en tabla de hechos
                all_fact_denuncias.append({
                    'id_denuncia': next_denuncia_id,
                    'id_tiempo': tiempo_id,
                    'id_delito': delito_id,
                    'id_ubicacion': ubicacion_id,
                    'id_tipo_caso': tipocaso_id,
                    'cantidad': row['cantidad']
                })
                next_denuncia_id += 1
                
        except Exception as e:
            print(f"Error procesando archivo {csv_input_file}: {str(e)}")
    
    # Guardar resultados
    pd.DataFrame(all_dim_tiempo).to_csv(os.path.join(output_dir, 'Dim_Tiempo.csv'), index=False)
    pd.DataFrame(all_dim_delito).to_csv(os.path.join(output_dir, 'Dim_Delito.csv'), index=False)
    pd.DataFrame(all_dim_ubicacion).to_csv(os.path.join(output_dir, 'Dim_Ubicacion.csv'), index=False)
    pd.DataFrame(all_dim_tipocaso).to_csv(os.path.join(output_dir, 'Dim_TipoCaso.csv'), index=False)
    pd.DataFrame(all_fact_denuncias).to_csv(os.path.join(output_dir, 'Fact_Denuncias.csv'), index=False)
    
    print("\nProceso completado exitosamente!")
    print(f"Dimensiones guardadas en: {output_dir}")
    print(f"Total registros en Fact_Denuncias: {len(all_fact_denuncias)}")