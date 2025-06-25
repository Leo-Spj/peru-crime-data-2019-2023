import pandas as pd
import numpy as np
import os
import glob

def normalizar_datos_delitos(ruta_csv_original):
    print(f"Cargando datos desde: {ruta_csv_original}")
    df_original = pd.read_csv(ruta_csv_original)

    # --- AÑADE ESTA LÍNEA PARA DEBUGGING ---
    print("Columnas detectadas en el CSV:")
    print(df_original.columns)
    # ----------------------------------------

    # --- Limpieza y preparación inicial (si es necesaria) ---
    # Convertir columnas de fecha a formato datetime
    for col in ['Fecha_descarga', 'fecha_corte']:
        if col in df_original.columns: # Mantén este 'if' para evitar el error si la columna realmente falta después de esta verificación
            # Intentar varios formatos si es necesario, ddmmyyyy, mmddyyyy etc.
            # Aquí asumimos dd/mm/aaaa por la descripción del prompt
            df_original[col] = pd.to_datetime(df_original[col], format='%d/%m/%Y', errors='coerce')
        else:
            print(f"Advertencia: La columna '{col}' no se encontró en el CSV. Saltando conversión a fecha.")


    # Rellenar valores 'S/Art' o vacíos en 'articulo' y 'des_articulo' si es un problema para la unicidad
    df_original['articulo'] = df_original['articulo'].fillna('DESCONOCIDO')
    df_original['des_articulo'] = df_original['des_articulo'].fillna('DESCONOCIDO')
    df_original['subgenerico'] = df_original['subgenerico'].fillna('DESCONOCIDO')


    print("Creando tablas de dimensiones...")

    # --- 1. Dim_Tiempo ---
    # Usaremos 'Fecha_descarga' como la fecha principal para la dimensión de tiempo
    # Asegúrate de que los nombres de las columnas aquí coincidan EXACTAMENTE con lo que `print(df_original.columns)` te muestre.
    dim_tiempo_cols = ['Fecha_descarga', 'anio_denuncia', 'periodo_denuncia', 'fecha_corte']
    df_dim_tiempo = df_original[dim_tiempo_cols].drop_duplicates().reset_index(drop=True)
    df_dim_tiempo['id_tiempo'] = df_dim_tiempo.index + 1 # Generar ID numérico secuencial
    df_dim_tiempo = df_dim_tiempo[['id_tiempo', 'Fecha_descarga', 'anio_denuncia', 'periodo_denuncia', 'fecha_corte']]
    print(f"Dim_Tiempo creada con {len(df_dim_tiempo)} entradas únicas.")

    # --- 2. Dim_Delito ---
    dim_delito_cols = ['generico', 'subgenerico', 'articulo', 'des_articulo']
    df_dim_delito = df_original[dim_delito_cols].drop_duplicates().reset_index(drop=True)
    df_dim_delito['id_delito'] = df_dim_delito.index + 1 # Generar ID numérico secuencial
    df_dim_delito = df_dim_delito[['id_delito', 'generico', 'subgenerico', 'articulo', 'des_articulo']]
    print(f"Dim_Delito creada con {len(df_dim_delito)} entradas únicas.")

    # --- 3. Dim_Ubicacion ---
    dim_ubicacion_cols = ['ubigeo_pjfs', 'distrito_fiscal', 'dpto_pjfs', 'prov_pjfs', 'dist_pjfs']
    df_dim_ubicacion = df_original[dim_ubicacion_cols].drop_duplicates().reset_index(drop=True)
    df_dim_ubicacion = df_dim_ubicacion.rename(columns={'ubigeo_pjfs': 'id_ubicacion'}) # Renombrar para consistencia
    df_dim_ubicacion = df_dim_ubicacion[['id_ubicacion', 'distrito_fiscal', 'dpto_pjfs', 'prov_pjfs', 'dist_pjfs']]
    print(f"Dim_Ubicacion creada con {len(df_dim_ubicacion)} entradas únicas.")

    # --- 4. Dim_TipoCaso ---
    dim_tipocaso_cols = ['tipo_caso', 'especialidad']
    df_dim_tipo_caso = df_original[dim_tipocaso_cols].drop_duplicates().reset_index(drop=True)
    df_dim_tipo_caso['id_tipo_caso'] = df_dim_tipo_caso.index + 1 # Generar ID numérico secuencial
    df_dim_tipo_caso = df_dim_tipo_caso[['id_tipo_caso', 'tipo_caso', 'especialidad']]
    print(f"Dim_TipoCaso creada con {len(df_dim_tipo_caso)} entradas únicas.")

    print("Creando tabla de hechos (Fact_Denuncias)...")

    # --- 5. Fact_Denuncias ---
    df_fact_denuncias = df_original.copy()

    # Fusionar con Dim_Tiempo para obtener id_tiempo
    df_fact_denuncias = pd.merge(df_fact_denuncias, df_dim_tiempo,
                                 on=['Fecha_descarga', 'anio_denuncia', 'periodo_denuncia', 'fecha_corte'],
                                 how='left')

    # Fusionar con Dim_Delito para obtener id_delito
    df_fact_denuncias = pd.merge(df_fact_denuncias, df_dim_delito,
                                 on=['generico', 'subgenerico', 'articulo', 'des_articulo'],
                                 how='left')

    # Fusionar con Dim_Ubicacion para obtener id_ubicacion
    df_fact_denuncias = pd.merge(df_fact_denuncias, df_dim_ubicacion,
                                 left_on=['ubigeo_pjfs', 'distrito_fiscal', 'dpto_pjfs', 'prov_pjfs', 'dist_pjfs'],
                                 right_on=['id_ubicacion', 'distrito_fiscal', 'dpto_pjfs', 'prov_pjfs', 'dist_pjfs'],
                                 how='left')

    # Fusionar con Dim_TipoCaso para obtener id_tipo_caso
    df_fact_denuncias = pd.merge(df_fact_denuncias, df_dim_tipo_caso,
                                 on=['tipo_caso', 'especialidad'],
                                 how='left')

    # Seleccionar solo las columnas necesarias para la tabla de hechos
    df_fact_denuncias = df_fact_denuncias[['id_tiempo', 'id_delito', 'id_ubicacion', 'id_tipo_caso', 'cantidad']]
    df_fact_denuncias = df_fact_denuncias.reset_index(drop=True)
    df_fact_denuncias['id_denuncia'] = df_fact_denuncias.index + 1 # Clave primaria para la tabla de hechos
    df_fact_denuncias = df_fact_denuncias[['id_denuncia', 'id_tiempo', 'id_delito', 'id_ubicacion', 'id_tipo_caso', 'cantidad']]

    print(f"Fact_Denuncias creada con {len(df_fact_denuncias)} filas de hechos.")

    print("Normalización completada.")
    return df_fact_denuncias, df_dim_tiempo, df_dim_delito, df_dim_ubicacion, df_dim_tipo_caso

# --- Uso del script ---
if __name__ == "__main__":
    output_dir = 'normalizados'
    os.makedirs(output_dir, exist_ok=True)

    # DataFrames acumuladores
    all_dim_tiempo = []
    all_dim_delito = []
    all_dim_ubicacion = []
    all_dim_tipocaso = []
    all_fact_denuncias = []

    # Mapas para IDs globales
    tiempo_map = {}
    delito_map = {}
    ubicacion_map = {}
    tipocaso_map = {}
    next_tiempo_id = 1
    next_delito_id = 1
    next_ubicacion_id = 1
    next_tipocaso_id = 1
    next_denuncia_id = 1

    for csv_input_file in sorted(glob.glob('./[0-9][0-9][0-9][0-9].csv')):
        print(f"\nProcesando archivo: {csv_input_file}")
        try:
            fact_denuncias, dim_tiempo, dim_delito, dim_ubicacion, dim_tipo_caso = \
                normalizar_datos_delitos(csv_input_file)

            # --- Unificación de Dim_Tiempo ---
            for _, row in dim_tiempo.iterrows():
                key = (row['Fecha_descarga'], row['anio_denuncia'], row['periodo_denuncia'], row['fecha_corte'])
                if key not in tiempo_map:
                    tiempo_map[key] = next_tiempo_id
                    all_dim_tiempo.append({
                        'id_tiempo': next_tiempo_id,
                        'Fecha_descarga': row['Fecha_descarga'],
                        'anio_denuncia': row['anio_denuncia'],
                        'periodo_denuncia': row['periodo_denuncia'],
                        'fecha_corte': row['fecha_corte']
                    })
                    next_tiempo_id += 1
            # --- Unificación de Dim_Delito ---
            for _, row in dim_delito.iterrows():
                key = (row['generico'], row['subgenerico'], row['articulo'], row['des_articulo'])
                if key not in delito_map:
                    delito_map[key] = next_delito_id
                    all_dim_delito.append({
                        'id_delito': next_delito_id,
                        'generico': row['generico'],
                        'subgenerico': row['subgenerico'],
                        'articulo': row['articulo'],
                        'des_articulo': row['des_articulo']
                    })
                    next_delito_id += 1
            # --- Unificación de Dim_Ubicacion ---
            for _, row in dim_ubicacion.iterrows():
                key = (row['id_ubicacion'], row['distrito_fiscal'], row['dpto_pjfs'], row['prov_pjfs'], row['dist_pjfs'])
                if key not in ubicacion_map:
                    ubicacion_map[key] = next_ubicacion_id
                    all_dim_ubicacion.append({
                        'id_ubicacion': next_ubicacion_id,
                        'distrito_fiscal': row['distrito_fiscal'],
                        'dpto_pjfs': row['dpto_pjfs'],
                        'prov_pjfs': row['prov_pjfs'],
                        'dist_pjfs': row['dist_pjfs']
                    })
                    next_ubicacion_id += 1
            # --- Unificación de Dim_TipoCaso ---
            for _, row in dim_tipo_caso.iterrows():
                key = (row['tipo_caso'], row['especialidad'])
                if key not in tipocaso_map:
                    tipocaso_map[key] = next_tipocaso_id
                    all_dim_tipocaso.append({
                        'id_tipo_caso': next_tipocaso_id,
                        'tipo_caso': row['tipo_caso'],
                        'especialidad': row['especialidad']
                    })
                    next_tipocaso_id += 1
            # --- Unificación de Fact_Denuncias ---
            for _, row in fact_denuncias.iterrows():
                # Mapear los IDs locales a los globales
                tiempo_row = dim_tiempo.loc[dim_tiempo['id_tiempo'] == row['id_tiempo']].iloc[0]
                tiempo_key = (tiempo_row['Fecha_descarga'], tiempo_row['anio_denuncia'], tiempo_row['periodo_denuncia'], tiempo_row['fecha_corte'])
                id_tiempo_global = tiempo_map[tiempo_key]
                delito_row = dim_delito.loc[dim_delito['id_delito'] == row['id_delito']].iloc[0]
                delito_key = (delito_row['generico'], delito_row['subgenerico'], delito_row['articulo'], delito_row['des_articulo'])
                id_delito_global = delito_map[delito_key]
                ubicacion_row = dim_ubicacion.loc[dim_ubicacion['id_ubicacion'] == row['id_ubicacion']].iloc[0]
                ubicacion_key = (ubicacion_row['id_ubicacion'], ubicacion_row['distrito_fiscal'], ubicacion_row['dpto_pjfs'], ubicacion_row['prov_pjfs'], ubicacion_row['dist_pjfs'])
                id_ubicacion_global = ubicacion_map[ubicacion_key]
                tipocaso_row = dim_tipo_caso.loc[dim_tipo_caso['id_tipo_caso'] == row['id_tipo_caso']].iloc[0]
                tipocaso_key = (tipocaso_row['tipo_caso'], tipocaso_row['especialidad'])
                id_tipocaso_global = tipocaso_map[tipocaso_key]
                all_fact_denuncias.append({
                    'id_denuncia': next_denuncia_id,
                    'id_tiempo': id_tiempo_global,
                    'id_delito': id_delito_global,
                    'id_ubicacion': id_ubicacion_global,
                    'id_tipo_caso': id_tipocaso_global,
                    'cantidad': row['cantidad']
                })
                next_denuncia_id += 1
        except FileNotFoundError:
            print(f"Error: El archivo '{csv_input_file}' no fue encontrado. ")
        except Exception as e:
            print(f"Ocurrió un error durante la normalización de {csv_input_file}: {e}")

    # Guardar los archivos unificados
    pd.DataFrame(all_dim_tiempo).to_csv(os.path.join(output_dir, 'Dim_Tiempo.csv'), index=False)
    pd.DataFrame(all_dim_delito).to_csv(os.path.join(output_dir, 'Dim_Delito.csv'), index=False)
    pd.DataFrame(all_dim_ubicacion).to_csv(os.path.join(output_dir, 'Dim_Ubicacion.csv'), index=False)
    pd.DataFrame(all_dim_tipocaso).to_csv(os.path.join(output_dir, 'Dim_TipoCaso.csv'), index=False)
    pd.DataFrame(all_fact_denuncias).to_csv(os.path.join(output_dir, 'Fact_Denuncias.csv'), index=False)
    print("\n¡Unificación y guardado completados! Los archivos están en la carpeta 'normalizados'.")