#!/usr/bin/env python3
"""
Script para procesar y estructurar datos de población proyectada del Perú (2018-2022)
Autor: Asistente IA
Fecha: 2025

Este script procesa un archivo Excel con datos desordenados de población proyectada
de departamentos, provincias y distritos del Perú, y los convierte en un formato
tabular estructurado.
"""

import pandas as pd
import numpy as np
import re
import logging
from pathlib import Path
from typing import Dict, List, Tuple, Optional

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('procesamiento_poblacion.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class PoblacionPeruProcessor:
    """
    Clase para procesar los datos de población del Perú desde un archivo Excel
    con estructura irregular.
    """
    
    def __init__(self, archivo_excel: str):
        """
        Inicializar el procesador con la ruta del archivo Excel.
        
        Args:
            archivo_excel (str): Ruta al archivo Excel a procesar
        """
        self.archivo_excel = Path(archivo_excel)
        self.datos_limpios = None
        self.departamentos = {}
        self.provincias = {}
        self.distritos = {}
        
    def leer_archivo_excel(self) -> pd.DataFrame:
        """
        Leer el archivo Excel y retornar los datos en bruto.
        
        Returns:
            pd.DataFrame: Datos en bruto del archivo Excel
        """
        try:
            logger.info(f"Leyendo archivo Excel: {self.archivo_excel}")
            
            # Leer todas las hojas del archivo
            excel_file = pd.ExcelFile(self.archivo_excel)
            logger.info(f"Hojas encontradas: {excel_file.sheet_names}")
            
            # Leer la primera hoja (asumiendo que contiene los datos principales)
            df = pd.read_excel(
                self.archivo_excel,
                sheet_name=0,
                header=None,
                dtype=str,
                na_filter=False
            )
            
            logger.info(f"Datos leídos: {df.shape[0]} filas, {df.shape[1]} columnas")
            return df
            
        except Exception as e:
            logger.error(f"Error al leer el archivo Excel: {str(e)}")
            raise
    
    def identificar_encabezados(self, df: pd.DataFrame) -> int:
        """
        Identificar la fila que contiene los encabezados principales.
        
        Args:
            df (pd.DataFrame): DataFrame con los datos en bruto
            
        Returns:
            int: Índice de la fila con los encabezados
        """
        for i, row in df.iterrows():
            # Buscar la fila que contiene "UBIGEO" y años
            if any(str(cell).upper().strip() == 'UBIGEO' for cell in row):
                logger.info(f"Encabezados encontrados en la fila {i}")
                return i
        
        logger.warning("No se encontraron encabezados explícitos, usando fila 1")
        return 1
    
    def limpiar_ubigeo(self, ubigeo: str) -> Optional[str]:
        """
        Limpiar y validar códigos UBIGEO.
        
        Args:
            ubigeo (str): Código UBIGEO en bruto
            
        Returns:
            str: Código UBIGEO limpio o None si no es válido
        """
        if pd.isna(ubigeo) or ubigeo == '':
            return None
            
        # Convertir a string y limpiar
        ubigeo_str = str(ubigeo).strip()
        
        # Buscar patrón de números
        match = re.search(r'(\d+)', ubigeo_str)
        if not match:
            return None
            
        codigo = match.group(1)
        
        # Validar longitud (2, 4, o 6 dígitos)
        if len(codigo) in [2, 4, 6]:
            return codigo.zfill(6)  # Completar con ceros a la izquierda hasta 6 dígitos
        
        return None
    
    def determinar_tipo_region(self, ubigeo: str) -> str:
        """
        Determinar el tipo de región basado en el código UBIGEO.
        
        Args:
            ubigeo (str): Código UBIGEO de 6 dígitos
            
        Returns:
            str: Tipo de región (departamento, provincia, distrito)
        """
        if not ubigeo or len(ubigeo) != 6:
            return 'desconocido'
        
        # Departamento: XX0000
        if ubigeo[2:] == '0000':
            return 'departamento'
        # Provincia: XXXX00
        elif ubigeo[4:] == '00':
            return 'provincia'
        # Distrito: XXXXXX
        else:
            return 'distrito'
    
    def extraer_jerarquia(self, ubigeo: str) -> Dict[str, str]:
        """
        Extraer la jerarquía geográfica desde el código UBIGEO.
        
        Args:
            ubigeo (str): Código UBIGEO de 6 dígitos
            
        Returns:
            dict: Diccionario con códigos de departamento, provincia y distrito
        """
        if not ubigeo or len(ubigeo) != 6:
            return {'departamento': None, 'provincia': None, 'distrito': None}
        
        return {
            'departamento': ubigeo[:2] + '0000',
            'provincia': ubigeo[:4] + '00',
            'distrito': ubigeo
        }
    
    def limpiar_nombre_region(self, nombre: str) -> str:
        """
        Limpiar y normalizar nombres de regiones.
        
        Args:
            nombre (str): Nombre de la región en bruto
            
        Returns:
            str: Nombre limpio
        """
        if pd.isna(nombre):
            return ''
        
        # Convertir a string y limpiar espacios
        nombre_limpio = str(nombre).strip()
        
        # Remover patrones comunes de texto innecesario
        patrones_remover = [
            r'Continúa[\.]*',
            r'Creado \d+',
            r'^\d+\s*',  # Números al inicio
            r'\s+',      # Múltiples espacios
        ]
        
        for patron in patrones_remover:
            nombre_limpio = re.sub(patron, ' ', nombre_limpio, flags=re.IGNORECASE)
        
        return nombre_limpio.strip()
    
    def validar_datos_poblacion(self, valor: str) -> Optional[float]:
        """
        Validar y convertir datos de población.
        
        Args:
            valor (str): Valor de población en bruto
            
        Returns:
            float: Valor numérico de población o None si no es válido
        """
        if pd.isna(valor) or str(valor).strip() == '':
            return None
        
        try:
            # Convertir a string y limpiar
            valor_str = str(valor).replace(',', '').replace(' ', '')
            
            # Intentar convertir a float
            valor_num = float(valor_str)
            
            # Validar que sea un número positivo razonable
            if valor_num >= 0 and valor_num < 50000000:  # Límite razonable para población
                return valor_num
            
        except (ValueError, TypeError):
            pass
        
        return None
    
    def procesar_datos(self) -> pd.DataFrame:
        """
        Procesar los datos del archivo Excel y estructurarlos.
        
        Returns:
            pd.DataFrame: Datos procesados y estructurados
        """
        logger.info("Iniciando procesamiento de datos")
        
        # Leer archivo
        df_raw = self.leer_archivo_excel()
        
        # Identificar encabezados
        header_row = self.identificar_encabezados(df_raw)
        
        # Crear lista para almacenar datos procesados
        datos_procesados = []
        
        # Procesar cada fila
        for idx, row in df_raw.iterrows():
            if idx <= header_row:  # Saltar encabezados
                continue
            
            # Extraer UBIGEO
            ubigeo_raw = row.iloc[0] if len(row) > 0 else None
            ubigeo = self.limpiar_ubigeo(ubigeo_raw)
            
            if not ubigeo:
                continue
            
            # Extraer nombre
            nombre_raw = row.iloc[1] if len(row) > 1 else ''
            nombre = self.limpiar_nombre_region(nombre_raw)
            
            if not nombre:
                continue
            
            # Determinar tipo de región
            tipo_region = self.determinar_tipo_region(ubigeo)
            
            # Extraer jerarquía
            jerarquia = self.extraer_jerarquia(ubigeo)
            
            # Extraer datos de población (columnas 2-6 para años 2018-2022)
            poblacion_data = {}
            anos = ['2018', '2019', '2020', '2021', '2022']
            
            for i, ano in enumerate(anos):
                col_idx = i + 2  # Las columnas de población empiezan en la columna 2
                if col_idx < len(row):
                    valor = self.validar_datos_poblacion(row.iloc[col_idx])
                    poblacion_data[f'poblacion_{ano}'] = valor
                else:
                    poblacion_data[f'poblacion_{ano}'] = None
            
            # Crear registro
            registro = {
                'ubigeo': ubigeo,
                'nombre': nombre,
                'tipo_region': tipo_region,
                'cod_departamento': jerarquia['departamento'],
                'cod_provincia': jerarquia['provincia'],
                'cod_distrito': jerarquia['distrito'],
                **poblacion_data
            }
            
            datos_procesados.append(registro)
            
            # Almacenar en diccionarios por tipo para referencia cruzada
            if tipo_region == 'departamento':
                self.departamentos[ubigeo] = nombre
            elif tipo_region == 'provincia':
                self.provincias[ubigeo] = nombre
            elif tipo_region == 'distrito':
                self.distritos[ubigeo] = nombre
        
        # Crear DataFrame
        df_procesado = pd.DataFrame(datos_procesados)
        
        # Agregar nombres de departamento y provincia
        df_procesado['nombre_departamento'] = df_procesado['cod_departamento'].map(
            self.departamentos
        )
        df_procesado['nombre_provincia'] = df_procesado['cod_provincia'].map(
            self.provincias
        )
        
        logger.info(f"Procesamiento completado: {len(df_procesado)} registros")
        self.datos_limpios = df_procesado
        
        return df_procesado
    
    def validar_datos_procesados(self) -> Dict[str, any]:
        """
        Validar la calidad de los datos procesados.
        
        Returns:
            dict: Estadísticas de validación
        """
        if self.datos_limpios is None:
            logger.error("No hay datos procesados para validar")
            return {}
        
        df = self.datos_limpios
        
        stats = {
            'total_registros': len(df),
            'departamentos': len(df[df['tipo_region'] == 'departamento']),
            'provincias': len(df[df['tipo_region'] == 'provincia']),
            'distritos': len(df[df['tipo_region'] == 'distrito']),
            'registros_sin_nombre': len(df[df['nombre'] == '']),
            'registros_sin_poblacion_2022': len(df[df['poblacion_2022'].isna()]),
        }
        
        # Validar población total
        poblacion_peru_2022 = df[
            (df['tipo_region'] == 'departamento') & 
            (df['ubigeo'] == '000000')
        ]['poblacion_2022'].sum()
        
        stats['poblacion_total_peru_2022'] = poblacion_peru_2022
        
        logger.info("Estadísticas de validación:")
        for key, value in stats.items():
            logger.info(f"  {key}: {value}")
        
        return stats
    
    def generar_informe_analisis(self) -> str:
        """
        Generar un informe de análisis básico de los datos.
        
        Returns:
            str: Informe de análisis
        """
        if self.datos_limpios is None:
            return "No hay datos procesados para analizar"
        
        df = self.datos_limpios
        
        informe = ["=" * 80]
        informe.append("INFORME DE ANÁLISIS - POBLACIÓN PROYECTADA DEL PERÚ")
        informe.append("=" * 80)
        informe.append("")
        
        # Resumen general
        informe.append("RESUMEN GENERAL:")
        informe.append(f"- Total de registros procesados: {len(df):,}")
        informe.append(f"- Departamentos: {len(df[df['tipo_region'] == 'departamento']):,}")
        informe.append(f"- Provincias: {len(df[df['tipo_region'] == 'provincia']):,}")
        informe.append(f"- Distritos: {len(df[df['tipo_region'] == 'distrito']):,}")
        informe.append("")
        
        # Población por departamento en 2022
        dept_2022 = df[
            (df['tipo_region'] == 'departamento') & 
            (df['poblacion_2022'].notna())
        ].sort_values('poblacion_2022', ascending=False)
        
        informe.append("TOP 10 DEPARTAMENTOS POR POBLACIÓN (2022):")
        for _, row in dept_2022.head(10).iterrows():
            informe.append(f"- {row['nombre']}: {row['poblacion_2022']:,.0f} habitantes")
        informe.append("")
        
        # Crecimiento poblacional
        peru_total = df[df['ubigeo'] == '000000']
        if not peru_total.empty:
            pob_2018 = peru_total['poblacion_2018'].iloc[0]
            pob_2022 = peru_total['poblacion_2022'].iloc[0]
            if pob_2018 and pob_2022:
                crecimiento = ((pob_2022 - pob_2018) / pob_2018) * 100
                informe.append("CRECIMIENTO POBLACIONAL NACIONAL:")
                informe.append(f"- Población 2018: {pob_2018:,.0f}")
                informe.append(f"- Población 2022: {pob_2022:,.0f}")
                informe.append(f"- Crecimiento total: {crecimiento:.2f}%")
                informe.append("")
        
        # Calidad de datos
        informe.append("CALIDAD DE DATOS:")
        informe.append(f"- Registros con nombre vacío: {len(df[df['nombre'] == ''])}")
        informe.append(f"- Registros sin población 2022: {len(df[df['poblacion_2022'].isna()])}")
        informe.append("")
        
        informe.append("=" * 80)
        
        return "\n".join(informe)
    
    def guardar_datos(self, archivo_salida: str, formato: str = 'csv') -> None:
        """
        Guardar los datos procesados en un archivo.
        
        Args:
            archivo_salida (str): Ruta del archivo de salida
            formato (str): Formato de salida ('csv' o 'excel')
        """
        if self.datos_limpios is None:
            logger.error("No hay datos procesados para guardar")
            return
        
        archivo_path = Path(archivo_salida)
        
        try:
            if formato.lower() == 'csv':
                self.datos_limpios.to_csv(archivo_path, index=False, encoding='utf-8')
                logger.info(f"Datos guardados en CSV: {archivo_path}")
            
            elif formato.lower() == 'excel':
                with pd.ExcelWriter(archivo_path, engine='openpyxl') as writer:
                    # Hoja principal con todos los datos
                    self.datos_limpios.to_excel(
                        writer, 
                        sheet_name='Datos_Procesados', 
                        index=False
                    )
                    
                    # Hoja con resumen por departamentos
                    dept_summary = self.datos_limpios[
                        self.datos_limpios['tipo_region'] == 'departamento'
                    ].copy()
                    dept_summary.to_excel(
                        writer, 
                        sheet_name='Resumen_Departamentos', 
                        index=False
                    )
                
                logger.info(f"Datos guardados en Excel: {archivo_path}")
            
            else:
                logger.error(f"Formato no soportado: {formato}")
                
        except Exception as e:
            logger.error(f"Error al guardar archivo: {str(e)}")
            raise


def main():
    """
    Función principal para ejecutar el procesamiento.
    """
    # Configuración
    archivo_entrada = '3464927-anexo-1.xlsx'  # Cambiar por la ruta real
    archivo_salida_csv = 'poblacion_peru_procesada.csv'
    archivo_salida_excel = 'poblacion_peru_procesada.xlsx'
    archivo_informe = 'informe_analisis.txt'
    
    try:
        # Crear procesador
        processor = PoblacionPeruProcessor(archivo_entrada)
        
        # Procesar datos
        datos_procesados = processor.procesar_datos()
        
        # Validar datos
        stats = processor.validar_datos_procesados()
        
        # Generar informe
        informe = processor.generar_informe_analisis()
        
        # Guardar resultados
        processor.guardar_datos(archivo_salida_csv, 'csv')
        processor.guardar_datos(archivo_salida_excel, 'excel')
        
        # Guardar informe
        with open(archivo_informe, 'w', encoding='utf-8') as f:
            f.write(informe)
        
        logger.info("Procesamiento completado exitosamente")
        print("\n" + informe)
        
        # Mostrar muestra de datos
        print("\nMUESTRA DE DATOS PROCESADOS:")
        print(datos_procesados.head(10))
        
    except Exception as e:
        logger.error(f"Error en el procesamiento: {str(e)}")
        raise


if __name__ == "__main__":
    main()