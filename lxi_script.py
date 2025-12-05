"""
Data Pipeline para el procesamiento de información de diputaciones y datos de perfil
"""

# Cargar librerías requeridas para procesamiento de datos
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional
import logging
import polars as pl
import os
import re
from pipeline_config import PipelineConfig

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# ==================== CONFIGURACIÓN ====================

@dataclass
class PipelineConfig:
    """Configuración del data pipeline"""
    
    input_file: Path
    output_file: Path
    partido_mapping: Dict[str, str]
    
    @classmethod
    def default(cls):
        """Configuración por defecto con paths relativos"""
        return cls(
            input_file=Path(r"C:\Users\zigma\Projects\CongresoProject\data\raw\LXI.xlsx"),
            output_file=Path(r"C:\Users\zigma\Projects\CongresoProject\data\processed\LXI_processed.parquet"),
            partido_mapping=PARTIDO_MAPPING  # Usar constante definida abajo
        )


# ==================== MAPEOS ====================

LEGISLATURA_MAPPING = {
    "LXI": "51",
    "LXII": "52",
    "LXIII": "53",
    "LXIV": "54",
    "LXV": "55",
    "LXVI": "56",
}

TIPO_ELECCION_MAPPING = {
    "Mayoria Relativa": "mr",
    "Mayoría Relativa": "mr",  # Con acento
    "Representacion Proporcional": "rp",
    "Representación Proporcional": "rp",  # Con acento
    "Representación proporcional": "rp",
}


PARTIDO_MAPPING = {
    "PRI01": "PRI",
    "PAN": "PAN",
    "PRD01": "PRD",
    "LOGVRD": "VERDE",
    "LOGPT": "PT",
    "PANAL": "PANAL",
    "LOGO_MOVIMIENTO_CIUDADANO": "MC",
    "CONVERGENCIA": "CONVERGENCIA",
    "PASC": "PASC",
    "LOGOMORENA": "MORENA",
    "LOGO_SP": "SP",
    "LOGO_PT": "PT",
    "ENCUENTRO": "ENCUENTRO",
    "PRI": "PRI",
    "MORENA": "MORENA",
    "VERDE": "PVerde",
    "PT": "PT",
    "MC": "MC"
}

ENTIDAD_MAPPING = {
    "Aguascalientes": "AGS",
    "Baja California": "BC",
    "Baja California Sur": "BCS",
    "Campeche": "CAMP",
    "Chiapas": "CHIS",
    "Chihuahua": "CHIH",
    "Ciudad de México": "CDMX",
    "Coahuila de Zaragoza": "COAH",
    "Colima": "COL",
    "Durango": "DGO",
    "Guanajuato": "GTO",
    "Guerrero": "GRO",
    "Hidalgo": "HGO",
    "Jalisco": "JAL",
    "México": "MEX",
    "Michoacán de Ocampo": "MICH",
    "Morelos": "MOR",
    "Nayarit": "NAY",
    "Nuevo León": "NL",
    "Oaxaca": "OAX",
    "Puebla": "PUE",
    "Querétaro": "QRO",
    "Quintana Roo": "QR",
    "San Luis Potosí": "SLP",
    "Sinaloa": "SIN",
    "Sonora": "SON",
    "Tabasco": "TAB",
    "Tamaulipas": "TAMPS",
    "Tlaxcala": "TLAX",
    "Veracruz de Ignacio de la Llave": "VER",
    "Yucatán": "YUC",
    "Zacatecas": "ZAC",
    # Variantes
    "Distrito Federal": "CDMX",
    "DF": "CDMX",
}

TIPO_COMITE_MAPPING = {
    "ORDINARIA": "ordinaria",
    "COMITE": "comite",
    "COMITÉ": "comite",  # Con acento
    "ESPECIAL": "especial",
    "BICAMARAL": "bicamaral",
}

TIPO_ACTIVIDAD_MAPPING = {
    "ESCOLARIDAD": "escolaridad",
    "TRAYECTORIA POLITICA": "exp_politica",
    "TRAYECTORIA POLÍTICA": "exp_politica",  # Con acento
    "INICIATIVA PRIVADA": "exp_laboral_privada",
    "EXPERIENCIA LEGISLATIVA": "exp_leg_previa",
    "ADMINISTRACION PUBLICA FEDERAL": "exp_apf",
    "ADMINISTRACIÓN PÚBLICA FEDERAL": "exp_apf",  # Con acento
    "ADMINISTRACION PUBLICA LOCAL": "exp_aplocal",
    "ADMINISTRACIÓN PÚBLICA LOCAL": "exp_aplocal",  # Con acento
    "CARGOS EN LEGISLATURAS LOCALES O FEDERALES": "cargos_legislativos_previa",
    "CARGOS DE ELECCION POPULAR": "cargos_electos_previos",
    "CARGOS DE ELECCIÓN POPULAR": "cargos_electos_previos",  # Con acento
    "ASOCIACIONES A LAS QUE PERTENECE": "exp_asociaciones",
    "ACTIVIDADES DOCENTES": "exp_docente",
    "PUBLICACIONES": "publicaciones",
    "Actividad Empresarial": "exp_empresarial",
    "LOGROS DEPORTIVOS MAS DESTACADOS": "logros_deportivos",
    "LOGROS DEPORTIVOS MÁS DESTACADOS": "logros_deportivos",  # Con acento
}

# ==================== UTILIDADES ====================

def normalizar_texto(texto):
    """Función auxiliar para normalizar texto en Python puro."""
    if not isinstance(texto, str):
        return texto
    
    texto = texto.strip()
    texto = re.sub(r"\s+", " ", texto)
    texto = texto.lower()
    texto = re.sub(r"[^\w\sáéíóúñüÁÉÍÓÚÑÜ]", "", texto)
    
    # Normalizar caracteres acentuados
    texto = texto.replace("á", "a")
    texto = texto.replace("é", "e")
    texto = texto.replace("í", "i")
    texto = texto.replace("ó", "o")
    texto = texto.replace("ú", "u")
    texto = texto.replace("ñ", "n")
    texto = texto.replace("ü", "u")
    
    return texto

def normalizar_expresiones_pl(expr):
    """
    Función helper para aplicar normalizaciones de texto usando Polars expressions.
    """
    return expr.map_elements(
        lambda x: normalizar_texto(x) if isinstance(x, str) else x,
        return_dtype=pl.String
    )


def safe_get_value(df: pl.DataFrame, column: str, idx: int, default: str = "") -> str:
    """Extrae valor de forma segura del dataframe"""
    try:
        # Verificar que la columna existe
        if column not in df.columns:
            return default
        
        # Obtener el valor usando slice
        value = df.slice(idx, 1)[column][0]
        return str(value) if value is not None else default
    except (IndexError, pl.exceptions.ColumnNotFoundError):
        return default


# ==================== CARGA DE DATOS ====================

def load_excel_sheet(input_file: Path, sheet_name: str) -> pl.DataFrame:
    """Carga una hoja de Excel con manejo de errores"""
    try:
        logger.info(f"Cargando {sheet_name} desde {input_file}...")
        df = pl.read_excel(input_file, sheet_name=sheet_name)
        logger.info(f"✓ {sheet_name} cargada: {len(df)} filas")
        return df
    except Exception as e:
        logger.error(f"✗ Error cargando {sheet_name}: {e}")
        raise


# ==================== PROCESAMIENTO SHEET1 ====================

def process_sheet1(df: pl.DataFrame, partido_mapping: Dict[str, str]) -> pl.DataFrame:
    """Procesa Sheet1: información básica de diputados"""
    logger.info("Procesando Sheet1...")
    
    if df.is_empty():
        logger.warning("Sheet1 está vacío")
        return df
    
    # Asegurar que la columna sea string para manipulación
    df = df.with_columns(
        pl.col("fecha_nacimiento").cast(pl.String)
    )
    
    # Parsear fecha con manejo de múltiples formatos
    fecha_expr = (
        pl.col("fecha_nacimiento")
        .str.strptime(pl.Date, format="%d-%m-%Y", strict=False)
        .fill_null(
            pl.col("fecha_nacimiento")
            .str.strptime(pl.Date, format="%Y-%m-%d", strict=False)
        )
        .fill_null(
            pl.col("fecha_nacimiento")
            .str.strptime(pl.Date, format="%d/%m/%Y", strict=False)
        )
    )
    
    return df.with_columns([
        pl.col("partido_diputado").replace_strict(partido_mapping, default=pl.col("partido_diputado")),
        pl.col("tipo_eleccion").replace_strict(TIPO_ELECCION_MAPPING, default=pl.col("tipo_eleccion")),
        pl.col("entidad").replace_strict(ENTIDAD_MAPPING, default=pl.col("entidad")),
        fecha_expr.alias("fecha_nacimiento"),
        normalizar_expresiones_pl(pl.col("nombre_completo")).alias("nombre_completo"),
        normalizar_expresiones_pl(pl.col("suplente")).alias("suplente"),
        normalizar_expresiones_pl(pl.col("cabecera")).alias("cabecera"),
        pl.col("legislatura_activo").replace_strict(LEGISLATURA_MAPPING, default=pl.col("legislatura_activo")),
    ])


# ==================== PROCESAMIENTO SHEET2 ====================

def process_sheet2(df: pl.DataFrame) -> pl.DataFrame:
    """Procesa Sheet2: comités y comisiones"""
    logger.info("Procesando Sheet2...")
    
    if df.is_empty():
        logger.warning("Sheet2 está vacío")
        return pl.DataFrame({"dip_id": []})
    
    # Normalizar nombres de comités
    df = df.with_columns([
        normalizar_expresiones_pl(pl.col("nombre_comite")).alias("nombre_comite_normalizado"),
        pl.col("tipo_comite")
            .replace_strict(TIPO_COMITE_MAPPING, default=pl.col("tipo_comite"))
            .alias("tipo_comite_std")
    ])
    
    # Agrupar por diputado y tipo de comité
    df_grouped = df.group_by(["dip_id", "tipo_comite_std"]).agg([
        pl.col("nombre_comite_normalizado").alias("comites"),
        pl.col("nombre_comite_normalizado").count().alias("num_comites"),
    ])
    
    result_rows = []
    
    for dip_id in df["dip_id"].unique().sort():
        deputy_data = df_grouped.filter(pl.col("dip_id") == dip_id)
        row = {"dip_id": dip_id, "total_comites": 0}
        
        for tipo in ["ordinaria", "comite", "especial", "bicamaral"]:
            tipo_data = deputy_data.filter(pl.col("tipo_comite_std") == tipo)
            
            if len(tipo_data) > 0:
                comites_list = tipo_data["comites"][0]
                num_comites = tipo_data["num_comites"][0]
                row[f"num_{tipo}"] = num_comites
                row["total_comites"] += num_comites
                
                for idx, comite in enumerate(comites_list, start=1):
                    if comite:
                        row[f"{tipo}_{idx}"] = comite
            else:
                row[f"num_{tipo}"] = 0
        
        result_rows.append(row)
    
    logger.info(f"✓ Procesados {len(result_rows)} diputados")
    return pl.DataFrame(result_rows)


# ==================== PROCESAMIENTO SHEET3 ====================

def process_cargo_eleccion_popular(dip_data: pl.DataFrame) -> Dict:
    """Procesa cargos de elección popular previos"""
    # FIX: Corregir nombre de columna
    cargo_elec = dip_data.filter(pl.col("tipo_actividad_std") == "cargos_electos_previos")
    result = {"cargo_eleccion_popular": 1 if len(cargo_elec) > 0 else 0}
    
    for idx in range(len(cargo_elec)):
        result[f"cargo_eleccion_popular_{idx + 1}"] = safe_get_value(cargo_elec, "descripcion", idx)
        result[f"cargo_eleccion_popular_partido_{idx + 1}"] = safe_get_value(cargo_elec, "detalle", idx)
        result[f"cargo_eleccion_popular_periodo_{idx + 1}"] = safe_get_value(cargo_elec, "periodo", idx)
    
    return result

def process_deputy_profile(dip_id: int, dip_data: pl.DataFrame) -> Dict:
    """Procesa el perfil completo de un diputado"""
    profile = {"dip_id": dip_id}
    
    # Escolaridad
    escolaridad = dip_data.filter(pl.col("tipo_actividad_std") == "escolaridad")
    profile["escolaridad"] = 1 if len(escolaridad) > 0 else 0
    for idx in range(len(escolaridad)):
        profile[f"escolaridad_{idx + 1}"] = safe_get_value(escolaridad, "descripcion", idx)
        profile[f"escolaridad_institucion_{idx + 1}"] = safe_get_value(escolaridad, "detalle", idx)
    
    # Experiencia Política
    exp_politica = dip_data.filter(pl.col("tipo_actividad_std") == "exp_politica")
    profile["exp_politica"] = 1 if len(exp_politica) > 0 else 0
    for idx in range(len(exp_politica)):
        profile[f"exp_politica_{idx + 1}"] = safe_get_value(exp_politica, "descripcion", idx)
        profile[f"exp_politica_periodo_{idx + 1}"] = safe_get_value(exp_politica, "periodo", idx)
    
    # Experiencia Laboral Privada
    exp_privada = dip_data.filter(pl.col("tipo_actividad_std") == "exp_laboral_privada")
    profile["exp_laboral_privada"] = 1 if len(exp_privada) > 0 else 0
    for idx in range(len(exp_privada)):
        profile[f"exp_laboral_privada_{idx + 1}"] = safe_get_value(exp_privada, "descripcion", idx)
    
    # Experiencia Legislativa Previa
    exp_leg = dip_data.filter(pl.col("tipo_actividad_std") == "exp_leg_previa")
    profile["exp_leg_previa"] = 1 if len(exp_leg) > 0 else 0
    for idx in range(len(exp_leg)):
        profile[f"exp_leg_previa_{idx + 1}"] = safe_get_value(exp_leg, "descripcion", idx)
        profile[f"exp_leg_previa_periodo_{idx + 1}"] = safe_get_value(exp_leg, "periodo", idx)
    
    # Administración Pública Federal
    exp_apf = dip_data.filter(pl.col("tipo_actividad_std") == "exp_apf")
    profile["exp_apf"] = 1 if len(exp_apf) > 0 else 0
    for idx in range(len(exp_apf)):
        profile[f"exp_apf_{idx + 1}"] = safe_get_value(exp_apf, "descripcion", idx)
        profile[f"exp_apf_periodo_{idx + 1}"] = safe_get_value(exp_apf, "periodo", idx)
    
    # Administración Pública Local
    exp_aplocal = dip_data.filter(pl.col("tipo_actividad_std") == "exp_aplocal")
    profile["exp_aplocal"] = 1 if len(exp_aplocal) > 0 else 0
    for idx in range(len(exp_aplocal)):
        profile[f"exp_aplocal_{idx + 1}"] = safe_get_value(exp_aplocal, "descripcion", idx)
        profile[f"exp_aplocal_periodo_{idx + 1}"] = safe_get_value(exp_aplocal, "periodo", idx)
    
    # Cargos Legislativos Previos
    cargos_leg = dip_data.filter(pl.col("tipo_actividad_std") == "cargos_legislativos_previa")
    profile["cargos_legislativos_previa"] = 1 if len(cargos_leg) > 0 else 0
    for idx in range(len(cargos_leg)):
        profile[f"cargo_legislativo_{idx + 1}"] = safe_get_value(cargos_leg, "descripcion", idx)
        profile[f"cargo_legislativo_periodo_{idx + 1}"] = safe_get_value(cargos_leg, "periodo", idx)
    
    # Cargos de Elección Popular (usar la función ya definida)
    profile.update(process_cargo_eleccion_popular(dip_data))
    
    # Asociaciones
    asociaciones = dip_data.filter(pl.col("tipo_actividad_std") == "exp_asociaciones")
    profile["exp_asociaciones"] = 1 if len(asociaciones) > 0 else 0
    for idx in range(len(asociaciones)):
        profile[f"asociacion_{idx + 1}"] = safe_get_value(asociaciones, "descripcion", idx)
    
    # Experiencia Docente
    exp_docente = dip_data.filter(pl.col("tipo_actividad_std") == "exp_docente")
    profile["exp_docente"] = 1 if len(exp_docente) > 0 else 0
    for idx in range(len(exp_docente)):
        profile[f"exp_docente_{idx + 1}"] = safe_get_value(exp_docente, "descripcion", idx)
        profile[f"exp_docente_institucion_{idx + 1}"] = safe_get_value(exp_docente, "detalle", idx)
    
    # Publicaciones
    publicaciones = dip_data.filter(pl.col("tipo_actividad_std") == "publicaciones")
    profile["publicaciones"] = 1 if len(publicaciones) > 0 else 0
    for idx in range(len(publicaciones)):
        profile[f"publicacion_{idx + 1}"] = safe_get_value(publicaciones, "descripcion", idx)
    
    # Experiencia Empresarial
    exp_empresarial = dip_data.filter(pl.col("tipo_actividad_std") == "exp_empresarial")
    profile["exp_empresarial"] = 1 if len(exp_empresarial) > 0 else 0
    for idx in range(len(exp_empresarial)):
        profile[f"exp_empresarial_{idx + 1}"] = safe_get_value(exp_empresarial, "descripcion", idx)
    
    # Logros Deportivos
    logros_dep = dip_data.filter(pl.col("tipo_actividad_std") == "logros_deportivos")
    profile["logros_deportivos"] = 1 if len(logros_dep) > 0 else 0
    for idx in range(len(logros_dep)):
        profile[f"logro_deportivo_{idx + 1}"] = safe_get_value(logros_dep, "descripcion", idx)
    
    return profile

def process_sheet3(df: pl.DataFrame) -> pl.DataFrame:
    """Procesa Sheet3: perfiles y experiencia de diputados"""
    logger.info("Procesando Sheet3...")
    
    if df.is_empty():
        logger.warning("Sheet3 está vacío")
        return pl.DataFrame({"dip_id": []})
    
    df = df.with_columns([
        pl.col("tipo")
            .replace_strict(TIPO_ACTIVIDAD_MAPPING, default=pl.col("tipo"), return_dtype=pl.Utf8)
            .alias("tipo_actividad_std")
    ])
    
    results = []
    for dip_id in df["dip_id"].unique().sort():
        dip_data = df.filter(pl.col("dip_id") == dip_id)
        results.append(process_deputy_profile(dip_id, dip_data))
    
    logger.info(f"✓ Procesados {len(results)} perfiles")
    return pl.DataFrame(results) if results else pl.DataFrame({"dip_id": []})


# [Las demás funciones de procesamiento se mantienen similares, solo agregando logging]


# ==================== PIPELINE PRINCIPAL ====================

def merge_dataframes(
    df_sheet1: pl.DataFrame,
    df_sheet2: pl.DataFrame,
    df_sheet3: pl.DataFrame
) -> pl.DataFrame:
    """Integra los tres dataframes procesados"""
    logger.info("Integrando dataframes...")
    
    # Comenzar con Sheet1 (información básica)
    df_final = df_sheet1
    
    # Unir con Sheet2 (comités) si no está vacío
    if not df_sheet2.is_empty() and "dip_id" in df_sheet2.columns:
        df_final = df_final.join(df_sheet2, on="dip_id", how="left")
        logger.info("✓ Sheet2 integrado")
    else:
        logger.warning("Sheet2 vacío o sin columna dip_id, se omite integración")
    
    # Unir con Sheet3 (perfiles) si no está vacío
    if not df_sheet3.is_empty() and "dip_id" in df_sheet3.columns:
        df_final = df_final.join(df_sheet3, on="dip_id", how="left")
        logger.info("✓ Sheet3 integrado")
    else:
        logger.warning("Sheet3 vacío o sin columna dip_id, se omite integración")
    
    logger.info(f"✓ Integración completa: {len(df_final)} filas, {len(df_final.columns)} columnas")
    return df_final

def reorder_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Reordena las columnas para mejor legibilidad"""
    logger.info("Reordenando columnas...")
    
    # Definir orden preferido de columnas principales
    priority_cols = [
        "dip_id",
        "nombre_completo",
        "partido_diputado",
        "tipo_eleccion",
        "entidad",
        "distrito",
        "cabecera",
        "circunscripcion",
        "fecha_nacimiento",
        "suplente",
        "legislatura_activo",
        "total_comites",
    ]
    
    # Columnas que existen en el dataframe
    existing_priority = [col for col in priority_cols if col in df.columns]
    
    # Columnas restantes (en orden alfabético)
    remaining_cols = sorted([col for col in df.columns if col not in existing_priority])
    
    # Orden final
    final_order = existing_priority + remaining_cols
    
    logger.info("✓ Columnas reordenadas")
    return df.select(final_order)


def run_pipeline(config: PipelineConfig) -> pl.DataFrame:
    """Ejecuta el pipeline completo de procesamiento"""
    logger.info("=" * 60)
    logger.info("INICIANDO DATA PIPELINE")
    logger.info(f"Input: {config.input_file}")
    logger.info(f"Output: {config.output_file}")
    logger.info("=" * 60)
    
    try:
        # Cargar datos
        df_sheet1 = load_excel_sheet(config.input_file, "Sheet1")
        df_sheet2 = load_excel_sheet(config.input_file, "Sheet2")
        df_sheet3 = load_excel_sheet(config.input_file, "Sheet3")
        
        # Procesar cada hoja
        df_sheet1_processed = process_sheet1(df_sheet1, config.partido_mapping)
        df_sheet2_processed = process_sheet2(df_sheet2)
        df_sheet3_processed = process_sheet3(df_sheet3)
        
        # Integrar datos
        df_final = merge_dataframes(df_sheet1_processed, df_sheet2_processed, df_sheet3_processed)
        
        # Reordenar columnas
        df_final = reorder_columns(df_final)
        
        # Crear directorio de salida si no existe
        config.output_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Guardar resultados
        logger.info("Guardando archivo de salida...")
        df_final.write_parquet(config.output_file)
        
        logger.info("=" * 60)
        logger.info("✓ PROCESAMIENTO COMPLETO")
        logger.info(f"Output guardado en: {config.output_file}")
        logger.info(f"Total filas: {len(df_final)}")
        logger.info(f"Total columnas: {len(df_final.columns)}")
        logger.info("=" * 60)
        
        return df_final
        
    except Exception as e:
        logger.error(f"✗ Error en pipeline: {e}")
        raise


# ==================== MAIN ====================

if __name__ == "__main__":
    # Opción 1: Usar configuración por defecto (con auto-detección)
    try:
        config = PipelineConfig.default()
    except FileNotFoundError as e:
        logger.error(f"Error de configuración: {e}")
        # Opción 2: Usar rutas manuales si falla la auto-detección
        config = PipelineConfig(
            input_file=Path(r"C:\Users\zigma\Projects\CongresoProject\data\raw\LXI.xlsx"),
            output_file=Path(r"C:\Users\zigma\Projects\CongresoProject\data\processed\LXI_processed.parquet"),
            partido_mapping=PARTIDO_MAPPING)
    
    # Verificar que el archivo existe antes de procesar
    if not config.input_file.exists():
        logger.error(f"❌ Archivo no encontrado: {config.input_file}")
        logger.info("Verifica que la ruta sea correcta y que el archivo exista.")
        exit(1)
    
    # Ejecutar pipeline
    df_result = run_pipeline(config)

