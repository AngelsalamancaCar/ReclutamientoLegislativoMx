"""
Data Pipeline para el procesamiento de información de diputaciones y datos de perfil
"""

# Cargar librerías requeridas para procesamiento de datos
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional
import logging
import polars as pl

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
            input_file=Path("data/raw/LXI.xlsx"),
            output_file=Path("data/processed/LXI_processed.parquet"),
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
    "Mayoría Relativa": "mr",
    "Representación Proporcional": "rp",
}

PARTIDO_MAPPING = {
    "PRI01": "PRI",
    "PAN": "PAN",
    "PRD01": "PRD",
    "LOGVRD": "PVerde",
    "LOGPT": "PT",
    "PANAL": "PANAL",
    "LOGO_MOVIMIENTO_CIUDADANO": "MovCiudadano",
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
    "COMITÉ": "comite",
    "ESPECIAL": "especial",
    "BICAMARAL": "bicamaral",
}

TIPO_ACTIVIDAD_MAPPING = {
    "ESCOLARIDAD": "escolaridad",
    "TRAYECTORIA POLÍTICA": "exp_politica",
    "INICIATIVA PRIVADA": "exp_laboral_privada",
    "EXPERIENCIA LEGISLATIVA": "exp_leg_previa",
    "ADMINISTRACIÓN PÚBLICA FEDERAL": "exp_apf",
    "ADMINISTRACIÓN PÚBLICA LOCAL": "exp_aplocal",
    "CARGOS EN LEGISLATURAS LOCALES O FEDERALES": "cargos_legislativos_previa",
    "CARGOS DE ELECCIÓN POPULAR": "cargos_electos_previos",
    "ASOCIACIONES A LAS QUE PERTENECE": "exp_asociaciones",
    "ACTIVIDADES DOCENTES": "exp_docente",
    "PUBLICACIONES": "publicaciones",
    "Actividad Empresarial": "exp_empresarial",
    "LOGROS DEPORTIVOS MÁS DESTACADOS": "logros_deportivos",
}


# ==================== UTILIDADES ====================

def normalizar_expresiones_pl(col: pl.Expr) -> pl.Expr:
    """Normaliza strings en español: remueve acentos y caracteres especiales"""
    return (
        col.str.normalize("nfd")
        .str.replace_all(r"\p{Mn}", "")
        .str.replace_all(r"[^\p{L}\s]", "")
        .str.replace_all(r"\s+", " ")
        .str.strip()
    )


def safe_get_value(df: pl.DataFrame, column: str, idx: int, default: str = "") -> str:
    """Extrae valor de forma segura del dataframe"""
    try:
        value = df[column][idx]
        return str(value) if value is not None else default
    except (IndexError, KeyError):
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
    
    return df.with_columns([
        pl.col("partido_diputado").replace(partido_mapping, default=pl.col("partido_diputado")),
        pl.col("tipo_eleccion").replace(TIPO_ELECCION_MAPPING, default=pl.col("tipo_eleccion")),
        pl.col("entidad").replace(ENTIDAD_MAPPING, default=pl.col("entidad")),
        pl.col("fecha_nacimiento")
            .str.strptime(pl.Date, format="%d-%m-%Y", strict=False)
            .alias("fecha_nacimiento"),
        normalizar_expresiones_pl(pl.col("nombre_completo")).alias("nombre_completo"),
        normalizar_expresiones_pl(pl.col("suplente")).alias("suplente"),
        normalizar_expresiones_pl(pl.col("cabecera")).alias("cabecera"),
        pl.col("legislatura_activo").replace(LEGISLATURA_MAPPING, default=pl.col("legislatura_activo")),
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
            .replace(TIPO_COMITE_MAPPING, default=pl.col("tipo_comite"))
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


def process_sheet3(df: pl.DataFrame) -> pl.DataFrame:
    """Procesa Sheet3: perfiles y experiencia de diputados"""
    logger.info("Procesando Sheet3...")
    
    if df.is_empty():
        logger.warning("Sheet3 está vacío")
        return pl.DataFrame({"dip_id": []})
    
    df = df.with_columns([
        pl.col("tipo")
            .replace(TIPO_ACTIVIDAD_MAPPING, default=pl.col("tipo"))
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
    config = PipelineConfig.default()
    df_result = run_pipeline(config)