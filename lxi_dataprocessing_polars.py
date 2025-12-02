"""
Data Pipeline para el procesamiento de información de diputaciones y datos de perfil
"""

# Cargar librerias requeridas para procesamiento de datos
from dataclasses import dataclass
from pathlib import Path
from typing import dict, List, Optional

import polars as pl


# Establecer clase para data pipeline
@dataclass
class PipelineConfig:
    """Configuracion del data pipeline"""

    input_file: Path
    output_file: Path
    partido_mapping: Dict[str, str]

    @classmethod
    def default(cls):
        return cls(
            input_file=Path(
                r"C:\Users\zigma\Projects\CongresoProject\data\raw\LXI.xlsx"
            ),
            output_file=Path(
                r"C:\Users\zigma\Projects\CongresoProject\data\processed\LXI_processed.parquet"
            ),
        )


# ==================== CARGAR DATOS DE FUENTE EXTRAIDA ====================


def load_excel_sheet(file_path: Path, sheet_name: str) -> pl.DataFrame:
    """Carga una hoja de excel conforme a patrón establecido"""
    print(f"Loading file: {input_file}...")
    print(f"Loading sheet: {sheet_name}...")
    return pl.read_excel(file_path, sheet_name=sheet_name)


# ==================== PROCESAMIENTO SHEET 1 ====================
# Utileria para strings en espanol
def normalizar_expresionespl(col: pl.Expr) -> pl.Expr:
    return (
        col.str.normalize("nfd")  # separa acentos de caracteres
        .str.replace_all(r"\p{Mn}", "")  # remover acento diacritico
        .str.replace_all(
            r"[^\p{L}\s]", ""
        )  # Mantiene solamente letras y espacios no repetidos
        .str.replace_all(r"\s+", " ")  # normaliza los espacios en blanco
        .str.strip()
    )


# Mapeo de legislaturas
legislatura_mapping = {
    "LXI": "51",
    "LXII": "52",
    "LXIII": "53",
    "LXIV": "54",
    "LXV": "55",
    "LXVI": "56",
}

# Mapeo para tipos de representación de diputados
tipo_eleccion_mapping = {
    "Mayoría Relativa": "mr",
    "Representación Proporcional": "rp",
}

# Mapeo para partidos políticos
partido_mapping = {
    "PRI01": "PRI",
    "PAN": "PAN",
    "PRD01": "PRD",
    "LOGVRD": "PVerde",
    "LOGPT": "PT",
    "PANAL": "PANAL",
    "LOGO_MOVIMIENTO_CIUDADANO": "MovCiudadano",
}
entidad_mapping = {
    # Nombres estandar con acentos
    "Aguascalientes": "AGS",
    "Baja California": "BC",
    "Baja California Sur": "BCS",
    "Campeche": "CAMP",
    "Chiapas": "CHIS",
    "Chihuahua": "CHIH",
    "Ciudad de México": "CDMX",  # Alternative: "CMX"
    "Coahuila de Zaragoza": "COAH",
    "Colima": "COL",
    "Durango": "DGO",
    "Guanajuato": "GTO",
    "Guerrero": "GRO",
    "Hidalgo": "HGO",
    "Jalisco": "JAL",
    "México": "MEX",  # Estado de México
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
    # Variantes comunes
    "Mexico": "MEX",  # Estado de México sin acento y en breve
    "Michoacan": "MICH",
    "Nuevo Leon": "NL",
    "Queretaro": "QRO",
    "San Luis Potosi": "SLP",
    "Yucatan": "YUC",
    "Distrito Federal": "CDMX",  # Nombre viejo
    "DF": "CDMX",
    "Baja California Norte": "BC",  # Variante comun pero no oficial
    "Edomex": "MEX",  # Abreviatura común de Estado de México
    "Estado de Mexico": "MEX",  # Sin acento
    "Estado de México": "MEX",  # Con acento
}


def procesar_sheet1(df: pl.DataFrame, partido_mapping: Dict[str, str]) -> pl.DataFrame:
    """Procesar Sheet1: Limpiar nombres de partidos con mapa"""
    print("Procesando Sheet1...")
    return df.with_columns(
        [
            pl.col("partido_diputado").replace(
                partido_mapping, default=pl.col("partido_diputado")
            ),
            pl.col("tipo_eleccion").replace(
                tipo_eleccion_mapping, default=pl.col("tipo_eleccion")
            ),
            pl.col("entidad").replace(entidad_mapping, default=pl.col("entidad")),
            pl.col("fecha_nacimiento")
            .str.strptime(
                pl.Date,
                format="%d-%m-%Y",  # %y funciona para años con 2 digitos e infiere (00-69 = 2000-2069, 70-99 = 1970-1999)
                strict=False,  # Regresa null cuando hay errores de formato en lugar de dar error.
            )
            .alias("fecha_nacimiento_limpia"),
            normalizar_expresionespl(pl.col("nombre_completo")).alias(
                "nombre_completo_limpio"
            ),
            normalizar_expresionespl(pl.col("suplente")).alias("suplente_limpio"),
            normalizar_expresionespl(pl.col("cabecera")).alias("cabecera_limpia"),
            pl.col("legislatura_activo").replace(
                legislatura_mapping, default=pl.col("legislatura_activo")
            ),
        ]
    )


# ==================== PROCESAMIENTO SHEET2 ====================

tipo_comite_mapping = {
    "ORDINARIA": "ordinaria",
    "COMITÉ": "comite",
    "ESPECIAL": "especial",
    "BICAMARAL": "bicamaral",
    "": "",
}

def process_sheet2(df: pl.DataFrame) -> pl.DataFrame:
    """
    Process Sheet2: Restructure committee assignments for profile analysis

    This function transforms the data so each deputy (dip_id) has one row
    with columns indicating their committee memberships across all types.
    """
    print("Procesando Sheet2...")

    # Normalize committee names for consistency
    df = df.with_columns(
        normalizar_expresionespl(pl.col("nombre_comite")).alias(
            "nombre_comite_normalizado"
        )
    )

    # Standardize tipo_comite values
    df = df.with_columns(
        pl.col("tipo_comite")
        .replace(tipo_comite_mapping, default=pl.col("tipo_comite"))
        .alias("tipo_comite_std")
    )

    # Group by deputy and committee type to collect all committees
    df_grouped = df.group_by(["dip_id", "tipo_comite_std"]).agg([
        pl.col("nombre_comite_normalizado").alias("comites"),
        pl.col("nombre_comite_normalizado").count().alias("num_comites")
    ])

    # Create a wide format where each committee type becomes columns
    result_rows = []

    for dip_id in df["dip_id"].unique().sort():
        # Get all committee memberships for this deputy
        deputy_data = df_grouped.filter(pl.col("dip_id") == dip_id)

        # Initialize the row for this deputy
        row = {"dip_id": dip_id}

        # Track total committees across all types
        total_committees = 0

        # Process each committee type
        for tipo in ["ordinaria", "comite", "especial", "bicamaral"]:
            # Filter for this specific committee type
            tipo_data = deputy_data.filter(pl.col("tipo_comite_std") == tipo)

            if len(tipo_data) > 0:
                # Get the list of committees
                comites_list = tipo_data["comites"][0]
                num_comites = tipo_data["num_comites"][0]

                # Add count column
                row[f"num_{tipo}"] = num_comites
                total_committees += num_comites

                # Add individual committee names as separate columns
                for idx, comite in enumerate(comites_list, start=1):
                    if comite is not None and comite != "":
                        row[f"{tipo}_{idx}"] = comite
            else:
                # Deputy doesn't belong to this committee type
                row[f"num_{tipo}"] = 0

        # Add total count
        row["total_comites"] = total_committees

        result_rows.append(row)

    # Convert to DataFrame
    result_df = pl.DataFrame(result_rows)

    print(f"Procesados {len(result_rows)} diputados")

    return result_df


# ==================== PROCESAMIENTO SHEET3 ====================

#Mapeo de actividades
actividades_mapping = {
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

# Standarizar los valores de la variable tipo del Sheet3
df = df.with_columns(
   pl.col("tipo")
   .replace(tipo_comite_mapping, default=pl.col("tipo"))
   .alias("tipo_actividad_std")

def safe_get_value(df: pl.DataFrame, column: str, idx: int) -> str:
    """Safely extract value from dataframe"""
    value = df[column][idx] if df[column][idx] is not None else ""
    return str(value)


def process_actividad_empresarial(dip_data: pl.DataFrame) -> Dict:
    """Procesamiento de tipo exp_empresarial"""
    actividad_emp = dip_data.filter(pl.col("tipo") == "exp_empresarial")
    result = {}

    for idx in range(len(actividad_emp)):
        detalle = safe_get_value(actividad_emp, "detalle", idx)
        descripcion = safe_get_value(actividad_emp, "descripcion", idx)
        periodo = safe_get_value(actividad_emp, "periodo", idx)
        result[f"actividad_empresarial_{idx + 1}"] = (
            f"{detalle},{descripcion},{periodo}"
        )

    return result


def process_actividad_docente(dip_data: pl.DataFrame) -> Dict:
    """Procesamiento de actividad docente"""
    actividad_doc = dip_data.filter(pl.col("tipo") == "exp_docente")
    has_docente = False

    if len(actividad_doc) > 0 and "actividad" in actividad_doc.columns:
        has_docente = (actividad_doc["actividad"] == "Docente").any()

    return {"actividad_docente": 1 if (len(actividad_doc) > 0 and has_docente) else 0}


def process_experiencia_apf(dip_data: pl.DataFrame) -> Dict:
    """Procesamiento de experiencia en administracion publica federal"""
    exp_apf = dip_data.filter(pl.col("tipo") == "exp_apf")
    result = {"experiencia_apf": 1 if len(exp_apf) > 0 else 0}

    for idx in range(len(exp_apf)):
        descripcion = safe_get_value(exp_apf, "descripcion", idx)
        detalle = safe_get_value(exp_apf, "detalle", idx)
        result[f"detalle_exp_apf_{idx + 1}"] = f"{descripcion},{detalle}"

    return result


def process_experiencia_aplocal(dip_data: pl.DataFrame) -> Dict:
    """Procesamiento de experiencia en admin publica local (estatal o municipal)"""
    exp_aplocal = dip_data.filter(pl.col("tipo") == "exp_aplocal")
    result = {"experiencia_aplocal": 1 if len(exp_aplocal) > 0 else 0}

    for idx in range(len(exp_aplocal)):
        descripcion = safe_get_value(exp_aplocal, "descripcion", idx)
        detalle = safe_get_value(exp_aplocal, "detalle", idx)
        result[f"detalle_exp_aplocal_{idx + 1}"] = f"{descripcion},{detalle}"

    return result


def process_asociaciones(dip_data: pl.DataFrame) -> Dict:
    """Procesamiento de actividad en asociaciones de la soc civil"""
    asociaciones = dip_data.filter(pl.col("tipo") == "exp_asociaciones")
    result = {"asociaciones": 1 if len(asociaciones) > 0 else 0}

    for idx in range(len(asociaciones)):
        result[f"asociaciones_rol_{idx + 1}"] = (
            asociaciones["descripcion"][idx]
            if asociaciones["descripcion"][idx] is not None
            else ""
        )
        result[f"asociaciones_detalle_{idx + 1}"] = (
            asociaciones["detalle"][idx]
            if asociaciones["detalle"][idx] is not None
            else ""
        )

    return result


def process_cargo_eleccion_popular(dip_data: pl.DataFrame) -> Dict:
    """Procesamiento de info de cargos de eleccion popular previos el inicio de la actual legislatura del diputado"""
    cargo_elec = dip_data.filter(pl.col("tipo") == "cargos_electos_previos")
    result = {"cargo_eleccion_popular": 1 if len(cargo_elec) > 0 else 0}

    for idx in range(len(cargo_elec)):
        result[f"cargo_eleccion_popular_{idx + 1}"] = (
            cargo_elec["descripcion"][idx]
            if cargo_elec["descripcion"][idx] is not None
            else ""
        )
        result[f"cargo_eleccion_popular_partido_{idx + 1}"] = (
            cargo_elec["detalle"][idx] if cargo_elec["detalle"][idx] is not None else ""
        )
        result[f"cargo_eleccion_popular_periodo_{idx + 1}"] = (
            cargo_elec["periodo"][idx] if cargo_elec["periodo"][idx] is not None else ""
        )

    return result


def process_experiencia_legislativa(dip_data: pl.DataFrame) -> Dict:
    """Procesamiento de experiencia legislativa previa de un diputado"""
    exp_leg = dip_data.filter(
        pl.col("tipo") == "cargos_legislativos_previa"
    )
    result = {"experiencia_legislativa": 1 if len(exp_leg) > 0 else 0}

    for idx in range(len(exp_leg)):
        result[f"experiencia_legislativa_detalle_{idx + 1}"] = (
            exp_leg["descripcion"][idx]
            if exp_leg["descripcion"][idx] is not None
            else ""
        )
        result[f"experiencia_legislativa_legislatura_{idx + 1}"] = (
            exp_leg["detalle"][idx] if exp_leg["detalle"][idx] is not None else ""
        )

    return result


def process_escolaridad(dip_data: pl.DataFrame) -> Dict:
    """Procesamiento de informacion de escolaridad del diputado"""
    escolaridad = dip_data.filter(pl.col("tipo") == "escolaridad")
    result = {}

    for idx in range(len(escolaridad)):
        result[f"escolaridad_tipo_{idx + 1}"] = (
            escolaridad["descripcion"][idx]
            if escolaridad["descripcion"][idx] is not None
            else ""
        )
        result[f"escolaridad_detalle_{idx + 1}"] = (
            escolaridad["detalle"][idx]
            if escolaridad["detalle"][idx] is not None
            else ""
        )
        result[f"escolaridad_periodo_{idx + 1}"] = (
            escolaridad["periodo"][idx]
            if escolaridad["periodo"][idx] is not None
            else ""
        )

    return result


def process_experiencia_legislativa_previa(dip_data: pl.DataFrame) -> Dict:
    """Procesamiento de experiencia legislativa previa antes de iniciar la actual legislatura"""
    exp_leg_prev = dip_data.filter(pl.col("tipo") == "exp_leg_previa")
    result = {}

    for idx in range(len(exp_leg_prev)):
        result[f"exp_leg_previa_{idx + 1}"] = (
            exp_leg_prev["descripcion"][idx]
            if exp_leg_prev["descripcion"][idx] is not None
            else ""
        )
        result[f"exp_leg_previa_legislatura_{idx + 1}"] = (
            exp_leg_prev["detalle"][idx]
            if exp_leg_prev["detalle"][idx] is not None
            else ""
        )
        result[f"exp_leg_previa_yr_{idx + 1}"] = (
            exp_leg_prev["periodo"][idx]
            if exp_leg_prev["periodo"][idx] is not None
            else ""
        )

    return result


def process_empleo_privado(dip_data: pl.DataFrame) -> Dict:
    """Procesamiento de empleo en el sector privado previo al iniico de la legislatura"""
    emp_priv = dip_data.filter(pl.col("tipo") == "exp_laboral_privada")
    result = {"empleo_privado": 1 if len(emp_priv) > 0 else 0}

    for idx in range(len(emp_priv)):
        result[f"empleo_privado_{idx + 1}"] = (
            emp_priv["descripcion"][idx]
            if emp_priv["descripcion"][idx] is not None
            else ""
        )
        result[f"empleo_privado_empresa_{idx + 1}"] = (
            emp_priv["detalle"][idx] if emp_priv["detalle"][idx] is not None else ""
        )
        result[f"empleo_privado_yr_{idx + 1}"] = (
            emp_priv["periodo"][idx] if emp_priv["periodo"][idx] is not None else ""
        )

    return result


def process_deportista(dip_data: pl.DataFrame) -> Dict:
    """Procesamiento de informacion de diputados con experiencia deportiva profesional probada"""
    deportista = dip_data.filter(pl.col("tipo") == "logros_deportivos")
    return {"deportista_altorend": 1 if len(deportista) > 0 else 0}


def process_experiencia_politica(dip_data: pl.DataFrame) -> Dict:
    """Procesamiento de experiencia política de diputados antes de comenzar la legislatura"""
    exp_pol = dip_data.filter(pl.col("tipo") == "exp_politica")
    result = {"experiencia_pol": 1 if len(exp_pol) > 0 else 0}

    for idx in range(len(exp_pol)):
        result[f"exp_pol_{idx + 1}"] = (
            exp_pol["descripcion"][idx]
            if exp_pol["descripcion"][idx] is not None
            else ""
        )
        result[f"exp_pol_org_{idx + 1}"] = (
            exp_pol["detalle"][idx] if exp_pol["detalle"][idx] is not None else ""
        )

    return result


def process_deputy_profile(dip_id: int, dip_data: pl.DataFrame) -> Dict:
    """Process all profile information for a single deputy"""
    result = {"dip_id": dip_id}

    # Combine all profile processing functions
    result.update(process_actividad_empresarial(dip_data))
    result.update(process_actividad_docente(dip_data))
    result.update(process_experiencia_apf(dip_data))
    result.update(process_experiencia_aplocal(dip_data))
    result.update(process_asociaciones(dip_data))
    result.update(process_cargo_eleccion_popular(dip_data))
    result.update(process_experiencia_legislativa(dip_data))
    result.update(process_escolaridad(dip_data))
    result.update(process_experiencia_legislativa_previa(dip_data))
    result.update(process_empleo_privado(dip_data))
    result.update(process_deportista(dip_data))
    result.update(process_experiencia_politica(dip_data))

    return result


def process_sheet3(df: pl.DataFrame) -> pl.DataFrame:
    """Procesando Sheet3: Perfiles de diputados y experiencia"""
    print("Procesando Sheet3...")

    results = []
    for dip_id in df["dip_id"].unique().sort():
        dip_data = df.filter(pl.col("dip_id") == dip_id)
        results.append(process_deputy_profile(dip_id, dip_data))

    return pl.DataFrame(results) if results else pl.DataFrame()


# ==================== INTEGRACION DE DATOS ====================


def merge_dataframes(
    df1: pl.DataFrame, df2: pl.DataFrame, df3: pl.DataFrame
) -> pl.DataFrame:
    """Merge all processed dataframes"""
    print("Consolidating all data...")

    df_final = df1

    if not df2.is_empty():
        df_final = df_final.join(df2, on="dip_id", how="left")

    if not df3.is_empty():
        df_final = df_final.join(df3, on="dip_id", how="left")

    return df_final.sort("dip_id")


# ==================== TRANSFORMACION DE DATOS ====================


def format_dates(df: pl.DataFrame) -> pl.DataFrame:
    """Formateo de columnas de fechas a DD-MM-YYYY"""
    print("Formateo de datos...")

    if "fecha_nacimiento" in df.columns:
        df = df.with_columns(
            pl.col("fecha_nacimiento").str.to_date(strict=False).dt.strftime("%d-%m-%Y")
        )

    return df


def natural_sort_key(col_name: str) -> List:
    """Generar mecanismos de organizacion de columnas por nombre"""
    parts = re.split(r"(\d+)", col_name)
    return [int(part) if part.isdigit() else part for part in parts]


def get_column_groups() -> Dict[str, List[str]]:
    """Definir estructura de agrupacion de columnas"""
    return {
        "base_info": [
            "dip_id",
            "nombre_completo",
            "entidad",
            "cabecera",
            "distrito_diputacion",
            "partido_diputado",
            "tipo_eleccion",
            "curul",
            "fecha_nacimiento",
            "suplente",
            "Url",
            "legislatura_activo",
        ],
        "nombre_comite": [],
        "tipo_comite": [],
        "actividad_empresarial": [],
        "actividad_docente": ["actividad_docente"],
        "experiencia_apf": ["experiencia_apf"],
        "detalle_exp_apf": [],
        "experiencia_aplocal": ["experiencia_aplocal"],
        "detalle_exp_aplocal": [],
        "asociaciones": ["asociaciones"],
        "asociaciones_rol": [],
        "asociaciones_detalle": [],
        "cargo_eleccion_popular": ["cargo_eleccion_popular"],
        "cargo_eleccion_popular_numbered": [],
        "cargo_eleccion_popular_partido": [],
        "cargo_eleccion_popular_periodo": [],
        "experiencia_legislativa": ["experiencia_legislativa"],
        "experiencia_legislativa_detalle": [],
        "experiencia_legislativa_legislatura": [],
        "escolaridad_tipo": [],
        "escolaridad_detalle": [],
        "escolaridad_periodo": [],
        "exp_leg_previa": [],
        "exp_leg_previa_legislatura": [],
        "exp_leg_previa_yr": [],
        "empleo_privado": ["empleo_privado"],
        "empleo_privado_numbered": [],
        "empleo_privado_empresa": [],
        "empleo_privado_yr": [],
        "deportista_altorend": ["deportista_altorend"],
        "exp_pol": [],
        "exp_pol_org": [],
    }


def categorize_columns(
    df: pl.DataFrame, column_groups: Dict[str, List[str]]
) -> Dict[str, List[str]]:
    """Categorizar las columnas del dataframe en grupos"""
    all_columns = df.columns

    for col in all_columns:
        if col in column_groups["base_info"]:
            continue
        elif col.startswith("nombre_comite"):
            column_groups["nombre_comite"].append(col)
        elif col.startswith("actividad_empresarial"):
            column_groups["actividad_empresarial"].append(col)
        elif col.startswith("detalle_exp_apf"):
            column_groups["detalle_exp_apf"].append(col)
        elif col.startswith("detalle_exp_aplocal"):
            column_groups["detalle_exp_aplocal"].append(col)
        elif col.startswith("asociaciones_rol"):
            column_groups["asociaciones_rol"].append(col)
        elif col.startswith("asociaciones_detalle"):
            column_groups["asociaciones_detalle"].append(col)
        elif col.startswith("cargo_eleccion_popular_partido"):
            column_groups["cargo_eleccion_popular_partido"].append(col)
        elif col.startswith("cargo_eleccion_popular_periodo"):
            column_groups["cargo_eleccion_popular_periodo"].append(col)
        elif (
            col.startswith("cargo_eleccion_popular_")
            and col != "cargo_eleccion_popular"
        ):
            column_groups["cargo_eleccion_popular_numbered"].append(col)
        elif col.startswith("experiencia_legislativa_detalle"):
            column_groups["experiencia_legislativa_detalle"].append(col)
        elif col.startswith("experiencia_legislativa_legislatura"):
            column_groups["experiencia_legislativa_legislatura"].append(col)
        elif col.startswith("escolaridad_tipo"):
            column_groups["escolaridad_tipo"].append(col)
        elif col.startswith("escolaridad_detalle"):
            column_groups["escolaridad_detalle"].append(col)
        elif col.startswith("escolaridad_periodo"):
            column_groups["escolaridad_periodo"].append(col)
        elif col.startswith("exp_leg_previa_legislatura"):
            column_groups["exp_leg_previa_legislatura"].append(col)
        elif col.startswith("exp_leg_previa_yr"):
            column_groups["exp_leg_previa_yr"].append(col)
        elif col.startswith("exp_leg_previa_") and col != "exp_leg_previa":
            column_groups["exp_leg_previa"].append(col)
        elif col.startswith("empleo_privado_empresa"):
            column_groups["empleo_privado_empresa"].append(col)
        elif col.startswith("empleo_privado_yr"):
            column_groups["empleo_privado_yr"].append(col)
        elif col.startswith("empleo_privado_") and col != "empleo_privado":
            column_groups["empleo_privado_numbered"].append(col)
        elif col.startswith("exp_pol_org"):
            column_groups["exp_pol_org"].append(col)
        elif col.startswith("exp_pol"):
            column_groups["exp_pol"].append(col)

    return column_groups


def reorder_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Reorder dataframe columns by logical grouping"""
    print("Grouping similar columns...")

    column_groups = get_column_groups()
    column_groups = categorize_columns(df, column_groups)

    # Sort numbered columns naturally
    for key in column_groups:
        if column_groups[key]:
            column_groups[key].sort(key=natural_sort_key)

    # Create final column order
    ordered_columns = []
    for group_name, columns in column_groups.items():
        ordered_columns.extend(columns)

    return df.select(ordered_columns)


# ==================== PIPELINE ORCHESTRATION ====================


def run_pipeline(config: PipelineConfig):
    """Ejecucion del data pipeline"""
    print(f"Iniciando data pipeline...")
    print(f"Input: {config.input_file}")
    print(f"Output: {config.output_file}")

    # Cargar datos
    df_sheet1 = load_excel_sheet(config.input_file, "Sheet1")
    df_sheet2 = load_excel_sheet(config.input_file, "Sheet2")
    df_sheet3 = load_excel_sheet(config.input_file, "Sheet3")

    # Procesar cada hoja del doc de excel fuente
    df_sheet1_processed = procesar_sheet1(df_sheet1, config.partido_mapping)
    df_sheet2_processed = process_sheet2(df_sheet2)
    df_sheet3_processed = process_sheet3(df_sheet3)

    # Hacer merge de los datos generados
    df_final = merge_dataframes(
        df_sheet1_processed, df_sheet2_processed, df_sheet3_processed
    )

    # Transformar datos
    df_final = format_dates(df_final)
    df_final = reorder_columns(df_final)

    # Guardar resultados del df integrados
    print("Guardando archivo de salida...")
    df_final.write_parquet(config.output_file)

    print(f"\n{'=' * 60}")
    print(f"Procesamiento completo!")
    print(f"Output guardado en : {config.output_file}")
    print(f"Total filas: {len(df_final)}")
    print(f"Total columnas: {len(df_final.columns)}")
    print(f"{'=' * 60}")

    return df_final


# ==================== MAIN ====================

if __name__ == "__main__":
    config = PipelineConfig.default()
    df_result = run_pipeline(config)
