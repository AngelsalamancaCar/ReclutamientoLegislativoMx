import polars as pl
import re

# File paths
input_file = r'C:\Users\zigma\Projects\CongresoProject\data\raw\LXI.xlsx'
output_file = r'C:\Users\zigma\Projects\CongresoProject\data\processed\LXI_processed.xlsx'

print("Loading Excel file...")

# ===== Procesamiento Sheet1 =====
print("Procesando Sheet1...")
df_sheet1 = pl.read_excel(input_file, sheet_name='Sheet1')

# Clean the 'partido_diputado' column using mapping
partido_mapping = {
    'PRI01': 'PRI',
    'PAN': 'PAN',
    'PRD01': 'PRD',
    'LOGVRD': 'PVerde',
    'LOGPT': 'PT',
    'PANAL': 'PANAL',
    'LOGO_MOVIMIENTO_CIUDADANO': 'MovCiudadano'
}

df_sheet1 = df_sheet1.with_columns(
    pl.col('partido_diputado').replace(partido_mapping, default=pl.col('partido_diputado'))
)

# ===== Procesamiento Sheet2 =====
print("Procesando Sheet2...")

df_sheet2 = pl.read_excel(input_file, sheet_name='Sheet2')

# Filtro de ORDINARIA tipo_comite
df_sheet2_ordinaria = df_sheet2.filter(pl.col('tipo_comite') == 'ORDINARIA')

# Procesar datos de Sheet2 - separar nombre_comite y crear columnas con indice numerado
sheet2_results = []

for dip_id in df_sheet2_ordinaria['dip_id'].unique().sort():
    group = df_sheet2_ordinaria.filter(pl.col('dip_id') == dip_id)
    
    result_row = {
        'dip_id': dip_id,
        'tipo_comite': group['tipo_comite'][0]
    }
    
    for idx, row_idx in enumerate(range(len(group)), start=1):
        nombre_comite = group['nombre_comite'][row_idx]
        
        if nombre_comite is not None and nombre_comite != '':
            comite_parts = [part.strip() for part in str(nombre_comite).split(',')]
            
            if len(comite_parts) > 1:
                for part_idx, part in enumerate(comite_parts, start=1):
                    col_name = f'nombre_comite_{idx}_{part_idx}'
                    result_row[col_name] = part
            else:
                result_row[f'nombre_comite_{idx}'] = comite_parts[0]
        else:
            result_row[f'nombre_comite_{idx}'] = None
    
    sheet2_results.append(result_row)

df_sheet2_processed = pl.DataFrame(sheet2_results) if sheet2_results else pl.DataFrame()

# ===== Procesamiento SHEET 3 =====
print("Processing Sheet3...")
df_sheet3 = pl.read_excel(input_file, sheet_name='Sheet3')

sheet3_results = []

for dip_id in df_sheet3['dip_id'].unique().sort():
    dip_data = df_sheet3.filter(pl.col('dip_id') == dip_id)
    
    result_row = {'dip_id': dip_id}
    
    # Funcion auxiliar para filtrar datos de tipo de actividades en datos por tipo
    def get_tipo_data(tipo_value):
        return dip_data.filter(pl.col('tipo') == tipo_value)
    
    # 1. Actividad Empresarial
    actividad_emp = get_tipo_data('Actividad Empresarial')
    for idx in range(len(actividad_emp)):
        detalle = str(actividad_emp['detalle'][idx]) if actividad_emp['detalle'][idx] is not None else ''
        descripcion = str(actividad_emp['descripcion'][idx]) if actividad_emp['descripcion'][idx] is not None else ''
        periodo = str(actividad_emp['periodo'][idx]) if actividad_emp['periodo'][idx] is not None else ''
        result_row[f'actividad_empresarial_{idx+1}'] = f"{detalle},{descripcion},{periodo}"
    
    # 2. Actividades Docentes
    actividad_doc = get_tipo_data('ACTIVIDADES DOCENTES')
    has_docente = False
    if len(actividad_doc) > 0 and 'actividad' in actividad_doc.columns:
        has_docente = (actividad_doc['actividad'] == 'Docente').any()
    result_row['actividad_docente'] = 1 if (len(actividad_doc) > 0 and has_docente) else 0
    
    # 3. Experiencia APF
    exp_apf = get_tipo_data('ADMINISTRACIÃ"N PÃšBLICA FEDERAL')
    result_row['experiencia_apf'] = 1 if len(exp_apf) > 0 else 0
    
    # 4. Detalle experiencia APF
    for idx in range(len(exp_apf)):
        descripcion = str(exp_apf['descripcion'][idx]) if exp_apf['descripcion'][idx] is not None else ''
        detalle = str(exp_apf['detalle'][idx]) if exp_apf['detalle'][idx] is not None else ''
        result_row[f'detalle_exp_apf_{idx+1}'] = f"{descripcion},{detalle}"
    
    # 5. Experiencia AP Local
    exp_aplocal = get_tipo_data('ADMINISTRACIÃ"N PÃšBLICA LOCAL')
    result_row['experiencia_aplocal'] = 1 if len(exp_aplocal) > 0 else 0
    
    # 6. Detalle experiencia AP Local
    for idx in range(len(exp_aplocal)):
        descripcion = str(exp_aplocal['descripcion'][idx]) if exp_aplocal['descripcion'][idx] is not None else ''
        detalle = str(exp_aplocal['detalle'][idx]) if exp_aplocal['detalle'][idx] is not None else ''
        result_row[f'detalle_exp_aplocal_{idx+1}'] = f"{descripcion},{detalle}"
    
    # 7. Asociaciones
    asociaciones = get_tipo_data('ASOCIACIONES A LAS QUE PERTENECE')
    result_row['asociaciones'] = 1 if len(asociaciones) > 0 else 0
    
    # 8 & 9. Asociaciones rol y detalle
    for idx in range(len(asociaciones)):
        result_row[f'asociaciones_rol_{idx+1}'] = asociaciones['descripcion'][idx] if asociaciones['descripcion'][idx] is not None else ''
        result_row[f'asociaciones_detalle_{idx+1}'] = asociaciones['detalle'][idx] if asociaciones['detalle'][idx] is not None else ''
    
    # 10. Cargo elección popular
    cargo_elec = get_tipo_data('CARGOS DE ELECCIÃ"N POPULAR')
    result_row['cargo_eleccion_popular'] = 1 if len(cargo_elec) > 0 else 0
    
    # 11, 12, 13. Cargo elección popular details
    for idx in range(len(cargo_elec)):
        result_row[f'cargo_eleccion_popular_{idx+1}'] = cargo_elec['descripcion'][idx] if cargo_elec['descripcion'][idx] is not None else ''
        result_row[f'cargo_eleccion_popular_partido_{idx+1}'] = cargo_elec['detalle'][idx] if cargo_elec['detalle'][idx] is not None else ''
        result_row[f'cargo_eleccion_popular_periodo_{idx+1}'] = cargo_elec['periodo'][idx] if cargo_elec['periodo'][idx] is not None else ''
    
    # 14. Experiencia legislativa
    exp_leg = get_tipo_data('CARGOS EN LEGISLATURAS LOCALES O FEDERALES')
    result_row['experiencia_legislativa'] = 1 if len(exp_leg) > 0 else 0
    
    # 15 & 16. Experiencia legislativa details
    for idx in range(len(exp_leg)):
        result_row[f'experiencia_legislativa_detalle_{idx+1}'] = exp_leg['descripcion'][idx] if exp_leg['descripcion'][idx] is not None else ''
        result_row[f'experiencia_legislativa_legislatura_{idx+1}'] = exp_leg['detalle'][idx] if exp_leg['detalle'][idx] is not None else ''
    
    # 17, 18, 19. Escolaridad
    escolaridad = get_tipo_data('ESCOLARIDAD')
    for idx in range(len(escolaridad)):
        result_row[f'escolaridad_tipo_{idx+1}'] = escolaridad['descripcion'][idx] if escolaridad['descripcion'][idx] is not None else ''
        result_row[f'escolaridad_detalle_{idx+1}'] = escolaridad['detalle'][idx] if escolaridad['detalle'][idx] is not None else ''
        result_row[f'escolaridad_periodo_{idx+1}'] = escolaridad['periodo'][idx] if escolaridad['periodo'][idx] is not None else ''
    
    # 20, 21, 22. Experiencia legislativa previa
    exp_leg_prev = get_tipo_data('EXPERIENCIA LEGISLATIVA')
    for idx in range(len(exp_leg_prev)):
        result_row[f'exp_leg_previa_{idx+1}'] = exp_leg_prev['descripcion'][idx] if exp_leg_prev['descripcion'][idx] is not None else ''
        result_row[f'exp_leg_previa_legislatura_{idx+1}'] = exp_leg_prev['detalle'][idx] if exp_leg_prev['detalle'][idx] is not None else ''
        result_row[f'exp_leg_previa_yr_{idx+1}'] = exp_leg_prev['periodo'][idx] if exp_leg_prev['periodo'][idx] is not None else ''
    
    # 23. Empleo privado
    emp_priv = get_tipo_data('INICIATIVA PRIVADA')
    result_row['empleo_privado'] = 1 if len(emp_priv) > 0 else 0
    
    # 24, 25, 26. Empleo privado details
    for idx in range(len(emp_priv)):
        result_row[f'empleo_privado_{idx+1}'] = emp_priv['descripcion'][idx] if emp_priv['descripcion'][idx] is not None else ''
        result_row[f'empleo_privado_empresa_{idx+1}'] = emp_priv['detalle'][idx] if emp_priv['detalle'][idx] is not None else ''
        result_row[f'empleo_privado_yr_{idx+1}'] = emp_priv['periodo'][idx] if emp_priv['periodo'][idx] is not None else ''
    
    # 27. Deportista alto rendimiento
    deportista = get_tipo_data('LOGROS DEPORTIVOS MÃS DESTACADOS')
    result_row['deportista_altorend'] = 1 if len(deportista) > 0 else 0
    
    # 28 & 29. Experiencia política
    exp_pol = get_tipo_data('TRAYECTORIA POLÃTICA')
    for idx in range(len(exp_pol)):
        result_row[f'exp_pol_{idx+1}'] = exp_pol['descripcion'][idx] if exp_pol['descripcion'][idx] is not None else ''
        result_row[f'exp_pol_org_{idx+1}'] = exp_pol['detalle'][idx] if exp_pol['detalle'][idx] is not None else ''
    
    sheet3_results.append(result_row)

df_sheet3_processed = pl.DataFrame(sheet3_results) if sheet3_results else pl.DataFrame()

# ===== Consolidar todos los datos =====
print("Consolidando todos los datos...")

# Se inicia el Sheet1 como base de la integración
df_final = df_sheet1

# Integrar el sheet2 a los datos
if not df_sheet2_processed.is_empty():
    df_final = df_final.join(df_sheet2_processed, on='dip_id', how='left')

# Integrar el Sheet3 a los datos
if not df_sheet3_processed.is_empty():
    df_final = df_final.join(df_sheet3_processed, on='dip_id', how='left')

# Organizar por dip_id
df_final = df_final.sort('dip_id')

# ===== Formatear fechas =====
print("Formateando fechas...")
# Convertir fecha_nacimiento a DD-MM-YYYY (remover tiempo generado al cargar)
if 'fecha_nacimiento' in df_final.columns:
    df_final = df_final.with_columns(
        pl.col('fecha_nacimiento')
        .str.to_date(strict=False)
        .dt.strftime('%d-%m-%Y')
    )

# ===== Agrupar columnas con nombres similares =====
print("Grouping similar columns...")

# Definir el orden de columnas
column_groups = {
    'base_info': ['dip_id', 'nombre_completo', 'entidad', 'cabecera', 'distrito_diputacion', 
                  'partido_diputado', 'tipo_eleccion', 'curul', 'fecha_nacimiento', 'suplente', 
                  'Url', 'legislatura_activo'],
    'nombre_comite': [],
    'tipo_comite': [],
    'actividad_empresarial': [],
    'actividad_docente': ['actividad_docente'],
    'experiencia_apf': ['experiencia_apf'],
    'detalle_exp_apf': [],
    'experiencia_aplocal': ['experiencia_aplocal'],
    'detalle_exp_aplocal': [],
    'asociaciones': ['asociaciones'],
    'asociaciones_rol': [],
    'asociaciones_detalle': [],
    'cargo_eleccion_popular': ['cargo_eleccion_popular'],
    'cargo_eleccion_popular_numbered': [],
    'cargo_eleccion_popular_partido': [],
    'cargo_eleccion_popular_periodo': [],
    'experiencia_legislativa': ['experiencia_legislativa'],
    'experiencia_legislativa_detalle': [],
    'experiencia_legislativa_legislatura': [],
    'escolaridad_tipo': [],
    'escolaridad_detalle': [],
    'escolaridad_periodo': [],
    'exp_leg_previa': [],
    'exp_leg_previa_legislatura': [],
    'exp_leg_previa_yr': [],
    'empleo_privado': ['empleo_privado'],
    'empleo_privado_numbered': [],
    'empleo_privado_empresa': [],
    'empleo_privado_yr': [],
    'deportista_altorend': ['deportista_altorend'],
    'exp_pol': [],
    'exp_pol_org': []
}

# Categorizar todas las columnas
all_columns = df_final.columns
for col in all_columns:
    if col in column_groups['base_info']:
        continue
    elif col.startswith('nombre_comite'):
        column_groups['nombre_comite'].append(col)
    elif col.startswith('actividad_empresarial'):
        column_groups['actividad_empresarial'].append(col)
    elif col.startswith('detalle_exp_apf'):
        column_groups['detalle_exp_apf'].append(col)
    elif col.startswith('detalle_exp_aplocal'):
        column_groups['detalle_exp_aplocal'].append(col)
    elif col.startswith('asociaciones_rol'):
        column_groups['asociaciones_rol'].append(col)
    elif col.startswith('asociaciones_detalle'):
        column_groups['asociaciones_detalle'].append(col)
    elif col.startswith('cargo_eleccion_popular_partido'):
        column_groups['cargo_eleccion_popular_partido'].append(col)
    elif col.startswith('cargo_eleccion_popular_periodo'):
        column_groups['cargo_eleccion_popular_periodo'].append(col)
    elif col.startswith('cargo_eleccion_popular_') and col != 'cargo_eleccion_popular':
        column_groups['cargo_eleccion_popular_numbered'].append(col)
    elif col.startswith('experiencia_legislativa_detalle'):
        column_groups['experiencia_legislativa_detalle'].append(col)
    elif col.startswith('experiencia_legislativa_legislatura'):
        column_groups['experiencia_legislativa_legislatura'].append(col)
    elif col.startswith('escolaridad_tipo'):
        column_groups['escolaridad_tipo'].append(col)
    elif col.startswith('escolaridad_detalle'):
        column_groups['escolaridad_detalle'].append(col)
    elif col.startswith('escolaridad_periodo'):
        column_groups['escolaridad_periodo'].append(col)
    elif col.startswith('exp_leg_previa_legislatura'):
        column_groups['exp_leg_previa_legislatura'].append(col)
    elif col.startswith('exp_leg_previa_yr'):
        column_groups['exp_leg_previa_yr'].append(col)
    elif col.startswith('exp_leg_previa_') and col != 'exp_leg_previa':
        column_groups['exp_leg_previa'].append(col)
    elif col.startswith('empleo_privado_empresa'):
        column_groups['empleo_privado_empresa'].append(col)
    elif col.startswith('empleo_privado_yr'):
        column_groups['empleo_privado_yr'].append(col)
    elif col.startswith('empleo_privado_') and col != 'empleo_privado':
        column_groups['empleo_privado_numbered'].append(col)
    elif col.startswith('exp_pol_org'):
        column_groups['exp_pol_org'].append(col)
    elif col.startswith('exp_pol'):
        column_groups['exp_pol'].append(col)

# Organiza las columnas numeradas por indice natural(1, 2, 3... no 1, 10, 11, 2...)
def natural_sort_key(col_name):
    parts = re.split(r'(\d+)', col_name)
    return [int(part) if part.isdigit() else part for part in parts]

for key in column_groups:
    if column_groups[key]:
        column_groups[key].sort(key=natural_sort_key)

# Crear el orden final de las columnas
ordered_columns = []
for group_name, columns in column_groups.items():
    ordered_columns.extend(columns)

# Reordenar columnas del dataframe
df_final = df_final.select(ordered_columns)

# ===== Guardar output =====
print("Saving output file...")
df_final.write_excel(output_file)

print(f"Processing complete! Output saved to: {output_file}")
print(f"Total rows: {len(df_final)}")
print(f"Total columns: {len(df_final.columns)}")

