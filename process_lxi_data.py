import pandas as pd
import numpy as np
from collections import defaultdict

# Load the Excel file
input_file = r'C:\Users\zigma\Projects\CongresoProject\data\raw\LXI.xlsx'
output_file = r'C:\Users\zigma\Projects\CongresoProject\data\processed\LXI_processed.xlsx'

print("Loading Excel file...")
xls = pd.ExcelFile(input_file)

# ===== SHEET 1 PROCESSING =====
print("Processing Sheet1...")
df_sheet1 = pd.read_excel(input_file, sheet_name='Sheet1')

# Clean the 'partido_diputado' column
partido_mapping = {
    'PRI01': 'PRI',
    'PAN': 'PAN',
    'PRD01': 'PRD',
    'LOGVRD': 'PVerde',
    'LOGPT': 'PT',
    'PANAL': 'PANAL',
    'LOGO_MOVIMIENTO_CIUDADANO': 'MovCiudadano'
}

df_sheet1['partido_diputado'] = df_sheet1['partido_diputado'].map(partido_mapping).fillna(df_sheet1['partido_diputado'])

# ===== SHEET 2 PROCESSING =====
print("Processing Sheet2...")
df_sheet2 = pd.read_excel(input_file, sheet_name='Sheet2')

# Filter for ORDINARIA tipo_comite
df_sheet2_ordinaria = df_sheet2[df_sheet2['tipo_comite'] == 'ORDINARIA'].copy()

# Split nombre_comite and create numbered columns
sheet2_results = []
for dip_id, group in df_sheet2_ordinaria.groupby('dip_id'):
    #Extract the original column
    tipocomite = group['tipo_comite'].iloc[0]
    
    result_row = {'dip_id': dip_id, 'tipo_comite':tipocomite}
    
    for idx, (_, row) in enumerate(group.iterrows(), start=1):
        # Split the nombre_comite by comma
        if pd.notna(row['nombre_comite']):
            comite_parts = [part.strip() for part in str(row['nombre_comite']).split(',')]
            for part_idx, part in enumerate(comite_parts, start=1):
                col_name = f'nombre_comite_{idx}_{part_idx}' if len(comite_parts) > 1 else f'nombre_comite_{idx}'
                result_row[col_name] = part
        else:
            result_row[f'nombre_comite_{idx}'] = None
    
    sheet2_results.append(result_row)

df_sheet2_processed = pd.DataFrame(sheet2_results)

# ===== SHEET 3 PROCESSING =====
print("Processing Sheet3...")
df_sheet3 = pd.read_excel(input_file, sheet_name='Sheet3')

# Initialize result dictionary for each dip_id
sheet3_results = defaultdict(lambda: {'dip_id': None})

for dip_id in df_sheet3['dip_id'].unique():
    sheet3_results[dip_id]['dip_id'] = dip_id
    dip_data = df_sheet3[df_sheet3['dip_id'] == dip_id]
    
    # 1. Actividad Empresarial
    actividad_emp = dip_data[dip_data['tipo'] == 'Actividad Empresarial']
    for idx, (_, row) in enumerate(actividad_emp.iterrows(), start=1):
        detalle = str(row['detalle']) if pd.notna(row['detalle']) else ''
        descripcion = str(row['descripcion']) if pd.notna(row['descripcion']) else ''
        periodo = str(row['periodo']) if pd.notna(row['periodo']) else ''
        sheet3_results[dip_id][f'actividad_empresarial_{idx}'] = f"{detalle},{descripcion},{periodo}"
    
    # 2. Actividades Docentes
    actividad_doc = dip_data[dip_data['tipo'] == 'ACTIVIDADES DOCENTES']
    has_docente = any((actividad_doc['actividad'] == 'Docente').values) if 'actividad' in actividad_doc.columns else False
    sheet3_results[dip_id]['actividad_docente'] = 1 if (len(actividad_doc) > 0 and has_docente) else 0
    
    # 3. Experiencia APF
    exp_apf = dip_data[dip_data['tipo'] == 'ADMINISTRACIÓN PÚBLICA FEDERAL']
    sheet3_results[dip_id]['experiencia_apf'] = 1 if len(exp_apf) > 0 else 0
    
    # 4. Detalle experiencia APF
    for idx, (_, row) in enumerate(exp_apf.iterrows(), start=1):
        descripcion = str(row['descripcion']) if pd.notna(row['descripcion']) else ''
        detalle = str(row['detalle']) if pd.notna(row['detalle']) else ''
        sheet3_results[dip_id][f'detalle_exp_apf_{idx}'] = f"{descripcion},{detalle}"
    
    # 5. Experiencia AP Local
    exp_aplocal = dip_data[dip_data['tipo'] == 'ADMINISTRACIÓN PÚBLICA LOCAL']
    sheet3_results[dip_id]['experiencia_aplocal'] = 1 if len(exp_aplocal) > 0 else 0
    
    # 6. Detalle experiencia AP Local (note: specification says detalle_exp_apf_# but likely meant detalle_exp_aplocal_#)
    for idx, (_, row) in enumerate(exp_aplocal.iterrows(), start=1):
        descripcion = str(row['descripcion']) if pd.notna(row['descripcion']) else ''
        detalle = str(row['detalle']) if pd.notna(row['detalle']) else ''
        sheet3_results[dip_id][f'detalle_exp_aplocal_{idx}'] = f"{descripcion},{detalle}"
    
    # 7. Asociaciones
    asociaciones = dip_data[dip_data['tipo'] == 'ASOCIACIONES A LAS QUE PERTENECE']
    sheet3_results[dip_id]['asociaciones'] = 1 if len(asociaciones) > 0 else 0
    
    # 8 & 9. Asociaciones rol y detalle
    for idx, (_, row) in enumerate(asociaciones.iterrows(), start=1):
        sheet3_results[dip_id][f'asociaciones_rol_{idx}'] = row['descripcion'] if pd.notna(row['descripcion']) else ''
        sheet3_results[dip_id][f'asociaciones_detalle_{idx}'] = row['detalle'] if pd.notna(row['detalle']) else ''
    
    # 10. Cargo elección popular
    cargo_elec = dip_data[dip_data['tipo'] == 'CARGOS DE ELECCIÓN POPULAR']
    sheet3_results[dip_id]['cargo_eleccion_popular'] = 1 if len(cargo_elec) > 0 else 0
    
    # 11, 12, 13. Cargo elección popular details
    for idx, (_, row) in enumerate(cargo_elec.iterrows(), start=1):
        sheet3_results[dip_id][f'cargo_eleccion_popular_{idx}'] = row['descripcion'] if pd.notna(row['descripcion']) else ''
        sheet3_results[dip_id][f'cargo_eleccion_popular_partido_{idx}'] = row['detalle'] if pd.notna(row['detalle']) else ''
        sheet3_results[dip_id][f'cargo_eleccion_popular_periodo_{idx}'] = row['periodo'] if pd.notna(row['periodo']) else ''
    
    # 14. Experiencia legislativa
    exp_leg = dip_data[dip_data['tipo'] == 'CARGOS EN LEGISLATURAS LOCALES O FEDERALES']
    sheet3_results[dip_id]['experiencia_legislativa'] = 1 if len(exp_leg) > 0 else 0
    
    # 15 & 16. Experiencia legislativa details
    for idx, (_, row) in enumerate(exp_leg.iterrows(), start=1):
        sheet3_results[dip_id][f'experiencia_legislativa_detalle_{idx}'] = row['descripcion'] if pd.notna(row['descripcion']) else ''
        sheet3_results[dip_id][f'experiencia_legislativa_legislatura_{idx}'] = row['detalle'] if pd.notna(row['detalle']) else ''
    
    # 17, 18, 19. Escolaridad
    escolaridad = dip_data[dip_data['tipo'] == 'ESCOLARIDAD']
    for idx, (_, row) in enumerate(escolaridad.iterrows(), start=1):
        sheet3_results[dip_id][f'escolaridad_tipo_{idx}'] = row['descripcion'] if pd.notna(row['descripcion']) else ''
        sheet3_results[dip_id][f'escolaridad_detalle_{idx}'] = row['detalle'] if pd.notna(row['detalle']) else ''
        sheet3_results[dip_id][f'escolaridad_periodo_{idx}'] = row['periodo'] if pd.notna(row['periodo']) else ''
    
    # 20, 21, 22. Experiencia legislativa previa
    exp_leg_prev = dip_data[dip_data['tipo'] == 'EXPERIENCIA LEGISLATIVA']
    for idx, (_, row) in enumerate(exp_leg_prev.iterrows(), start=1):
        sheet3_results[dip_id][f'exp_leg_previa_{idx}'] = row['descripcion'] if pd.notna(row['descripcion']) else ''
        sheet3_results[dip_id][f'exp_leg_previa_legislatura_{idx}'] = row['detalle'] if pd.notna(row['detalle']) else ''
        sheet3_results[dip_id][f'exp_leg_previa_yr_{idx}'] = row['periodo'] if pd.notna(row['periodo']) else ''
    
    # 23. Empleo privado
    emp_priv = dip_data[dip_data['tipo'] == 'INICIATIVA PRIVADA']
    sheet3_results[dip_id]['empleo_privado'] = 1 if len(emp_priv) > 0 else 0
    
    # 24, 25, 26. Empleo privado details
    for idx, (_, row) in enumerate(emp_priv.iterrows(), start=1):
        sheet3_results[dip_id][f'empleo_privado_{idx}'] = row['descripcion'] if pd.notna(row['descripcion']) else ''
        sheet3_results[dip_id][f'empleo_privado_empresa_{idx}'] = row['detalle'] if pd.notna(row['detalle']) else ''
        sheet3_results[dip_id][f'empleo_privado_yr_{idx}'] = row['periodo'] if pd.notna(row['periodo']) else ''
    
    # 27. Deportista alto rendimiento
    deportista = dip_data[dip_data['tipo'] == 'LOGROS DEPORTIVOS MÁS DESTACADOS']
    sheet3_results[dip_id]['deportista_altorend'] = 1 if len(deportista) > 0 else 0
    
    # 28 & 29. Experiencia política
    exp_pol = dip_data[dip_data['tipo'] == 'TRAYECTORIA POLÍTICA']
    for idx, (_, row) in enumerate(exp_pol.iterrows(), start=1):
        sheet3_results[dip_id][f'exp_pol_{idx}'] = row['descripcion'] if pd.notna(row['descripcion']) else ''
        sheet3_results[dip_id][f'exp_pol_org_{idx}'] = row['detalle'] if pd.notna(row['detalle']) else ''

df_sheet3_processed = pd.DataFrame.from_dict(sheet3_results, orient='index')

# ===== MERGE ALL DATA =====
print("Merging all data...")

# Start with sheet1 as base
df_final = df_sheet1.copy()

# Merge sheet2 data
if not df_sheet2_processed.empty:
    df_final = df_final.merge(df_sheet2_processed, on='dip_id', how='left')

# Merge sheet3 data
if not df_sheet3_processed.empty:
    df_final = df_final.merge(df_sheet3_processed, on='dip_id', how='left')

# Sort by dip_id
df_final = df_final.sort_values('dip_id').reset_index(drop=True)

# ===== FORMAT DATES =====
print("Formatting dates...")
# Convert fecha_nacimiento to DD-MM-YYYY format (no time)
if 'fecha_nacimiento' in df_final.columns:
    df_final['fecha_nacimiento'] = pd.to_datetime(df_final['fecha_nacimiento'], errors='coerce').dt.strftime('%d-%m-%Y')

# ===== GROUP SIMILAR COLUMNS =====
print("Grouping similar columns...")

# Define column order groups
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

# Categorize all columns
all_columns = list(df_final.columns)
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

# Sort numbered columns naturally (1, 2, 3... not 1, 10, 11, 2...)
def natural_sort_key(col_name):
    import re
    parts = re.split(r'(\d+)', col_name)
    return [int(part) if part.isdigit() else part for part in parts]

for key in column_groups:
    if column_groups[key]:
        column_groups[key].sort(key=natural_sort_key)

# Build final column order
ordered_columns = []
for group_name, columns in column_groups.items():
    ordered_columns.extend(columns)

# Reorder dataframe columns
df_final = df_final[ordered_columns]

# ===== SAVE OUTPUT =====
print("Saving output file...")
df_final.to_excel(output_file, index=False, engine='openpyxl')

print(f"Processing complete! Output saved to: {output_file}")
print(f"Total rows: {len(df_final)}")
print(f"Total columns: {len(df_final.columns)}")
