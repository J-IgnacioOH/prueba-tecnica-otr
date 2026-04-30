from pathlib import Path
import sqlite3
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
INPUT_FILE = BASE_DIR / "data" / "Datos Prueba Técnica.xlsx"
OUTPUT_DIR = BASE_DIR / "output"
DB_FILE = OUTPUT_DIR / "otr_clean.db"


def clean_text(value):
    if pd.isna(value):
        return ""
    return " ".join(str(value).strip().split())


def key_text(value):
    return clean_text(value).upper()


def clean_sku(value):
    if pd.isna(value):
        return ""
    value = str(value).strip()
    try:
        return str(int(float(value)))
    except ValueError:
        return value.lstrip("0") or value


def clean_status(value):
    return key_text(value)


def build_client_dictionary(clientes):
    clientes_dict = {
        key_text(row["NOMBRE_CLIENTE"]): clean_text(row["NOMBRE_CLIENTE"])
        for _, row in clientes.iterrows()
    }

    aliases = {
        "WALMART": "WALMART CHILE",
        "WALMART CHILE": "WALMART CHILE",
        "LIDER EXPRESS": "LIDER EXPRESS",
        "TOTTUS": "TOTTUS",
        "TOTTUS S.A.": "TOTTUS",
        "TOTTUS EXPRESS": "TOTTUS EXPRESS",
        "JUMBO": "JUMBO",
        "SANTA ISABEL": "SANTA ISABEL",
        "COMIDAS PREP OOH": "COMIDAS PREPARADAS OOH",
        "COMIDAS PREPARADAS OOH": "COMIDAS PREPARADAS OOH",
        "UNIMARC": "UNIMARC",
        "MAYORISTA 10": "MAYORISTA 10",
        "ALVI": "ALVI",
        "ACUENTA": "ACUENTA",
        "EKONO": "EKONO",
    }

    for alias, master_key in aliases.items():
        if master_key in clientes_dict:
            clientes_dict[alias] = clientes_dict[master_key]

    return clientes_dict


def normalize_client(value, clientes_dict):
    value_clean = clean_text(value)
    value_key = key_text(value)
    return clientes_dict.get(value_key, value_clean)


def read_input_file(input_file):
    otr = pd.read_excel(input_file, sheet_name="OTR")
    paletizado = pd.read_excel(input_file, sheet_name="PALETIZADO")
    clientes = pd.read_excel(input_file, sheet_name="CLIENTES")
    return otr, paletizado, clientes


def clean_clientes(clientes):
    clientes = clientes.copy()
    clientes["NOMBRE_CLIENTE"] = clientes["NOMBRE_CLIENTE"].apply(clean_text)
    clientes["CANAL"] = clientes["CANAL"].apply(clean_text)
    clientes["HOLDING"] = clientes["HOLDING"].apply(clean_text)
    clientes["REGION"] = clientes["REGION"].apply(clean_text)

    clientes = clientes[["NOMBRE_CLIENTE", "CANAL", "HOLDING", "REGION"]]
    clientes = clientes.drop_duplicates(subset=["NOMBRE_CLIENTE"], keep="first")
    return clientes


def clean_productos(paletizado):
    productos = paletizado.copy()
    productos["SKU"] = productos["SKU"].apply(clean_sku)
    productos["DESCRIPCION"] = productos["Descripcion"].apply(clean_text)
    productos["PESO_KG"] = pd.to_numeric(productos["Peso_KG"], errors="coerce")
    productos["VOLUMEN_M3"] = pd.to_numeric(productos["Volumen_M3"], errors="coerce")
    productos["CANTIDAD_PALLET"] = pd.to_numeric(productos["Cantidad_pallet"], errors="coerce")
    productos["TIPO_PRODUCTO"] = productos["Tipo_Producto"].apply(clean_text)

    productos = productos[[
        "SKU",
        "DESCRIPCION",
        "PESO_KG",
        "VOLUMEN_M3",
        "CANTIDAD_PALLET",
        "TIPO_PRODUCTO",
    ]]

    productos = productos.drop_duplicates(subset=["SKU"], keep="first")
    return productos


def clean_otr(otr, clientes_limpios):
    otr = otr.copy()
    clientes_dict = build_client_dictionary(clientes_limpios)

    otr["PEDIDO"] = otr["PEDIDO"].apply(clean_text)
    otr["FECHA_PEDIDO"] = pd.to_datetime(otr["FECHA_PEDIDO"], errors="coerce", dayfirst=True)
    otr["FECHA_SOLICITADA_ENTREGA"] = pd.to_datetime(otr["FECHA_SOLICITADA_ENTREGA"], errors="coerce", dayfirst=True)
    otr["FECHA_ENTREGA"] = pd.to_datetime(otr["FECHA_ENTREGA"], errors="coerce", dayfirst=True)
    otr["NOMBRE_CLIENTE"] = otr["CLIENTE"].apply(lambda x: normalize_client(x, clientes_dict))
    otr["SKU"] = otr["SKU"].apply(clean_sku)
    otr["CANTIDAD"] = pd.to_numeric(otr["CANTIDAD"], errors="coerce").abs()
    otr["STATUS"] = otr["STATUS"].apply(clean_status)
    otr["DIAS_DIFERENCIA_ENTREGA"] = (
    otr["FECHA_ENTREGA"] - otr["FECHA_SOLICITADA_ENTREGA"]
    ).dt.days.astype("Int64")

    fact_otr = otr[[
        "PEDIDO",
        "FECHA_PEDIDO",
        "FECHA_SOLICITADA_ENTREGA",
        "FECHA_ENTREGA",
        "NOMBRE_CLIENTE",
        "SKU",
        "CANTIDAD",
        "STATUS",
        "DIAS_DIFERENCIA_ENTREGA",
    ]]

    return fact_otr


def add_missing_clients(fact_otr, clientes):
    clientes = clientes.copy()
    master_clients = set(clientes["NOMBRE_CLIENTE"])
    otr_clients = set(fact_otr["NOMBRE_CLIENTE"].dropna())
    missing_clients = sorted(otr_clients - master_clients)

    if missing_clients:
        missing_df = pd.DataFrame({
            "NOMBRE_CLIENTE": missing_clients,
            "CANAL": "SIN MAESTRO",
            "HOLDING": "SIN MAESTRO",
            "REGION": "SIN MAESTRO",
        })
        clientes = pd.concat([clientes, missing_df], ignore_index=True)

    return clientes


def build_data_quality_log(otr_raw, paletizado_raw, fact_otr, clientes_limpios):
    issues = []

    sku_pal = paletizado_raw["SKU"].apply(clean_sku)
    sku_duplicados = int(sku_pal.duplicated().sum())
    if sku_duplicados > 0:
        issues.append({
            "ISSUE_TYPE": "PRODUCTOS_DUPLICADOS",
            "DESCRIPCION": "SKU duplicados en la hoja PALETIZADO luego de estandarizar el formato.",
            "FILAS_AFECTADAS": sku_duplicados,
            "TRATAMIENTO": "Se conserva el primer registro por SKU para evitar duplicar pedidos al cruzar tablas.",
        })

    cantidades = pd.to_numeric(otr_raw["CANTIDAD"], errors="coerce")
    cantidades_negativas = int((cantidades < 0).sum())
    if cantidades_negativas > 0:
        issues.append({
            "ISSUE_TYPE": "CANTIDADES_NEGATIVAS",
            "DESCRIPCION": "Cantidades negativas en la hoja OTR.",
            "FILAS_AFECTADAS": cantidades_negativas,
            "TRATAMIENTO": "Se transforman a valor absoluto por tratarse de un análisis logístico de flujo físico. Devoluciones o ajustes deberían gestionarse en un flujo separado.",
        })

    master_clients = set(clientes_limpios[clientes_limpios["CANAL"] != "SIN MAESTRO"]["NOMBRE_CLIENTE"])
    clientes_sin_maestro = fact_otr[~fact_otr["NOMBRE_CLIENTE"].isin(master_clients)]
    if len(clientes_sin_maestro) > 0:
        issues.append({
            "ISSUE_TYPE": "CLIENTES_NO_EN_MAESTRO",
            "DESCRIPCION": "Clientes presentes en OTR que no existen en la hoja CLIENTES.",
            "FILAS_AFECTADAS": int(len(clientes_sin_maestro)),
            "TRATAMIENTO": "Se mantienen las transacciones y se agregan a la dimensión de clientes con atributos SIN MAESTRO.",
        })

    entregas_anticipadas = int((fact_otr["DIAS_DIFERENCIA_ENTREGA"] < 0).sum())
    if entregas_anticipadas > 0:
        issues.append({
            "ISSUE_TYPE": "ENTREGAS_ANTICIPADAS",
            "DESCRIPCION": "Registros donde la fecha de entrega es anterior a la fecha solicitada.",
            "FILAS_AFECTADAS": entregas_anticipadas,
            "TRATAMIENTO": "Se conserva la diferencia negativa porque representa entrega anticipada, no atraso.",
        })

    sku_otr = set(fact_otr["SKU"].dropna())
    sku_productos = set(paletizado_raw["SKU"].apply(clean_sku).dropna())
    sku_sin_maestro = sorted(sku_otr - sku_productos)
    if sku_sin_maestro:
        filas_afectadas = int(fact_otr["SKU"].isin(sku_sin_maestro).sum())
        issues.append({
            "ISSUE_TYPE": "SKU_NO_EN_MAESTRO",
            "DESCRIPCION": "SKU presentes en OTR que no existen en PALETIZADO.",
            "FILAS_AFECTADAS": filas_afectadas,
            "TRATAMIENTO": "Se mantienen los pedidos para no perder transacciones y se documenta el problema para revisión.",
        })

    issues.append({
        "ISSUE_TYPE": "SKU_FORMATO_INCONSISTENTE",
        "DESCRIPCION": "Los SKU pueden venir con formatos distintos entre hojas, por ejemplo con ceros a la izquierda.",
        "FILAS_AFECTADAS": int(len(paletizado_raw)),
        "TRATAMIENTO": "Se estandariza el SKU directamente en la columna SKU, eliminando ceros a la izquierda y guardándolo como texto.",
    })

    return pd.DataFrame(issues)


def export_outputs(fact_otr, dim_clientes, dim_productos, data_quality_log):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    fact_otr.to_csv(OUTPUT_DIR / "fact_otr.csv", index=False, encoding="utf-8-sig")
    dim_clientes.to_csv(OUTPUT_DIR / "dim_clientes.csv", index=False, encoding="utf-8-sig")
    dim_productos.to_csv(OUTPUT_DIR / "dim_productos.csv", index=False, encoding="utf-8-sig")
    data_quality_log.to_csv(OUTPUT_DIR / "data_quality_log.csv", index=False, encoding="utf-8-sig")

    with sqlite3.connect(DB_FILE) as conn:
        fact_otr.to_sql("fact_otr", conn, if_exists="replace", index=False)
        dim_clientes.to_sql("dim_clientes", conn, if_exists="replace", index=False)
        dim_productos.to_sql("dim_productos", conn, if_exists="replace", index=False)
        data_quality_log.to_sql("data_quality_log", conn, if_exists="replace", index=False)


def main():
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"No se encontró el archivo de entrada: {INPUT_FILE}")

    otr_raw, paletizado_raw, clientes_raw = read_input_file(INPUT_FILE)

    dim_clientes = clean_clientes(clientes_raw)
    dim_productos = clean_productos(paletizado_raw)
    fact_otr = clean_otr(otr_raw, dim_clientes)
    dim_clientes = add_missing_clients(fact_otr, dim_clientes)
    data_quality_log = build_data_quality_log(otr_raw, paletizado_raw, fact_otr, dim_clientes)

    export_outputs(fact_otr, dim_clientes, dim_productos, data_quality_log)

    print("Carga finalizada correctamente.")
    print(f"Pedidos cargados: {len(fact_otr)}")
    print(f"Clientes cargados: {len(dim_clientes)}")
    print(f"Productos cargados: {len(dim_productos)}")
    print(f"Problemas de calidad documentados: {len(data_quality_log)}")
    print(f"Archivos generados en: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
