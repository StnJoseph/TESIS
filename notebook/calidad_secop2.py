"""
Módulo de diagnóstico de calidad de datos para el SECOP II.
Tesis: Capa de Notarización Digital sobre SECOP II
Universidad de los Andes - Ingeniería de Sistemas y Computación
Joseph Steven Linares Gutierrez
2026

Uso:
    from calidad_secop2 import *

Dependencias: pandas, numpy, requests, matplotlib, seaborn, openpyxl, time, re, os
"""

import os
import re
import time

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import seaborn as sns
import requests
from pathlib import Path


# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTES GLOBALES
# ─────────────────────────────────────────────────────────────────────────────

COLUMNAS_FECHA_DEFAULT = [
    "fecha_de_firma",
    "fecha_de_inicio_del_contrato",
    "fecha_de_fin_del_contrato",
    "ultima_actualizacion",
    "fecha_de_notificaci_n_de_prorrogaci_n",
    "fecha_inicio_liquidacion",
    "fecha_fin_liquidacion",
]

COLUMNAS_MONTO_DEFAULT = [
    "valor_del_contrato",
    "valor_de_pago_adelantado",
    "valor_facturado",
    "valor_pendiente_de_pago",
    "valor_pagado",
    "valor_amortizado",
    "valor_pendiente_de",
    "valor_pendiente_de_ejecucion",
    "saldo_cdp",
    "saldo_vigencia",
]

COLUMNAS_CATEGORICAS_DEFAULT = [
    "modalidad_de_contratacion",
    "estado_contrato",
    "tipo_de_contrato",
    "orden",
    "sector",
    "rama",
    "entidad_centralizada",
    "condiciones_de_entrega",
    "origen_de_los_recursos",
    "destino_gasto",
    "es_grupo",
    "es_pyme",
    "habilita_pago_adelantado",
    "liquidaci_n",
]

COLUMNAS_CRITICAS_DEFAULT = [
    "id_contrato",
    "proceso_de_compra",
    "nit_entidad",
    "nombre_entidad",
    "documento_proveedor",
    "proveedor_adjudicado",
    "valor_del_contrato",
    "modalidad_de_contratacion",
    "estado_contrato",
    "tipo_de_contrato",
    "fecha_de_firma",
    "fecha_de_inicio_del_contrato",
    "fecha_de_fin_del_contrato",
    "urlproceso_clean",
    "departamento",
    "ciudad",
]

TOKENS_VACIO = [
    "No Definido", "No definido", "NO DEFINIDO",
    "N/A", "n/a", "NA", "na",
    "-", "--", "---",
    "Sin información", "Sin Información", "SIN INFORMACIÓN",
    "No aplica", "No Aplica", "NO APLICA",
    "nd", "ND", "Nd",
    "", " ",
]

CATALOGO_MODALIDADES = {
    "CONTRATACIÓN DIRECTA",
    "LICITACIÓN PÚBLICA",
    "SELECCIÓN ABREVIADA",
    "CONCURSO DE MÉRITOS",
    "MÍNIMA CUANTÍA",
    "CONTRATO INTERADMINISTRATIVO",
    "ASOCIACIÓN PÚBLICO PRIVADA",
    "RÉGIMEN ESPECIAL",
    "CONTRATACIÓN CON ENTIDADES PRIVADAS SIN ÁNIMO DE LUCRO",
    "CONTRATACIÓN CON FUENTE INTERNACIONAL",
    "CONTRATACIÓN ESTADO EMERGENCIA",
    "SUBASTA",
    "ACUERDO MARCO DE PRECIOS",
    "CONTRATACIÓN RÉGIMEN ESPECIAL"    
}

COLORES_SEVERIDAD = {
    "CRÍTICO": "#d62728",
    "ADVERTENCIA": "#ff7f0e",
    "OK": "#2ca02c",
}

plt.rcParams.update({
    "figure.dpi": 150,
    "savefig.dpi": 300,
    "font.family": "DejaVu Sans",
    "axes.spines.top": False,
    "axes.spines.right": False,
    "axes.grid": True,
    "grid.alpha": 0.3,
    "figure.facecolor": "white",
    "axes.facecolor": "white",
})


# ═════════════════════════════════════════════════════════════════════════════
# SECCIÓN 1 — PRE-PROCESAMIENTO
# ═════════════════════════════════════════════════════════════════════════════

def extraer_url(df: pd.DataFrame, col_url: str = "urlproceso") -> pd.DataFrame:
    """
    Extrae la URL del campo diccionario y crea columna 'urlproceso_clean'.

    Maneja los tres casos observados en la API: dict con clave 'url',
    string directo, o None/NaN.

    Params:
        df       : DataFrame con la columna de URL original.
        col_url  : Nombre de la columna que contiene el dict o string de URL.

    Returns:
        df sin col_url original, con nueva columna 'urlproceso_clean' (str | NaN).
    """
    df = df.copy()

    def extraer(x):
        if isinstance(x, dict):
            return x.get("url") or x.get("URL") or np.nan
        
        if isinstance(x, str) and x.strip():
            return x.strip()
        return np.nan

    df["urlproceso_clean"] = df[col_url].apply(extraer)

    if col_url != "urlproceso_clean":
        df = df.drop(columns=[col_url], errors="ignore")

    n_vacias = df["urlproceso_clean"].isna().sum()
    print(
        f"[extraer_url] URLs extraídas: {len(df) - n_vacias:,} | "
        f"Vacías/inválidas: {n_vacias:,} ({n_vacias / len(df) * 100:.1f}%)"
    )
    return df


def estandarizar_vacios(
    df: pd.DataFrame,
    columnas: list | None = None,
    tokens: list | None = None,
    ) -> pd.DataFrame:
    """
    Reemplaza tokens de valor ausente (string) por np.nan en columnas de tipo object.

    Params:
        df       : DataFrame de entrada.
        columnas : Columnas a procesar. Si None, aplica a todas las columnas object.
        tokens   : Lista de strings a reemplazar. Si None usa TOKENS_VACIO global.

    Returns:
        df con tokens de vacío reemplazados por np.nan.
    """
    df = df.copy()
    tokens = tokens or TOKENS_VACIO
    columnas = columnas or df.select_dtypes(include="object").columns.tolist()

    total_reemplazados = 0
    for col in columnas:
        if col not in df.columns:
            continue
        
        # strip de espacios + reemplazo
        df[col] = df[col].astype(str).str.strip()
        antes = df[col].isna().sum()
        df[col] = df[col].replace(tokens, np.nan)
        
        # reemplazar también strings que sean solo espacios
        df[col] = df[col].replace(r"^\s*$", np.nan, regex=True)
        despues = df[col].isna().sum()
        total_reemplazados += despues - antes

    print(f"[estandarizar_vacios] Tokens vacíos convertidos a NaN: {total_reemplazados:,}")
    return df


def normalizar_fechas(
    df: pd.DataFrame,
    columnas_fecha: list | None = None,
    ) -> pd.DataFrame:
    """
    Convierte columnas de fecha de string ISO a datetime64[ns].

    Acepta formatos como '2022-08-04T00:00:00.000' y '2022-08-04'.
    Los valores no parseables quedan como NaT y se reportan.

    Params:
        df            : DataFrame de entrada.
        columnas_fecha: Lista de columnas a convertir. Default: COLUMNAS_FECHA_DEFAULT.

    Returns:
        df con columnas de fecha convertidas a datetime64[ns].
    """
    df = df.copy()
    columnas_fecha = columnas_fecha or COLUMNAS_FECHA_DEFAULT
    columnas_fecha = [c for c in columnas_fecha if c in df.columns]

    resumen = []
    for col in columnas_fecha:
        n_antes_nulos = df[col].isna().sum()
        df[col] = pd.to_datetime(df[col], errors="coerce", utc=False)
        n_despues_nulos = df[col].isna().sum()
        n_nuevos_nat = n_despues_nulos - n_antes_nulos
        resumen.append({
            "columna": col,
            "nulos_previos": int(n_antes_nulos),
            "NaT_nuevos_por_conversion": int(max(n_nuevos_nat, 0)),
        })

    df_resumen = pd.DataFrame(resumen)
    print("[normalizar_fechas] Resumen de conversión:")
    print(df_resumen.to_string(index=False))
    return df


def normalizar_numericos(
    df: pd.DataFrame,
    columnas_num: list | None = None,
    ) -> pd.DataFrame:
    """
    Limpia y convierte columnas monetarias de string a float64.

    Elimina caracteres no numéricos (comas, $, espacios) antes de convertir.

    Params:
        df           : DataFrame de entrada.
        columnas_num : Lista de columnas monetarias. Default: COLUMNAS_MONTO_DEFAULT.

    Returns:
        df con columnas numéricas correctamente tipificadas.
    """
    df = df.copy()
    columnas_num = columnas_num or COLUMNAS_MONTO_DEFAULT
    columnas_num = [c for c in columnas_num if c in df.columns]

    resumen = []
    for col in columnas_num:
        n_antes_nulos = df[col].isna().sum()
        # limpiar caracteres no numéricos excepto punto decimal
        df[col] = (
            df[col]
            .astype(str)
            .str.strip()
            .str.replace(r"[^\d.]", "", regex=True)
            .replace("", np.nan)
        )
        df[col] = pd.to_numeric(df[col], errors="coerce")
        n_despues_nulos = df[col].isna().sum()
        resumen.append({
            "columna": col,
            "dtype_final": str(df[col].dtype),
            "NaN_nuevos": int(max(n_despues_nulos - n_antes_nulos, 0)),
        })

    df_resumen = pd.DataFrame(resumen)
    print("\n[normalizar_numericos] Resumen de conversión:")
    print(df_resumen.to_string(index=False))
    return df


def limpiar_strings(
    df: pd.DataFrame,
    columnas: list | None = None,
    ) -> pd.DataFrame:
    """
    Normaliza columnas categóricas: strip + uppercase.

    Elimina variantes de capitalización y espacios invisibles que producen
    falsos duplicados en análisis de consistencia.

    Params:
        df       : DataFrame de entrada.
        columnas : Columnas categóricas a limpiar. Default: COLUMNAS_CATEGORICAS_DEFAULT.

    Returns:
        df con strings normalizados.
    """
    df = df.copy()
    columnas = columnas or COLUMNAS_CATEGORICAS_DEFAULT
    columnas = [c for c in columnas if c in df.columns]

    for col in columnas:
        df[col] = df[col].astype(str).str.strip().str.upper()
        df[col] = df[col].replace({"NAN": np.nan, "NONE": np.nan})

    print(f"\n[limpiar_strings] Normalización aplicada a {len(columnas)} columnas.")
    return df


def preprocesar_todo(
    df: pd.DataFrame,
    col_url: str = "urlproceso",
    columnas_fecha: list | None = None,
    columnas_num: list | None = None,
    columnas_cat: list | None = None,
    tokens_vacio: list | None = None,
    ) -> pd.DataFrame:
    """
    Pipeline completo de pre-procesamiento en un solo llamado.

    Ejecuta en orden: extraer_url → estandarizar_vacios → normalizar_fechas
                      → normalizar_numericos → limpiar_strings.

    Params:
        df              : DataFrame crudo de la API SODA.
        col_url         : Columna de URL a limpiar.
        columnas_fecha  : Columnas de fecha (None = default).
        columnas_num    : Columnas monetarias (None = default).
        columnas_cat    : Columnas categóricas (None = default).
        tokens_vacio    : Tokens extra de vacío.

    Returns:
        df completamente pre-procesado.
    """
    print("=" * 60)
    print("PIPELINE DE PRE-PROCESAMIENTO")
    print("=" * 60)

    if col_url in df.columns:
        df = extraer_url(df, col_url)

    df = estandarizar_vacios(df, tokens_vacio)
    df = normalizar_fechas(df, columnas_fecha)
    df = normalizar_numericos(df, columnas_num)
    df = limpiar_strings(df, columnas_cat)

    print("\n[preprocesar_todo] Pre-procesamiento completado.")
    print('\nTipos de datos por categoría:')
    print('  datetime64:', df.select_dtypes('datetime64[ns]').columns.tolist())
    print('  int64:     ', df.select_dtypes('int64').columns.tolist())
    print('  object:    ', len(df.select_dtypes('object').columns), 'columnas')
    return df


# ═════════════════════════════════════════════════════════════════════════════
# SECCIÓN 2 — COMPLETITUD
# ═════════════════════════════════════════════════════════════════════════════

def severidad_completitud(pct_nulo: float, es_id: bool = False) -> str:
    """Clasifica la severidad según el porcentaje de nulos."""
    
    if es_id:
        umbral_critico = 5.0 
    else:
        umbral_critico = 20.0
        
    if es_id:
        umbral_advertencia = 1.0 
    else:
        umbral_advertencia = 5.0
    
    if pct_nulo >= umbral_critico:
        return "CRÍTICO"
    
    elif pct_nulo >= umbral_advertencia:
        return "ADVERTENCIA"
    
    return "OK"


def reporte_completitud(
    df: pd.DataFrame,
    columnas_criticas: list | None = None,
    ids_estrictos: list | None = None,
    ) -> pd.DataFrame:
    """
    Genera tabla con métricas de ausencia por columna crítica.

    Calcula nulos reales (NaN/NaT) sobre el DataFrame ya pre-procesado
    (estandarizar_vacios debe haberse aplicado previamente).

    Params:
        df               : DataFrame pre-procesado.
        columnas_criticas: Columnas a inspeccionar. Default: COLUMNAS_CRITICAS_DEFAULT.
        ids_estrictos    : Columnas que son IDs (umbral de alerta más estricto: 1%).

    Returns:
        DataFrame con: campo | n_total | n_nulos | pct_nulo | severidad
        Imprime resumen por severidad.
    """
    columnas_criticas = columnas_criticas or COLUMNAS_CRITICAS_DEFAULT
    columnas_criticas = [c for c in columnas_criticas if c in df.columns]
    ids_estrictos = ids_estrictos or ["id_contrato", "proceso_de_compra", "nit_entidad"] # Ids con umbral de alerta más estricto

    n_total = len(df)
    registros = []
    for col in columnas_criticas:
        n_nulos = int(df[col].isna().sum())
        pct = round(n_nulos / n_total * 100, 2)
        registros.append({
            "campo": col,
            "n_total": n_total,
            "n_nulos": n_nulos,
            "pct_nulo": pct,
            "severidad": severidad_completitud(pct, es_id=(col in ids_estrictos)),
        })

    resultado = pd.DataFrame(registros).sort_values("pct_nulo", ascending=False)

    # resumen ejecutivo
    counts = resultado["severidad"].value_counts()
    print("\n[reporte_completitud] Resumen de severidad:")
    for sev in ["CRÍTICO", "ADVERTENCIA", "OK"]:
        n = counts.get(sev, 0)
        print(f"  {sev:12s}: {n} campo(s)")

    return resultado


def top_entidades_incompletas(
    df: pd.DataFrame,
    columnas_criticas: list | None = None,
    top_n: int = 20,
    ) -> pd.DataFrame:
    """
    Identifica las entidades con mayor índice promedio de campos críticos vacíos.

    Params:
        df               : DataFrame pre-procesado.
        columnas_criticas: Columnas a evaluar. Default: COLUMNAS_CRITICAS_DEFAULT.
        top_n            : Número de entidades a retornar.

    Returns:
        DataFrame con: nombre_entidad | n_contratos | pct_incompleto_promedio
        Ordenado de mayor a menor incompletitud.
    """
    columnas_criticas = columnas_criticas or COLUMNAS_CRITICAS_DEFAULT
    columnas_criticas = [c for c in columnas_criticas if c in df.columns and c != "nombre_entidad"]

    if "nombre_entidad" not in df.columns:
        print("[top_entidades_incompletas] Columna 'nombre_entidad' no encontrada.")
        return pd.DataFrame()

    if not columnas_criticas:
        print("[top_entidades_incompletas] No hay columnas críticas válidas para evaluar.")
        return pd.DataFrame()

    # máscara de nulos por fila y columnas críticas
    df_nulos = df[columnas_criticas].isna().astype(int).copy()
    df_nulos["nombre_entidad"] = df["nombre_entidad"].values

    # recalcular correctamente
    agrupado = df_nulos.groupby("nombre_entidad", sort=False)
    resumen = []
    for entidad, grupo in agrupado:
        pct = grupo[columnas_criticas].mean().mean() * 100
        resumen.append({
            "nombre_entidad": entidad,
            "n_contratos": len(grupo),
            "pct_incompleto_promedio": round(pct, 2),
        })

    resultado = (
        pd.DataFrame(resumen)
        .sort_values("pct_incompleto_promedio", ascending=False)
        .head(top_n)
        .reset_index(drop=True)
    )

    print(f"[top_entidades_incompletas] Top {top_n} entidades con mayor incompletitud:")
    return resultado


# ═════════════════════════════════════════════════════════════════════════════
# SECCIÓN 3 — UNICIDAD
# ═════════════════════════════════════════════════════════════════════════════

def reporte_duplicados(
    df: pd.DataFrame,
    claves_naturales: list | None = None,
    ) -> dict:
    """
    Detecta y cuantifica duplicados exactos y por clave natural.

    Params:
        df               : DataFrame pre-procesado.
        claves_naturales : Columnas que forman la clave primaria esperada.
                           Default: ['id_contrato', 'proceso_de_compra'].

    Returns:
        Dict con:
          'duplicados_exactos'    : int
          'pct_duplicados_exactos': float
          'duplicados_por_clave'  : int
          'pct_duplicados_clave'  : float
          'df_duplicados_clave'   : DataFrame con registros duplicados por clave
          'claves_usadas'         : list
    """
    claves_naturales = claves_naturales or ["id_contrato", "proceso_de_compra"]
    claves_validas = [c for c in claves_naturales if c in df.columns]

    n = len(df)
    dup_exactos = int(df.duplicated(keep=False).sum())
    pct_exactos = round(dup_exactos / n * 100, 3)

    dup_clave = 0
    df_dup_clave = pd.DataFrame()
    if claves_validas:
        mask_dup = df.duplicated(subset=claves_validas, keep=False)
        dup_clave = int(mask_dup.sum())
        df_dup_clave = df[mask_dup].sort_values(claves_validas)

    pct_clave = round(dup_clave / n * 100, 3)

    alerta_exactos = "ALERTA" if pct_exactos > 0.1 else "OK"
    alerta_clave = "ALERTA" if pct_clave > 1.0 else "OK"

    print(f"\n[reporte_duplicados]")
    print(f"  Duplicados exactos      : {dup_exactos:,} ({pct_exactos}%) {alerta_exactos}")
    print(f"  Duplicados por clave    : {dup_clave:,} ({pct_clave}%) {alerta_clave}")
    print(f"  Claves usadas           : {claves_validas}")

    return {
        "duplicados_exactos": dup_exactos,
        "pct_duplicados_exactos": pct_exactos,
        "duplicados_por_clave": dup_clave,
        "pct_duplicados_clave": pct_clave,
        "df_duplicados_clave": df_dup_clave,
        "claves_usadas": claves_validas,
    }


def analizar_multiples_contratos_por_proceso(
    df: pd.DataFrame,
    umbral_sospechoso: int = 5,
    ) -> pd.DataFrame:
    """
    Agrupa por proceso_de_compra y cuenta contratos asociados.

    Distingue entre adiciones legítimas (mismo proceso, distinto id_contrato)
    y posibles duplicados reales (mismo proceso, mismo valor, mismas fechas).

    Params:
        df                 : DataFrame pre-procesado.
        umbral_sospechoso  : Número de contratos por proceso a partir del cual
                             se marca como sospechoso. Default: 5.

    Returns:
        DataFrame con: proceso_de_compra | n_contratos | n_ids_unicos |
                       n_estados_distintos | valor_total | sospechoso
    """
    if "proceso_de_compra" not in df.columns:
        print("[analizar_multiples_contratos] Columna 'proceso_de_compra' no encontrada.")
        return pd.DataFrame()

    agg_dict = {
        "n_contratos": ("id_contrato", "count"),
        "n_ids_unicos": ("id_contrato", "nunique"),
    }
    if "estado_contrato" in df.columns:
        agg_dict["n_estados_distintos"] = ("estado_contrato", "nunique")
    if "valor_del_contrato" in df.columns:
        agg_dict["valor_total"] = ("valor_del_contrato", "sum")

    resultado = (
        df.groupby("proceso_de_compra")
        .agg(**agg_dict)
        .reset_index()
        .sort_values("n_contratos", ascending=False)
    )

    resultado["sospechoso"] = resultado["n_contratos"] > umbral_sospechoso

    n_sosp = resultado["sospechoso"].sum()
    print(f"\n[analizar_multiples_contratos]")
    print(f"  Procesos únicos            : {len(resultado):,}")
    print(f"  Procesos sospechosos (>{umbral_sospechoso}): {n_sosp:,} ({n_sosp/len(resultado)*100:.2f}%)")
    print(f"  Proceso con más contratos  : {resultado.iloc[0]['proceso_de_compra']} "
          f"({resultado.iloc[0]['n_contratos']} contratos)")

    return resultado


# ═════════════════════════════════════════════════════════════════════════════
# SECCIÓN 4 — CONSISTENCIA
# ═════════════════════════════════════════════════════════════════════════════

def verificar_orden_temporal(
    df: pd.DataFrame,
    pares_fecha: list | None = None,
    ) -> pd.DataFrame:
    """
    Detecta registros donde la secuencia temporal entre pares de fechas es inválida.

    Params:
        df          : DataFrame con fechas ya convertidas a datetime.
        pares_fecha : Lista de tuplas (col_antes, col_despues).
                      Default: firma→inicio, inicio→fin, firma→fin.

    Returns:
        DataFrame con: par | n_ambas_presentes | n_violaciones | pct_violaciones | alerta
    """
    pares_fecha = pares_fecha or [
        ("fecha_de_firma", "fecha_de_inicio_del_contrato"),
        ("fecha_de_inicio_del_contrato", "fecha_de_fin_del_contrato"),
        ("fecha_de_firma", "fecha_de_fin_del_contrato"),
    ]

    registros = []
    for col_antes, col_despues in pares_fecha:
        
        if col_antes not in df.columns or col_despues not in df.columns:
            continue
        mask_ambas = df[col_antes].notna() & df[col_despues].notna()
        n_ambas = int(mask_ambas.sum())
        
        if n_ambas == 0:
            continue
        
        n_violaciones = int((df.loc[mask_ambas, col_antes] > df.loc[mask_ambas, col_despues]).sum())
        pct = round(n_violaciones / n_ambas * 100, 2)
        alerta = "CRÍTICO" if pct > 5 else ("ADVERTENCIA" if pct > 1 else "OK")
        registros.append({
            "par": f"{col_antes}  →  {col_despues}",
            "n_ambas_presentes": n_ambas,
            "n_violaciones": n_violaciones,
            "pct_violaciones": pct,
            "alerta": alerta,
        })

    resultado = pd.DataFrame(registros)
    print("\n[verificar_orden_temporal]")
    return resultado


def verificar_montos(
    df: pd.DataFrame,
    columnas_monto: list | None = None,
    percentil_outlier: float = 99.5,
    ) -> pd.DataFrame:
    """
    Valida integridad de columnas monetarias: negativos, ceros y outliers.

    Params:
        df                : DataFrame con columnas numéricas ya convertidas.
        columnas_monto    : Columnas a validar. Default: COLUMNAS_MONTO_DEFAULT.
        percentil_outlier : Percentil a partir del cual se considera outlier (default 99.5).

    Returns:
        DataFrame con: columna | n_no_nulos | n_negativos | n_ceros | n_outliers |
                       pct_problematicos | valor_max | valor_mediana
    """
    columnas_monto = columnas_monto or COLUMNAS_MONTO_DEFAULT
    columnas_monto = [c for c in columnas_monto if c in df.columns]

    registros = []
    for col in columnas_monto:
        serie = pd.to_numeric(df[col], errors="coerce").dropna()
        if len(serie) == 0:
            continue
        n_no_nulos = len(serie)
        n_neg = int((serie < 0).sum())
        n_cero = int((serie == 0).sum())
        umbral_outlier = serie.quantile(percentil_outlier / 100)
        n_outlier = int((serie > umbral_outlier).sum())
        
        n_problematicos = n_neg + n_cero  # outliers son informativos, no erróneos
        pct_prob = round((n_neg) / n_no_nulos * 100, 3)  # solo negativos son errores

        registros.append({
            "columna": col,
            "n_no_nulos": n_no_nulos,
            "n_negativos": n_neg,
            "n_ceros": n_cero,
            "n_outliers": n_outlier,
            "pct_negativos": pct_prob,
            "valor_mediana": round(float(serie.median()), 0),
            "valor_max": round(float(serie.max()), 0),
            f"umbral_p{percentil_outlier}": round(float(umbral_outlier), 0),
        })

    resultado = pd.DataFrame(registros)
    n_con_neg = (resultado["n_negativos"] > 0).sum()
    print(f"\n[verificar_montos] Columnas con valores negativos: {n_con_neg}")
    return resultado


def verificar_coherencia_modalidad_estado(
    df: pd.DataFrame,
    col_modalidad: str = "modalidad_de_contratacion",
    col_estado: str = "estado_contrato",
    combos_invalidas: set | None = None,
    ) -> pd.DataFrame:
    """
    Detecta combinaciones inválidas entre modalidad y estado del contrato.

    Params:
        df              : DataFrame con columnas normalizadas (limpiar_strings aplicado).
        col_modalidad   : Columna de modalidad de contratación.
        col_estado      : Columna de estado del contrato.
        combos_invalidas: Set de tuplas (modalidad, estado) explícitamente inválidas.
                          Si None, solo reporta todas las combinaciones y su frecuencia.

    Returns:
        DataFrame con: modalidad | estado | n_contratos | pct | es_invalida
    """
    if col_modalidad not in df.columns or col_estado not in df.columns:
        print(f"[verificar_coherencia] Columnas {col_modalidad} o {col_estado} no encontradas.")
        return pd.DataFrame()

    combos_invalidas = combos_invalidas or set()

    resultado = (
        df.groupby([col_modalidad, col_estado])
        .size()
        .reset_index(name="n_contratos")
        .sort_values("n_contratos", ascending=False)
    )
    n_total = resultado["n_contratos"].sum()
    resultado["pct"] = (resultado["n_contratos"] / n_total * 100).round(2)
    resultado["es_invalida"] = resultado.apply(
        lambda r: (r[col_modalidad], r[col_estado]) in combos_invalidas, axis=1
    )

    # Verificar modalidades fuera del catálogo
    resultado["modalidad_fuera_catalogo"] = ~resultado[col_modalidad].isin(CATALOGO_MODALIDADES)

    n_fuera = resultado.loc[resultado["modalidad_fuera_catalogo"], "n_contratos"].sum()
    pct_fuera = round(n_fuera / n_total * 100, 2)
    print(f"\n[verificar_coherencia_modalidad_estado]")
    print(f"  Combinaciones únicas      : {len(resultado)}")
    print(f"  Contratos fuera catálogo  : {n_fuera:,} ({pct_fuera}%)")

    return resultado


# ═════════════════════════════════════════════════════════════════════════════
# SECCIÓN 5 — VALIDEZ
# ═════════════════════════════════════════════════════════════════════════════

def verificar_formato_nit(
    df: pd.DataFrame,
    col_nit: str = "nit_entidad",
    ) -> pd.DataFrame:
    """
    Valida que los NITs tengan formato numérico de 8-12 dígitos.

    Params:
        df      : DataFrame pre-procesado.
        col_nit : Columna con el NIT.

    Returns:
        DataFrame con los registros de NIT inválido:
        nit_entidad | nombre_entidad | formato_invalido | motivo
    """
    if col_nit not in df.columns:
        print(f"[verificar_formato_nit] Columna '{col_nit}' no encontrada.")
        return pd.DataFrame()

    patron = re.compile(r"^\d{8,12}$")

    def _validar_nit(nit):
        if pd.isna(nit):
            return "nulo"
        nit_str = str(nit).strip()
        if not patron.match(nit_str):
            if not nit_str.isdigit():
                return "contiene_no_digitos"
            elif len(nit_str) < 8:
                return "muy_corto"
            elif len(nit_str) > 12:
                return "muy_largo"
        return "ok"

    df_temp = df[[col_nit] + (["nombre_entidad"] if "nombre_entidad" in df.columns else [])].copy()
    df_temp["motivo"] = df_temp[col_nit].apply(_validar_nit)
    df_temp["formato_invalido"] = df_temp["motivo"] != "ok"

    invalidos = df_temp[df_temp["formato_invalido"]].copy()
    pct = round(len(invalidos) / len(df) * 100, 3)
    alerta = "ALERTA" if pct > 0.5 else "OK"

    print(f"\n[verificar_formato_nit]")
    print(f"  NITs inválidos: {len(invalidos):,} ({pct}%) {alerta}")
    if len(invalidos) > 0:
        print(f"  Distribución de motivos:\n{invalidos['motivo'].value_counts().to_string()}")

    return invalidos.reset_index(drop=True)


def verificar_modalidades_catalogo(
    df: pd.DataFrame,
    col_modalidad: str = "modalidad_de_contratacion",
    catalogo: set | None = None,
    ) -> pd.DataFrame:
    """
    Verifica que las modalidades de contratación pertenezcan al catálogo oficial CCE.

    Params:
        df            : DataFrame con columnas normalizadas (limpiar_strings aplicado).
        col_modalidad : Columna de modalidad.
        catalogo      : Set de valores válidos. Default: CATALOGO_MODALIDADES global.

    Returns:
        DataFrame con: modalidad | n_contratos | pct | en_catalogo
    """
    catalogo = catalogo or CATALOGO_MODALIDADES

    if col_modalidad not in df.columns:
        print(f"[verificar_modalidades_catalogo] Columna '{col_modalidad}' no encontrada.")
        return pd.DataFrame()

    resultado = (
        df[col_modalidad]
        .value_counts()
        .reset_index()
        .rename(columns={col_modalidad: "modalidad", "count": "n_contratos"})
    )
    n_total = resultado["n_contratos"].sum()
    resultado["pct"] = (resultado["n_contratos"] / n_total * 100).round(2)
    resultado["en_catalogo"] = resultado["modalidad"].isin(catalogo)

    n_fuera = resultado.loc[~resultado["en_catalogo"], "n_contratos"].sum()
    pct_fuera = round(n_fuera / n_total * 100, 2)

    print(f"\n[verificar_modalidades_catalogo]")
    print(f"  Modalidades únicas        : {len(resultado)}")
    print(f"  Contratos fuera catálogo  : {n_fuera:,} ({pct_fuera}%)")
    return resultado


def verificar_rangos_temporales(
    df: pd.DataFrame,
    columnas_fecha: list | None = None,
    anio_min: int = 2014,
    anio_max: int | None = None,
    ) -> pd.DataFrame:
    """
    Detecta fechas fuera del rango operacional del SECOP II [2014, año_actual + 10].

    Params:
        df            : DataFrame con fechas convertidas a datetime.
        columnas_fecha: Lista de columnas de fecha a validar.
        anio_min      : Año mínimo válido (SECOP II inició en 2014).
        anio_max      : Año máximo válido. Default: año actual + 10.

    Returns:
        DataFrame con: columna | n_no_nulos | n_anteriores_min | n_posteriores_max |
                       pct_invalidas | alerta
    """
    columnas_fecha = columnas_fecha or COLUMNAS_FECHA_DEFAULT
    columnas_fecha = [c for c in columnas_fecha if c in df.columns]
    anio_max = anio_max or (pd.Timestamp.now().year + 10)

    registros = []
    for col in columnas_fecha:
        if not pd.api.types.is_datetime64_any_dtype(df[col]):
            continue
        serie = df[col].dropna()
        if len(serie) == 0:
            continue
        anios = serie.dt.year
        n_antes = int((anios < anio_min).sum())
        n_despues = int((anios > anio_max).sum())
        n_invalidas = n_antes + n_despues
        pct = round(n_invalidas / len(serie) * 100, 3)
        alerta = "ALERTA" if pct > 0.1 else "OK"
        registros.append({
            "columna": col,
            "n_no_nulos": len(serie),
            "n_anteriores_min": n_antes,
            "n_posteriores_max": n_despues,
            "pct_invalidas": pct,
            "alerta": alerta,
        })

    resultado = pd.DataFrame(registros)
    print(f"\n[verificar_rangos_temporales] Rango válido: [{anio_min}, {anio_max}]")
    return resultado


# ═════════════════════════════════════════════════════════════════════════════
# SECCIÓN 6 — SONDEO DE URLs
# ═════════════════════════════════════════════════════════════════════════════

def sondear_urls(
    df: pd.DataFrame,
    col_url: str = "urlproceso_clean",
    n_muestra: int = 100000,
    timeout: int = 5,
    estratificar_por: str | None = "estado_contrato",
    pausa_segundos: float = 0.3,
    semilla: int = 42,
    ) -> pd.DataFrame:
    """
    Verifica accesibilidad de una muestra de URLs mediante requests HEAD.

    NO descarga el contenido del documento; solo verifica status code y tipo MIME.

    Params:
        df               : DataFrame con columna de URL limpia.
        col_url          : Nombre de columna URL.
        n_muestra        : Tamaño de muestra total.
        timeout          : Segundos de timeout por request.
        estratificar_por : Columna para estratificar la muestra (e.g. estado_contrato).
                           Si None, muestra aleatoria simple.
        pausa_segundos   : Pausa entre requests para no saturar el servidor.
        semilla          : Semilla aleatoria para reproducibilidad.

    Returns:
        DataFrame con: url | status_code | content_type | accesible | es_pdf | tiempo_ms | error
    """
    if col_url not in df.columns:
        print(f"[sondear_urls] Columna '{col_url}' no encontrada.")
        return pd.DataFrame()

    df_validas = df[df[col_url].notna()].copy()
    if len(df_validas) == 0:
        print("[sondear_urls] No hay URLs válidas para sondear.")
        return pd.DataFrame()

    # muestra estratificada o aleatoria
    n_muestra = min(n_muestra, len(df_validas))
    if estratificar_por and estratificar_por in df_validas.columns:
        muestra = (
            df_validas
            .groupby(estratificar_por, group_keys=False)
            .apply(lambda g: g.sample(
                min(len(g), max(1, int(n_muestra * len(g) / len(df_validas)))),
                random_state=semilla
            ))
            .head(n_muestra)
        )
    else:
        muestra = df_validas.sample(n_muestra, random_state=semilla)

    urls = muestra[col_url].tolist()
    print(f"[sondear_urls] Sondeando {len(urls)} URLs (HEAD request)...")

    registros = []
    headers = {"User-Agent": "SECOP-QualityCheck/1.0 (Tesis Uniandes)"}

    for i, url in enumerate(urls):
        if (i + 1) % 20 == 0:
            print(f"  Progreso: {i+1}/{len(urls)}")

        t_inicio = time.time()
        status_code = -1
        content_type = ""
        error_msg = ""

        try:
            resp = requests.head(
                url, timeout=timeout, allow_redirects=True, headers=headers
            )
            status_code = resp.status_code
            content_type = resp.headers.get("Content-Type", "").lower()
        except requests.exceptions.Timeout:
            error_msg = "timeout"
        except requests.exceptions.ConnectionError:
            error_msg = "connection_error"
        except requests.exceptions.TooManyRedirects:
            error_msg = "too_many_redirects"
        except Exception as e:
            error_msg = str(e)[:50]

        tiempo_ms = int((time.time() - t_inicio) * 1000)
        accesible = 200 <= status_code < 400
        es_pdf = "application/pdf" in content_type

        registros.append({
            "url": url,
            "status_code": status_code,
            "content_type": content_type,
            "accesible": accesible,
            "es_pdf": es_pdf,
            "tiempo_ms": tiempo_ms,
            "error": error_msg,
        })
        time.sleep(pausa_segundos)

    resultado = pd.DataFrame(registros)
    print(f"\n[sondear_urls] Resultados:")
    print(f"  Accesibles: {resultado['accesible'].sum()} ({resultado['accesible'].mean()*100:.1f}%)")
    print(f"  Son PDF             : {resultado['es_pdf'].sum()} ({resultado['es_pdf'].mean()*100:.1f}%)")
    print(f"  Errores de red      : {(resultado['error'] != '').sum()}")
    return resultado


def resumir_sondeo_urls(df_sondeo: pd.DataFrame) -> dict:
    """
    Calcula métricas de accesibilidad a partir del DataFrame de sondeo.

    Params:
        df_sondeo: DataFrame resultado de sondear_urls().

    Returns:
        Dict con métricas clave para el informe de tesis.
    """
    if df_sondeo.empty:
        return {}

    n = len(df_sondeo)
    resumen = {
        "n_urls_sondeadas": n,
        "pct_accesibles": round(df_sondeo["accesible"].mean() * 100, 1),
        "pct_pdf": round(df_sondeo["es_pdf"].mean() * 100, 1),
        "pct_404": round((df_sondeo["status_code"] == 404).mean() * 100, 1),
        "pct_timeout": round((df_sondeo["error"] == "timeout").mean() * 100, 1),
        "pct_error_red": round((df_sondeo["error"] != "").mean() * 100, 1),
        "tiempo_medio_ms": int(df_sondeo["tiempo_ms"].mean()),
        "distribucion_status_codes": df_sondeo["status_code"].value_counts().to_dict(),
        "distribucion_content_type": df_sondeo["content_type"].value_counts().head(10).to_dict(),
    }

    print("\n[resumir_sondeo_urls] Métricas de accesibilidad:")
    for k, v in resumen.items():
        if not isinstance(v, dict):
            print(f"  {k:35s}: {v}")
    return resumen


# ═════════════════════════════════════════════════════════════════════════════
# SECCIÓN 7 — VISUALIZACIONES
# ═════════════════════════════════════════════════════════════════════════════

def plot_barras_nulos(
    df_completitud: pd.DataFrame,
    titulo: str = "Completitud por Campo Crítico",
    guardar_en: str | None = None,
    ) -> None:
    """
    Gráfico de barras horizontales con % de nulos por campo, coloreado por severidad.

    Params:
        df_completitud: DataFrame resultado de reporte_completitud().
        titulo        : Título del gráfico.
        guardar_en    : Ruta para guardar la figura (PNG). Si None, solo muestra.
    """
    df = df_completitud.sort_values("pct_nulo", ascending=True).copy()
    colores = df["severidad"].map(COLORES_SEVERIDAD).fillna("#1f77b4")

    fig, ax = plt.subplots(figsize=(10, max(6, len(df) * 0.4)))
    bars = ax.barh(df["campo"], df["pct_nulo"], color=colores, edgecolor="white", height=0.7)

    # líneas de umbral
    ax.axvline(5, color="#ff7f0e", linestyle="--", linewidth=1, alpha=0.7, label="Umbral Advertencia (5%)")
    ax.axvline(20, color="#d62728", linestyle="--", linewidth=1, alpha=0.7, label="Umbral Crítico (20%)")

    # etiquetas de valor
    for bar, val in zip(bars, df["pct_nulo"]):
        if val > 0:
            ax.text(val + 0.3, bar.get_y() + bar.get_height() / 2,
                    f"{val:.1f}%", va="center", fontsize=8)

    # leyenda de severidad
    parches = [
        mpatches.Patch(color=v, label=k) for k, v in COLORES_SEVERIDAD.items()
    ]
    ax.legend(handles=parches + [
        plt.Line2D([0], [0], color="#ff7f0e", linestyle="--", label="Advertencia (5%)"),
        plt.Line2D([0], [0], color="#d62728", linestyle="--", label="Crítico (20%)"),
    ], loc="lower right", fontsize=9)

    ax.set_xlabel("Porcentaje de nulos (%)", fontsize=11)
    ax.set_title(titulo, fontsize=13, fontweight="bold", pad=15)
    ax.set_xlim(0, min(105, df["pct_nulo"].max() * 1.15 + 5))

    plt.tight_layout()
    if guardar_en:
        plt.savefig(guardar_en, dpi=300, bbox_inches="tight")
        print(f"[plot_barras_nulos] Figura guardada en: {guardar_en}")
    plt.show()


def plot_heatmap_nulos_por_entidad(
    df: pd.DataFrame,
    columnas_criticas: list | None = None,
    top_n: int = 30,
    titulo: str = "Heatmap de Incompletitud por Entidad",
    guardar_en: str | None = None,
    ) -> None:
    """
    Heatmap de % de nulos para las top_n entidades con mayor incompletitud.

    Filas = entidades, columnas = campos críticos. Útil para detectar
    patrones de sub-reporte por entidad o sector.

    Params:
        df               : DataFrame pre-procesado.
        columnas_criticas: Columnas a evaluar.
        top_n            : Número de entidades a mostrar.
        titulo           : Título del gráfico.
        guardar_en       : Ruta para guardar la figura.
    """
    columnas_criticas = columnas_criticas or COLUMNAS_CRITICAS_DEFAULT
    columnas_criticas = [c for c in columnas_criticas if c in df.columns and c != "nombre_entidad"]

    # calcular % nulos por entidad
    df_nulos = df.groupby("nombre_entidad")[columnas_criticas].apply(
        lambda g: g.isna().mean() * 100
    ).reset_index()

    # seleccionar top_n entidades con mayor incompletitud promedio
    df_nulos["promedio"] = df_nulos[columnas_criticas].mean(axis=1)
    df_nulos = df_nulos.sort_values("promedio", ascending=False).head(top_n)
    df_nulos = df_nulos.set_index("nombre_entidad")[columnas_criticas]

    # acortar nombres de entidades largos
    df_nulos.index = [name[:35] + "…" if len(name) > 35 else name for name in df_nulos.index]

    fig, ax = plt.subplots(figsize=(max(12, len(columnas_criticas) * 1.2), max(8, top_n * 0.35)))

    sns.heatmap(
        df_nulos,
        ax=ax,
        cmap="RdYlGn_r",
        vmin=0, vmax=100,
        linewidths=0.3,
        linecolor="white",
        cbar_kws={"label": "% Nulos", "shrink": 0.6},
        annot=False,
    )

    ax.set_title(titulo, fontsize=13, fontweight="bold", pad=15)
    ax.set_xlabel("Campo", fontsize=10)
    ax.set_ylabel("Entidad", fontsize=10)
    plt.xticks(rotation=45, ha="right", fontsize=8)
    plt.yticks(fontsize=8)
    plt.tight_layout()

    if guardar_en:
        plt.savefig(guardar_en, dpi=300, bbox_inches="tight")
        print(f"[plot_heatmap_nulos_por_entidad] Figura guardada en: {guardar_en}")
    plt.show()


def plot_serie_temporal_contratos(
    df: pd.DataFrame,
    col_fecha: str = "fecha_de_firma",
    freq: str = "ME",
    titulo: str = "Volumen de Contratos por Período",
    guardar_en: str | None = None,
    ) -> None:
    """
    Serie temporal del volumen de contratos firmados por período.

    Permite detectar picos anómalos, gaps de datos o patrones estacionales.

    Params:
        df        : DataFrame con fechas convertidas.
        col_fecha : Columna de fecha a usar como eje temporal.
        freq      : Frecuencia de agrupación: 'ME' (mensual), 'QE' (trimestral), 'YE' (anual).
        titulo    : Título del gráfico.
        guardar_en: Ruta para guardar la figura.
    """
    if col_fecha not in df.columns or not pd.api.types.is_datetime64_any_dtype(df[col_fecha]):
        print(f"[plot_serie_temporal] Columna '{col_fecha}' no disponible o no es datetime.")
        return

    serie = (
        df.set_index(col_fecha)
        .resample(freq)
        .size()
        .rename("n_contratos")
    )
    serie = serie[serie.index.year >= 2014]

    fig, ax = plt.subplots(figsize=(14, 5))
    ax.plot(serie.index, serie.values, color="#1f77b4", linewidth=1.5, alpha=0.8)
    ax.fill_between(serie.index, serie.values, alpha=0.15, color="#1f77b4")

    # marcar el máximo
    idx_max = serie.idxmax()
    ax.annotate(
        f"Máx: {serie.max():,}\n{idx_max.strftime('%b %Y')}",
        xy=(idx_max, serie.max()),
        xytext=(20, 10),
        textcoords="offset points",
        fontsize=8,
        color="#d62728",
        arrowprops=dict(arrowstyle="->", color="#d62728", lw=1.2),
    )

    etiquetas_freq = {"ME": "mes", "QE": "trimestre", "YE": "año"}
    ax.set_xlabel(f"Período ({etiquetas_freq.get(freq, freq)})", fontsize=11)
    ax.set_ylabel("Número de contratos", fontsize=11)
    ax.set_title(titulo, fontsize=13, fontweight="bold", pad=15)
    plt.xticks(rotation=30, ha="right", fontsize=9)
    plt.tight_layout()

    if guardar_en:
        plt.savefig(guardar_en, dpi=300, bbox_inches="tight")
        print(f"[plot_serie_temporal_contratos] Figura guardada en: {guardar_en}")
    plt.show()


def plot_distribucion_valores(
    df: pd.DataFrame,
    col: str = "valor_del_contrato",
    log_scale: bool = True,
    titulo: str = "Distribución de Valores Contractuales (COP)",
    guardar_en: str | None = None,
    ) -> None:
    """
    Histograma de valores contractuales con marcadores de percentiles clave.

    Params:
        df        : DataFrame con columna numérica ya convertida.
        col       : Columna de monto a visualizar.
        log_scale : Aplicar escala logarítmica en eje X (recomendado para COP).
        titulo    : Título del gráfico.
        guardar_en: Ruta para guardar la figura.
    """
    if col not in df.columns:
        print(f"[plot_distribucion_valores] Columna '{col}' no encontrada.")
        return

    serie = pd.to_numeric(df[col], errors="coerce").dropna()
    serie = serie[serie > 0]  # excluir ceros y negativos del histograma

    fig, ax = plt.subplots(figsize=(11, 5))

    if log_scale:
        datos_plot = np.log10(serie.clip(lower=1))
        ax.hist(datos_plot, bins=60, color="#1f77b4", edgecolor="white", alpha=0.8)
        ax.set_xlabel("log\u2081\u2080(Valor del contrato en COP)", fontsize=10)
        percentiles = [50, 75, 95, 99]
        for p in percentiles:
            val = np.percentile(datos_plot, p)
            ax.axvline(val, linestyle="--", alpha=0.7, linewidth=1,
                       label=f"p{p}: ${10**val:,.0f}")
    else:
        ax.hist(serie, bins=60, color="#1f77b4", edgecolor="white", alpha=0.8)
        ax.set_xlabel("Valor del contrato (COP)", fontsize=10)

    ax.set_ylabel("Frecuencia", fontsize=10)
    ax.legend(fontsize=8)

    fig.suptitle(titulo, fontsize=13, fontweight="bold")
    plt.tight_layout()

    if guardar_en:
        plt.savefig(guardar_en, dpi=300, bbox_inches="tight")
        print(f"[plot_distribucion_valores] Figura guardada en: {guardar_en}")
    plt.show()

    if guardar_en:
        plt.savefig(guardar_en, dpi=300, bbox_inches="tight")
        print(f"[plot_distribucion_valores] Figura guardada en: {guardar_en}")
    plt.show()


def plot_top_modalidades(
    df: pd.DataFrame,
    col: str = "modalidad_de_contratacion",
    top_n: int = 10,
    umbral_alerta: float = 65.0,
    titulo: str = "Distribución de Modalidades de Contratación",
    guardar_en: str | None = None,
    ) -> None:
    """
    Gráfico de barras de las modalidades más frecuentes con porcentaje acumulado.

    Resalta con color diferente si 'CONTRATACIÓN DIRECTA' supera el umbral
    de referencia citado en la propuesta de tesis (65%).

    Params:
        df           : DataFrame con columna de modalidad normalizada.
        col          : Columna de modalidad.
        top_n        : Número de modalidades a mostrar.
        umbral_alerta: Porcentaje a partir del cual se resalta CONTRATACIÓN DIRECTA.
        titulo       : Título del gráfico.
        guardar_en   : Ruta para guardar la figura.
    """
    if col not in df.columns:
        print(f"[plot_top_modalidades] Columna '{col}' no encontrada.")
        return

    conteos = df[col].value_counts().head(top_n)
    pct = (conteos / conteos.sum() * 100).round(1)

    colores_barras = []
    for modalidad in conteos.index:
        if "DIRECTA" in str(modalidad).upper() and pct[modalidad] >= umbral_alerta:
            colores_barras.append("#d62728")
        elif not str(modalidad) in CATALOGO_MODALIDADES:
            colores_barras.append("#9467bd")
        else:
            colores_barras.append("#1f77b4")

    fig, ax = plt.subplots(figsize=(12, 6))
    bars = ax.bar(range(len(conteos)), conteos.values, color=colores_barras,
                  edgecolor="white", width=0.7)

    # etiquetas con porcentaje
    for bar, p in zip(bars, pct.values):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + conteos.max() * 0.01,
                f"{p:.1f}%", ha="center", va="bottom", fontsize=9, fontweight="bold")

    ax.set_xticks(range(len(conteos)))
    ax.set_xticklabels(
        [m[:30] + "…" if len(m) > 30 else m for m in conteos.index],
        rotation=35, ha="right", fontsize=9
    )
    ax.set_ylabel("Número de contratos", fontsize=11)
    ax.set_title(titulo, fontsize=13, fontweight="bold", pad=15)

    # línea de umbral de alerta (65% del benchmark de la tesis)
    total = conteos.sum()
    umbral_abs = total * umbral_alerta / 100
    ax.axhline(umbral_abs, color="#d62728", linestyle="--", linewidth=1.2,
               label=f"Umbral alerta: {umbral_alerta}%", alpha=0.8)
    ax.legend(fontsize=9)

    plt.tight_layout()

    if guardar_en:
        plt.savefig(guardar_en, dpi=300, bbox_inches="tight")
        print(f"[plot_top_modalidades] Figura guardada en: {guardar_en}")
    plt.show()


def plot_urls_status(
    df_sondeo: pd.DataFrame,
    titulo: str = "Accesibilidad de URLs de Procesos",
    guardar_en: str | None = None,
    ) -> None:
    """
    Gráfico combinado: pie de accesibilidad + barras de status codes HTTP.

    Params:
        df_sondeo : DataFrame resultado de sondear_urls().
        titulo    : Título del gráfico.
        guardar_en: Ruta para guardar la figura.
    """
    if df_sondeo.empty:
        print("[plot_urls_status] DataFrame de sondeo vacío.")
        return

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

    # pie de accesibilidad
    n_accesibles = df_sondeo["accesible"].sum()
    n_inaccesibles = len(df_sondeo) - n_accesibles
    ax1.pie(
        [n_accesibles, n_inaccesibles],
        labels=[f"Accesibles\n({n_accesibles})", f"Inaccesibles\n({n_inaccesibles})"],
        colors=["#2ca02c", "#d62728"],
        autopct="%1.1f%%",
        startangle=90,
        wedgeprops=dict(edgecolor="white", linewidth=2),
    )
    ax1.set_title("Accesibilidad General", fontsize=11, fontweight="bold")

    # barras de distribución de status codes
    status_counts = (
        df_sondeo["status_code"]
        .replace(-1, "Error red")
        .astype(str)
        .value_counts()
        .sort_index()
    )
    colores_status = {
        "200": "#2ca02c", "301": "#17becf", "302": "#17becf",
        "404": "#d62728", "403": "#ff7f0e", "500": "#9467bd",
        "Error red": "#7f7f7f",
    }
    c = [colores_status.get(str(s), "#1f77b4") for s in status_counts.index]
    ax2.bar(status_counts.index.astype(str), status_counts.values, color=c, edgecolor="white")
    for i, (idx, val) in enumerate(status_counts.items()):
        ax2.text(i, val + 0.3, str(val), ha="center", fontsize=10, fontweight="bold")
    ax2.set_xlabel("Status Code HTTP", fontsize=10)
    ax2.set_ylabel("Número de URLs", fontsize=10)
    ax2.set_title("Distribución de Status Codes", fontsize=11, fontweight="bold")

    fig.suptitle(titulo, fontsize=13, fontweight="bold", y=1.02)
    plt.tight_layout()

    if guardar_en:
        plt.savefig(guardar_en, dpi=300, bbox_inches="tight")
        print(f"[plot_urls_status] Figura guardada en: {guardar_en}")
    plt.show()


# ═════════════════════════════════════════════════════════════════════════════
# SECCIÓN 8 — EXPORTACIÓN DE ARTEFACTOS
# ═════════════════════════════════════════════════════════════════════════════

def exportar_reporte_calidad(
    resultados: dict,
    ruta_salida: str = ".",
    nombre_excel: str = "reporte_calidad_secop2.xlsx",
    exportar_csv: bool = True,
    ) -> None:
    """
    Exporta todos los DataFrames de resultado a un Excel multi-hoja y CSVs individuales.

    Params:
        resultados   : Dict con nombre_hoja -> DataFrame.
                       Ej: {'completitud': df_comp, 'unicidad': df_dup, ...}
        ruta_salida  : Directorio de destino.
        nombre_excel : Nombre del archivo Excel.
        exportar_csv : Si True, también exporta cada hoja como CSV individual.
    """
    Path(ruta_salida).mkdir(parents=True, exist_ok=True)
    ruta_excel = os.path.join(ruta_salida, nombre_excel)

    with pd.ExcelWriter(ruta_excel, engine="openpyxl") as writer:
        for nombre_hoja, df_resultado in resultados.items():
            if not isinstance(df_resultado, pd.DataFrame) or df_resultado.empty:
                continue
            # limpiar nombre de hoja (máx 31 chars, sin caracteres especiales)
            hoja = re.sub(r"[\\/*?:\[\]]", "_", nombre_hoja)[:31]
            df_resultado.to_excel(writer, sheet_name=hoja, index=False)

            if exportar_csv:
                ruta_csv = os.path.join(ruta_salida, f"{nombre_hoja}.csv")
                df_resultado.to_csv(ruta_csv, index=False, encoding="utf-8-sig")
                print(f"  [CSV] {ruta_csv}")

    print(f"\n[exportar_reporte_calidad] Excel guardado en: {ruta_excel}")
    print(f"  Hojas exportadas: {list(resultados.keys())}")


def generar_resumen_ejecutivo(
    df_completitud: pd.DataFrame,
    resultado_duplicados: dict,
    df_orden_temporal: pd.DataFrame,
    df_montos: pd.DataFrame,
    df_modalidades: pd.DataFrame,
    resumen_urls: dict,
    ) -> pd.DataFrame:
    """
    Consolida la métrica principal de cada dimensión en una tabla de una sola vista.

    Params:
        df_completitud      : Resultado de reporte_completitud().
        resultado_duplicados: Resultado de reporte_duplicados().
        df_orden_temporal   : Resultado de verificar_orden_temporal().
        df_montos           : Resultado de verificar_montos().
        df_modalidades      : Resultado de verificar_modalidades_catalogo().
        resumen_urls        : Resultado de resumir_sondeo_urls().

    Returns:
        DataFrame con: dimensión | métrica_principal | valor | alerta
    """
    registros = []

    # Completitud
    if not df_completitud.empty:
        pct_criticos = (df_completitud["severidad"] == "CRÍTICO").mean() * 100
        registros.append({
            "dimension": "Completitud",
            "metrica": "% campos críticos con severidad CRÍTICO",
            "valor": round(pct_criticos, 1),
            "alerta": "No" if pct_criticos > 20 else "Sí",
        })

    # Unicidad
    if resultado_duplicados:
        registros.append({
            "dimension": "Unicidad",
            "metrica": "% duplicados por clave natural",
            "valor": resultado_duplicados.get("pct_duplicados_clave", 0),
            "alerta": "No" if resultado_duplicados.get("pct_duplicados_clave", 0) > 1 else "Sí",
        })

    # Consistencia temporal
    if not df_orden_temporal.empty:
        max_violacion = df_orden_temporal["pct_violaciones"].max()
        registros.append({
            "dimension": "Consistencia (temporal)",
            "metrica": "% max. violaciones de orden temporal",
            "valor": round(max_violacion, 2),
            "alerta": "No" if max_violacion > 5 else "Sí",
        })

    # Consistencia montos
    if not df_montos.empty:
        total_neg = df_montos["n_negativos"].sum()
        registros.append({
            "dimension": "Consistencia (montos)",
            "metrica": "n total valores negativos en montos",
            "valor": int(total_neg),
            "alerta": "No" if total_neg > 0 else "Sí",
        })

    # Validez
    if not df_modalidades.empty:
        pct_fuera = df_modalidades.loc[
            ~df_modalidades["en_catalogo"], "n_contratos"
        ].sum() / df_modalidades["n_contratos"].sum() * 100
        registros.append({
            "dimension": "Validez (modalidades)",
            "metrica": "% contratos con modalidad fuera de catálogo",
            "valor": round(pct_fuera, 2),
            "alerta": "No" if pct_fuera > 1 else "Sí",
        })

    # URLs
    if resumen_urls:
        pct_acc = resumen_urls.get("pct_accesibles", 0)
        registros.append({
            "dimension": "Validez (URLs)",
            "metrica": "% URLs accesibles (muestra)",
            "valor": pct_acc,
            "alerta": "No" if pct_acc < 80 else "Sí",
        })

    resultado = pd.DataFrame(registros)
    print("\n[generar_resumen_ejecutivo] Resumen consolidado:")
    return resultado


# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE COMPLETO DE DIAGNÓSTICO
# ─────────────────────────────────────────────────────────────────────────────

def diagnostico_completo(
    df: pd.DataFrame,
    ruta_salida: str = ".",
    sondear_urls_flag: bool = True,
    n_urls_muestra: int = 100,
    ) -> dict:
    """
    Ejecuta el pipeline completo de diagnóstico de calidad sobre el DataFrame.

    Encadena todas las funciones del módulo y exporta artefactos.

    Params:
        df                : DataFrame crudo de la API SODA (sin pre-procesar).
        ruta_salida       : Directorio para guardar reportes y figuras.
        sondear_urls_flag : Si True, ejecuta el sondeo de URLs (requiere conexión).
        n_urls_muestra    : Tamaño de muestra para el sondeo de URLs.

    Returns:
        Dict con todos los resultados intermedios para análisis posterior.
    """
    Path(ruta_salida).mkdir(parents=True, exist_ok=True)
    resultados = {}

    print("\n" + "═" * 60)
    print("DIAGNÓSTICO COMPLETO DE CALIDAD — SECOP II")
    print("═" * 60)

    # 1. Pre-procesamiento
    df_clean = preprocesar_todo(df)
    resultados["df_clean"] = df_clean

    # 2. Completitud
    print("\n" + "─" * 40)
    print("COMPLETITUD")
    print("─" * 40)
    df_comp = reporte_completitud(df_clean)
    df_top_ent = top_entidades_incompletas(df_clean)
    resultados["completitud"] = df_comp
    resultados["top_entidades_incompletas"] = df_top_ent
    plot_barras_nulos(df_comp, guardar_en=os.path.join(ruta_salida, "fig_completitud_nulos.png"))

    # 3. Unicidad
    print("\n" + "─" * 40)
    print("UNICIDAD")
    print("─" * 40)
    dup = reporte_duplicados(df_clean)
    multi = analizar_multiples_contratos_por_proceso(df_clean)
    resultados["duplicados"] = dup
    resultados["multiples_contratos"] = multi

    # 4. Consistencia
    print("\n" + "─" * 40)
    print("CONSISTENCIA")
    print("─" * 40)
    df_temp = verificar_orden_temporal(df_clean)
    df_montos = verificar_montos(df_clean)
    df_modal_estado = verificar_coherencia_modalidad_estado(df_clean)
    resultados["orden_temporal"] = df_temp
    resultados["montos"] = df_montos
    resultados["modalidad_estado"] = df_modal_estado
    plot_serie_temporal_contratos(df_clean, guardar_en=os.path.join(ruta_salida, "fig_serie_temporal.png"))
    plot_distribucion_valores(df_clean, guardar_en=os.path.join(ruta_salida, "fig_distribucion_valores.png"))

    # 5. Validez
    print("\n" + "─" * 40)
    print("VALIDEZ")
    print("─" * 40)
    df_nit = verificar_formato_nit(df_clean)
    df_modal = verificar_modalidades_catalogo(df_clean)
    df_rangos = verificar_rangos_temporales(df_clean)
    resultados["nit_invalidos"] = df_nit
    resultados["modalidades"] = df_modal
    resultados["rangos_temporales"] = df_rangos
    plot_top_modalidades(df_clean, guardar_en=os.path.join(ruta_salida, "fig_modalidades.png"))

    # 6. URLs
    resumen_urls = {}
    df_sondeo = pd.DataFrame()
    if sondear_urls_flag:
        print("\n" + "─" * 40)
        print("SONDEO DE URLs")
        print("─" * 40)
        df_sondeo = sondear_urls(df_clean, n_muestra=n_urls_muestra)
        resumen_urls = resumir_sondeo_urls(df_sondeo)
        if not df_sondeo.empty:
            plot_urls_status(df_sondeo, guardar_en=os.path.join(ruta_salida, "fig_urls_status.png"))
        resultados["sondeo_urls"] = df_sondeo
        resultados["resumen_urls"] = resumen_urls

    # 7. Resumen ejecutivo
    print("\n" + "─" * 40)
    print("RESUMEN EJECUTIVO")
    print("─" * 40)
    df_resumen = generar_resumen_ejecutivo(
        df_comp, dup, df_temp, df_montos, df_modal, resumen_urls
    )
    resultados["resumen_ejecutivo"] = df_resumen

    # 8. Exportación
    print("\n" + "─" * 40)
    print("EXPORTACIÓN")
    print("─" * 40)
    artefactos = {
        "completitud": df_comp,
        "top_entidades_incompletas": df_top_ent,
        "duplicados_clave": dup.get("df_duplicados_clave", pd.DataFrame()),
        "multiples_contratos": multi,
        "orden_temporal": df_temp,
        "montos": df_montos,
        "modalidad_estado": df_modal_estado,
        "nit_invalidos": df_nit,
        "modalidades": df_modal,
        "rangos_temporales": df_rangos,
        "sondeo_urls": df_sondeo,
        "resumen_ejecutivo": df_resumen,
    }
    exportar_reporte_calidad(artefactos, ruta_salida=ruta_salida)

    print("\n" + "═" * 60)
    print("DIAGNÓSTICO COMPLETADO")
    print(f"  Artefactos en: {ruta_salida}")
    print("═" * 60)

    return resultados
