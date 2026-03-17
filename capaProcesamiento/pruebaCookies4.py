import re
import fitz
import pandas as pd
from pathlib import Path
from pruebaCookies3 import procesar_contrato


def extraer_campos_pdf(ruta_pdf: Path) -> dict:
    """
    Extrae los campos estructurados del PDF generado por el SECOP II.
    El formato del PDF es etiqueta: valor en líneas consecutivas.
    """
    doc   = fitz.open(ruta_pdf)
    texto = "\n".join(
        doc.load_page(i).get_text("text") for i in range(doc.page_count)) # type: ignore
    doc.close()

    # Normalizar espacios y saltos de línea múltiples
    texto = re.sub(r'\n+', '\n', texto).strip()

    def extraer_valor(patron: str, texto: str,
                      grupo: int = 1, flags=re.IGNORECASE) -> str | None:
        match = re.search(patron, texto, flags)
        return match.group(grupo).strip() if match else None

    campos = {
        # Valor del contrato — puede venir como "23.076.923 COP" o similar
        "valor_pdf": extraer_valor(
            r'Precio estimado total[:\s]+([0-9.,]+\s*COP)', texto
        ),
        # Nombre de la entidad contratante
        "entidad_pdf": extraer_valor(
            r'(?:entidad|contratante)[:\s]+([^\n]+)', texto
        ),
        # Número o referencia del proceso
        "numero_proceso_pdf": extraer_valor(
            r'[Nn]úmero del proceso[:\s]+([^\n]+)', texto
        ),
        # Título del contrato
        "titulo_pdf": extraer_valor(
            r'[Tt]ítulo[:\s]+([^\n]+)', texto
        ),
        # Estado / Fase
        "estado_pdf": extraer_valor(
            r'(?:Estado|Fase)[:\s]+([^\n]+)', texto
        ),
        # Tipo de proceso / modalidad
        "tipo_proceso_pdf": extraer_valor(
            r'[Tt]ipo de proceso[:\s]+([^\n]+)', texto
        ),
        # Fecha de terminación
        "fecha_terminacion_pdf": extraer_valor(
            r'[Ff]echa de terminaci[oó]n[^:]*[:\s]+([^\n]+)', texto
        ),
        # Descripción del objeto
        "descripcion_pdf": extraer_valor(
            r'[Dd]escripci[oó]n[:\s]+([^\n]{20,})', texto
        ),
    }

    # Limpiar valor monetario para comparación numérica
    if campos["valor_pdf"]:
        valor_limpio = re.sub(r'[^\d]', '', campos["valor_pdf"])
        campos["valor_numerico_pdf"] = int(valor_limpio) if valor_limpio else None # type: ignore
    else:
        campos["valor_numerico_pdf"] = None

    return campos


def verificar_consistencia(fila_api: pd.Series, campos_pdf: dict) -> dict:
    """
    Cruza los datos de un contrato entre la API del SECOP II y el PDF
    descargado del portal. Retorna un dict con el resultado de cada
    verificación y una clasificación global del registro.

    Params:
        fila_api   : Fila del DataFrame preprocesado del notebook de calidad.
        campos_pdf : Dict retornado por extraer_campos_pdf().

    Returns:
        Dict con verificaciones individuales, inconsistencias detectadas
        y clasificación global (CONSISTENTE / ADVERTENCIA / INCONSISTENTE).
    """
    verificaciones = {}
    inconsistencias = []

    # ── 1. Valor del contrato ─────────────────────────────────────────────
    valor_api = fila_api.get("valor_del_contrato")
    valor_pdf = campos_pdf.get("valor_numerico_pdf")

    if valor_api is not None and valor_pdf is not None:
        try:
            diferencia_pct = abs(float(valor_api) - valor_pdf) / float(valor_api) * 100
            consistente    = diferencia_pct < 1.0  # tolerancia del 1%
            verificaciones["valor"] = {
                "api": float(valor_api),
                "pdf": valor_pdf,
                "diferencia_pct": round(diferencia_pct, 4),
                "resultado": "OK" if consistente else "INCONSISTENTE",
            }
            if not consistente:
                inconsistencias.append(
                    f"Valor: API=${valor_api:,.0f} vs PDF=${valor_pdf:,.0f} "
                    f"({diferencia_pct:.2f}% de diferencia)"
                )
        except (ValueError, TypeError):
            verificaciones["valor"] = {"resultado": "NO_VERIFICABLE"}
    else:
        verificaciones["valor"] = {"resultado": "DATO_AUSENTE"}

    # ── 2. Modalidad / tipo de proceso ────────────────────────────────────
    modal_api = str(fila_api.get("modalidad_de_contratacion", "")).upper().strip()
    modal_pdf = str(campos_pdf.get("tipo_proceso_pdf", "")).upper().strip()

    if modal_api and modal_pdf:
        # Verificación por contenido parcial — tolera variaciones de formato
        consistente = (modal_api in modal_pdf) or (modal_pdf in modal_api)
        verificaciones["modalidad"] = {
            "api": modal_api,
            "pdf": modal_pdf,
            "resultado": "OK" if consistente else "ADVERTENCIA",
        }
        if not consistente:
            inconsistencias.append(
                f"Modalidad: API='{modal_api}' vs PDF='{modal_pdf}'"
            )
    else:
        verificaciones["modalidad"] = {"resultado": "DATO_AUSENTE"}

    # ── 3. Estado del contrato ────────────────────────────────────────────
    estado_api = str(fila_api.get("estado_contrato", "")).upper().strip()
    estado_pdf = str(campos_pdf.get("estado_pdf", "")).upper().strip()

    if estado_api and estado_pdf:
        consistente = (estado_api in estado_pdf) or (estado_pdf in estado_api)
        verificaciones["estado"] = {
            "api": estado_api,
            "pdf": estado_pdf,
            "resultado": "OK" if consistente else "ADVERTENCIA",
        }
        if not consistente:
            inconsistencias.append(
                f"Estado: API='{estado_api}' vs PDF='{estado_pdf}'"
            )
    else:
        verificaciones["estado"] = {"resultado": "DATO_AUSENTE"}

    # ── 4. Nombre de la entidad ───────────────────────────────────────────
    entidad_api = str(fila_api.get("nombre_entidad", "")).upper().strip()
    entidad_pdf = str(campos_pdf.get("entidad_pdf", "")).upper().strip()

    if entidad_api and entidad_pdf:
        # Comparación por palabras clave para tolerar abreviaciones
        palabras_api = set(entidad_api.split())
        palabras_pdf = set(entidad_pdf.split())
        interseccion = palabras_api & palabras_pdf
        similitud    = len(interseccion) / max(len(palabras_api), 1)
        consistente  = similitud >= 0.5
        verificaciones["entidad"] = {
            "api":       entidad_api,
            "pdf":       entidad_pdf,
            "similitud": round(similitud, 3),
            "resultado": "OK" if consistente else "ADVERTENCIA",
        }
        if not consistente:
            inconsistencias.append(
                f"Entidad: API='{entidad_api}' vs PDF='{entidad_pdf}' "
                f"(similitud={similitud:.0%})"
            )
    else:
        verificaciones["entidad"] = {"resultado": "DATO_AUSENTE"}

    # ── Clasificación global ──────────────────────────────────────────────
    resultados = [v["resultado"] for v in verificaciones.values()]

    if "INCONSISTENTE" in resultados:
        clasificacion = "INCONSISTENTE"
    elif "ADVERTENCIA" in resultados:
        clasificacion = "ADVERTENCIA"
    elif all(r == "OK" for r in resultados if r != "DATO_AUSENTE"):
        clasificacion = "CONSISTENTE"
    else:
        clasificacion = "NO_VERIFICABLE"

    return {
        "notice_uid":      fila_api.get("proceso_de_compra", ""),
        "id_contrato":     fila_api.get("id_contrato", ""),
        "verificaciones":  verificaciones,
        "inconsistencias": inconsistencias,
        "n_inconsistencias": len(inconsistencias),
        "clasificacion":   clasificacion,
    }
    

def pipeline_completo(notice_uid: str, fila_api: pd.Series,
                      carpeta: Path = Path("./docs_descargados")) -> dict:
    """
    Para un contrato dado:
      1. Descarga el PDF via Playwright
      2. Extrae los campos estructurados del PDF
      3. Calcula el hash SHA-256
      4. Cruza los datos del PDF contra la API del SECOP II
      5. Retorna todo consolidado listo para notarizar o reportar

    El hash se calcula DESPUÉS de la verificación para que el registro
    en blockchain lleve también la clasificación de consistencia —
    un contrato INCONSISTENTE notarizado queda marcado como tal
    en la cadena desde el origen.
    """
    # Paso 1 y 2: descarga y extracción
    resultado_descarga = procesar_contrato(notice_uid, carpeta=carpeta)
    if "error" in resultado_descarga:
        return resultado_descarga

    ruta_pdf    = Path(resultado_descarga["ruta_local"])
    campos_pdf  = extraer_campos_pdf(ruta_pdf)
    hash_sha256 = resultado_descarga["hash_sha256"]

    # Paso 3: cruce con API
    resultado_cruce = verificar_consistencia(fila_api, campos_pdf)

    return {
        **resultado_descarga,
        "campos_pdf":        campos_pdf,
        "cruce_api_pdf":     resultado_cruce,
        "clasificacion":     resultado_cruce["clasificacion"],
        "n_inconsistencias": resultado_cruce["n_inconsistencias"],
    }
    

if __name__ == "__main__":
    resultado = pipeline_completo("CO1.NTC.9089204", pd.Series({}))
    
    print(f"\n{'='*60}")
    print("RESULTADO FINAL:")
    for k, v in resultado.items():
        if k != "texto_preview":
            print(f"  {k}: {v}")