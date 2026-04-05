"""
pipeline_notarizacion.py
========================
Pipeline integrado de notarización digital para contratos del SECOP II.

Integra la capa de acceso (API SODA) con la capa de procesamiento (extracción
de PDFs y hashing) y amplía la verificación de consistencia entre los datos
de la API y los datos extraídos del documento contractual.

Flujo principal:
    1. Extrae contratos de la API del SECOP II con filtros configurables.
    2. Para cada contrato, descarga el PDF via Playwright.
    3. Extrae texto y campos estructurados del PDF.
    4. Calcula el hash SHA-256 sobre los bytes crudos del archivo.
    5. Verifica la consistencia entre API y PDF en ocho dimensiones.
    6. Genera un registro consolidado listo para notarización en blockchain.
    7. Exporta los resultados a un archivo JSON y un CSV de resumen.

Tesis: Capa de Notarización Digital sobre SECOP II
Universidad de los Andes — Ingeniería de Sistemas y Computación
Joseph Steven Linares Gutiérrez — 2026

Dependencias:
    pip install sodapy pandas requests playwright pymupdf python-dotenv
    python -m playwright install chromium
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import fitz  # PyMuPDF
import pandas as pd
from dotenv import load_dotenv
from playwright.sync_api import TimeoutError as PlaywrightTimeout
from playwright.sync_api import sync_playwright
from sodapy import Socrata

# ── Configuración de logging ──────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Variables de entorno ──────────────────────────────────────────────────────
# Crear un archivo .env en la raíz del proyecto con el siguiente contenido:
#
#   SOCRATA_APP_TOKEN=tu_token_aqui
#   PUBLIC_SESSION_COOKIE=valor_de_la_cookie
#   ROUTE_ID=valor_de_routeid
#   STS_SESSION_COOKIE=valor_de_sts
#
# Las cookies se obtienen desde DevTools del navegador tras resolver el CAPTCHA
# en community.secop.gov.co. Su vida útil es limitada y deben renovarse
# manualmente cuando expiren. Ver sección "Limitaciones" en la documentación.

load_dotenv()

# ── Constantes de configuración ───────────────────────────────────────────────

DOMAIN     = "www.datos.gov.co"
DATASET_ID = "jbjy-vk9h"          # SECOP II – Contratos Electrónicos
BASE_URL   = "https://community.secop.gov.co"
PRINT_PATH = "/Public/Tendering/OpportunityDetail/PrintPDF"

COOKIES_SESION = [
    {
        "name":   "PublicSessionCookie",
        "value":  os.getenv("PUBLIC_SESSION_COOKIE", ""),
        "domain": "community.secop.gov.co",
        "path":   "/",
    },
    {
        "name":   "ROUTEID",
        "value":  os.getenv("ROUTE_ID", ""),
        "domain": "community.secop.gov.co",
        "path":   "/",
    },
    {
        "name":   "STSSessionCookie",
        "value":  os.getenv("STS_SESSION_COOKIE", ""),
        "domain": "community.secop.gov.co",
        "path":   "/",
    },
]

# Columnas que se solicitan a la API — selección de los campos relevantes
# para el cruce de consistencia con el PDF
COLUMNAS_API = [
    "proceso_de_compra",
    "id_contrato",
    "nombre_entidad",
    "nit_entidad",
    "departamento",
    "ciudad",
    "sector",
    "estado_contrato",
    "tipo_de_contrato",
    "modalidad_de_contratacion",
    "objeto_del_contrato",
    "descripcion_del_proceso",
    "proveedor_adjudicado",
    "documento_proveedor",
    "valor_del_contrato",
    "saldo_cdp",
    "fecha_de_firma",
    "fecha_de_inicio_del_contrato",
    "fecha_de_fin_del_contrato",
    "duraci_n_del_contrato",
    "urlproceso",
]

# ── Estructuras de datos ──────────────────────────────────────────────────────

@dataclass
class ResultadoVerificacion:
    """Resultado de la comparación entre un campo de la API y su equivalente en el PDF."""
    campo:     str
    valor_api: str | None
    valor_pdf: str | None
    resultado: str          # OK | ADVERTENCIA | INCONSISTENTE | DATO_AUSENTE | NO_VERIFICABLE
    detalle:   str = ""


@dataclass
class RegistroNotarizacion:
    """
    Unidad de registro lista para ser enviada a la capa de notarización
    blockchain. Contiene el hash del documento, la clasificación de
    consistencia y los metadatos de trazabilidad del proceso.
    """
    notice_uid:          str
    id_contrato:         str
    hash_sha256:         str
    timestamp_pipeline:  str
    n_paginas:           int
    clasificacion:       str          # CONSISTENTE | ADVERTENCIA | INCONSISTENTE | NO_VERIFICABLE
    n_inconsistencias:   int
    verificaciones:      list[dict]  = field(default_factory=list)
    inconsistencias:     list[str]   = field(default_factory=list)
    campos_pdf:          dict        = field(default_factory=dict)
    ruta_local:          str         = ""
    error:               str         = ""


# ── Capa de acceso: extracción desde la API del SECOP II ─────────────────────

def extraer_contratos_api(
    n_registros:   int   = 10,
    modalidad:     str   = "Contratación directa",
    valor_minimo:  int   = 100000000,
    solo_vigentes: bool  = True,
) -> pd.DataFrame:
    """
    Extrae contratos del SECOP II mediante la API SODA con los filtros
    indicados. Retorna un DataFrame con las columnas definidas en COLUMNAS_API.

    Params:
        n_registros   : Número de contratos a extraer (máximo recomendado: 50
                        por limitaciones de Playwright y cookies de sesión).
        modalidad     : Modalidad de contratación a filtrar. Por defecto
                        Contratación directa, que concentra el 75.71% de los
                        contratos y es la de mayor riesgo según el diagnóstico.
        valor_minimo  : Valor mínimo del contrato en pesos colombianos.
        solo_vigentes : Si es True, filtra únicamente contratos en estado
                        Activo o En ejecución.
    """
    token  = os.getenv("SOCRATA_APP_TOKEN", None)
    client = Socrata(DOMAIN, token, timeout=60)

    where_parts = [f"valor_del_contrato > {valor_minimo}"]
    
    if modalidad:
        where_parts.append(f"modalidad_de_contratacion = '{modalidad}'")
    if solo_vigentes:
        where_parts.append(
            "estado_contrato = 'En ejecución'"
        )

    where_expr = " AND ".join(where_parts)
    order_expr = "valor_del_contrato DESC"

    log.info(
        "Consultando API SECOP II | modalidad='%s' | valor>%s | n=%d",
        modalidad, f"{valor_minimo:,}", n_registros,
    )

    rows = client.get(
        DATASET_ID,
        select=",".join(COLUMNAS_API),
        where=where_expr,
        order=order_expr,
        limit=n_registros,
    )

    df = pd.DataFrame.from_records(rows)
    log.info("Registros recibidos de la API: %d", len(df))

    # Extraer la URL limpia del campo urlproceso (que llega como dict)
    if "urlproceso" in df.columns:
        df["url_proceso"] = df["urlproceso"].apply(
            lambda x: x.get("url") if isinstance(x, dict) else x
        )
        df.drop(columns=["urlproceso"], inplace=True)
    else:
        df["url_proceso"] = None

    # Extraer notice_uid desde la URL del proceso para usar con Playwright
    def _extraer_notice_uid(url: str | None) -> str | None:
        if not url:
            return None
        m = re.search(r'noticeUID=([\w\.]+)', str(url))
        return m.group(1) if m else None

    df["notice_uid"] = df["url_proceso"].apply(_extraer_notice_uid)

    # Convertir valor_del_contrato a numérico
    df["valor_del_contrato"] = pd.to_numeric(
        df["valor_del_contrato"], errors="coerce"
    )

    # Normalizar campos de fecha
    for col in ["fecha_de_firma", "fecha_de_inicio_del_contrato", "fecha_de_fin_del_contrato"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d")

    log.info(
        "Contratos con notice_uid válido: %d / %d",
        df["notice_uid"].notna().sum(), len(df),
    )
    return df


# ── Capa de procesamiento: descarga de PDF via Playwright ─────────────────────

def descargar_pdf_playwright(
    notice_uid: str,
    carpeta:    Path = Path("./docs_descargados"),
    headless:   bool = True,
    reintentos: int  = 2,
) -> Path | None:
    """
    Descarga el PDF del portal transaccional del SECOP II usando Playwright.

    Correcciones respecto a la versión anterior:
    - Un único clic sobre el botón. Se detecta el resultado (descarga directa
      o nueva pestaña) sin repetir el clic, eliminando el efecto cascada.
    - wait_until="domcontentloaded" en lugar de "networkidle" para evitar
      bloqueos en páginas con polling continuo.
    - Detección explícita de sesión expirada (redirección a CAPTCHA).
    - Reintentos configurables con pausa entre intentos.
    - Timeouts aumentados para el servidor de generación de PDFs.
    """
    url_proceso = (
        f"{BASE_URL}/Public/Tendering/OpportunityDetail/Index"
        f"?noticeUID={notice_uid}&isFromPublicArea=True"
    )

    carpeta.mkdir(parents=True, exist_ok=True)
    ruta_pdf = carpeta / f"{notice_uid}.pdf"

    if ruta_pdf.exists() and ruta_pdf.stat().st_size > 1_000:
        log.info("[%s] PDF ya existe localmente, reutilizando.", notice_uid)
        return ruta_pdf

    for intento in range(1, reintentos + 1):
        log.info("[%s] Intento %d/%d", notice_uid, intento, reintentos)
        resultado = _intentar_descarga(notice_uid, url_proceso, ruta_pdf, headless)

        if resultado == "ok":
            return ruta_pdf
        elif resultado == "sesion_expirada":
            # No tiene sentido reintentar si la cookie venció
            log.error(
                "[%s] Sesión expirada. Renovar las cookies en el archivo .env "
                "abriendo community.secop.gov.co en el navegador, resolviendo "
                "el CAPTCHA y copiando los nuevos valores de PublicSessionCookie, "
                "ROUTEID y STSSessionCookie desde DevTools → Application → Cookies.",
                notice_uid,
            )
            return None
        elif resultado == "boton_no_encontrado":
            log.warning("[%s] Botón de impresión no encontrado.", notice_uid)
            # Puede ser un problema transitorio — reintentar
        else:
            # "error_descarga" u otro fallo
            log.warning("[%s] Descarga fallida en intento %d.", notice_uid, intento)

        if intento < reintentos:
            time.sleep(3)

    log.error("[%s] Todos los intentos fallaron.", notice_uid)
    return None


def _intentar_descarga(
    notice_uid:  str,
    url_proceso: str,
    ruta_pdf:    Path,
    headless:    bool,
) -> str:
    """
    Ejecuta un único intento de descarga del PDF.

    Retorna uno de los siguientes códigos de resultado:
        "ok"                 — PDF descargado correctamente.
        "sesion_expirada"    — Cookies inválidas, se detectó redirección a CAPTCHA.
        "boton_no_encontrado"— El botón #btnTbPrint no apareció en la página.
        "error_descarga"     — Fallo en la descarga por otras causas.
    """
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            accept_downloads=True,
        )
        context.add_cookies(COOKIES_SESION)
        page = context.new_page()

        # ── Cargar la página ──────────────────────────────────────────────
        # domcontentloaded es más robusto que networkidle para portales
        # con polling continuo o conexiones persistentes.
        log.info("[%s] Cargando página...", notice_uid)
        try:
            page.goto(url_proceso, wait_until="domcontentloaded", timeout=45_000)
        except PlaywrightTimeout:
            log.warning("[%s] Timeout al cargar la página.", notice_uid)
            browser.close()
            return "error_descarga"

        # ── Detectar sesión expirada ──────────────────────────────────────
        # Cuando las cookies son inválidas el portal redirige a una página
        # de verificación CAPTCHA o de inicio de sesión. Se detecta por
        # ausencia del contenido esperado o presencia de indicadores de login.
        url_actual = page.url.lower()
        if any(indicador in url_actual for indicador in
               ["captcha", "login", "signin", "auth", "error"]):
            browser.close()
            return "sesion_expirada"

        # Verificación adicional: si la página no tiene el contenedor
        # principal del detalle del proceso, la sesión probablemente expiró
        try:
            page.wait_for_selector(
                "#divMainContent, .opportunity-detail, #btnTbPrint",
                timeout=15_000,
            )
        except PlaywrightTimeout:
            # Tomar screenshot para diagnóstico antes de cerrar
            page.screenshot(path=f"debug_sesion_{notice_uid}.png")
            log.warning(
                "[%s] Contenido del proceso no encontrado. "
                "Screenshot guardado en debug_sesion_%s.png",
                notice_uid, notice_uid,
            )
            browser.close()
            # Si llegamos aquí sin redirección explícita podría ser
            # sesión expirada o proceso inexistente
            return "sesion_expirada"

        # ── Localizar el botón de impresión ──────────────────────────────
        # El portal puede renderizar el botón con distintos selectores
        # según la versión de la interfaz. Se prueban en orden de prioridad.
        SELECTORES_BOTON = [
            "#btnTbPrint",
            "button[onclick*='PrintPDF']",
            "a[onclick*='PrintPDF']",
            "input[onclick*='PrintPDF']",
            "button:has-text('Imprimir')",
            "a:has-text('Imprimir')",
        ]

        boton = None
        for selector in SELECTORES_BOTON:
            try:
                candidato = page.locator(selector).first
                candidato.wait_for(state="visible", timeout=5_000)
                boton = candidato
                log.info("[%s] Botón encontrado con selector: %s", notice_uid, selector)
                break
            except PlaywrightTimeout:
                continue

        if boton is None:
            page.screenshot(path=f"debug_boton_{notice_uid}.png")
            log.warning(
                "[%s] Ningún selector encontró el botón. "
                "Screenshot guardado en debug_boton_%s.png",
                notice_uid, notice_uid,
            )
            browser.close()
            return "boton_no_encontrado"

        # ── Clic único con detección del resultado ────────────────────────
        # Se registran listeners para descarga directa y nueva pestaña
        # ANTES del clic. Así un único clic puede ser capturado por
        # cualquiera de los dos mecanismos sin repetirlo.
        log.info("[%s] Ejecutando clic en el botón de impresión...", notice_uid)

        descarga_exitosa = False

        # Registrar listener de nueva pestaña antes del clic
        nueva_pagina: list = []

        def _capturar_pagina(p):
            nueva_pagina.append(p)

        context.on("page", _capturar_pagina)

        try:
            # Intentar capturar descarga directa
            with page.expect_download(timeout=15_000) as dl_info:
                boton.click()

            descarga = dl_info.value
            descarga.save_as(ruta_pdf)
            log.info(
                "[%s] Descarga directa exitosa: %s bytes",
                notice_uid, f"{ruta_pdf.stat().st_size:,}",
            )
            descarga_exitosa = True

        except PlaywrightTimeout:
            # No hubo descarga directa — verificar si se abrió una nueva pestaña
            log.info("[%s] Sin descarga directa — revisando nueva pestaña...", notice_uid)

            if nueva_pagina:
                nueva = nueva_pagina[0]
                try:
                    nueva.wait_for_load_state("domcontentloaded", timeout=30_000)
                    url_nueva = nueva.url
                    log.info("[%s] Nueva pestaña: %s", notice_uid, url_nueva)

                    # Si la URL de la nueva pestaña apunta a un PDF, descargarlo
                    if "PrintPDF" in url_nueva or "pdf" in url_nueva.lower():
                        pdf_bytes = nueva.evaluate("""
                            async () => {
                                const resp = await fetch(window.location.href,
                                                         {credentials: 'include'});
                                const buf  = await resp.arrayBuffer();
                                return Array.from(new Uint8Array(buf));
                            }
                        """)
                        if pdf_bytes and len(pdf_bytes) > 1_000:
                            ruta_pdf.write_bytes(bytes(pdf_bytes))
                            log.info(
                                "[%s] PDF obtenido desde nueva pestaña via fetch: %s bytes",
                                notice_uid, f"{len(pdf_bytes):,}",
                            )
                            descarga_exitosa = True
                        else:
                            log.warning("[%s] Fetch en nueva pestaña devolvió respuesta vacía.", notice_uid)
                    else:
                        # La nueva pestaña no apunta al PDF — intentar
                        # un segundo clic en el botón de impresión si existe
                        try:
                            boton_nuevo = nueva.locator("#btnTbPrint, button:has-text('Imprimir')").first
                            boton_nuevo.wait_for(state="visible", timeout=5_000)
                            with nueva.expect_download(timeout=15_000) as dl2:
                                boton_nuevo.click()
                            dl2.value.save_as(ruta_pdf)
                            log.info("[%s] Descarga desde botón en nueva pestaña exitosa.", notice_uid)
                            descarga_exitosa = True
                        except PlaywrightTimeout:
                            log.warning("[%s] Sin descarga desde nueva pestaña.", notice_uid)

                except PlaywrightTimeout:
                    log.warning("[%s] Timeout esperando nueva pestaña.", notice_uid)
                finally:
                    nueva.close()

            else:
                log.warning("[%s] No se detectó descarga ni nueva pestaña.", notice_uid)

        finally:
            context.remove_listener("page", _capturar_pagina)
            browser.close()

        if not descarga_exitosa:
            return "error_descarga"

        # Verificar que el archivo descargado es un PDF válido
        if not ruta_pdf.exists() or ruta_pdf.stat().st_size < 1_000:
            log.warning("[%s] Archivo descargado vacío o demasiado pequeño.", notice_uid)
            return "error_descarga"

        # Los PDFs comienzan con la firma %PDF
        with open(ruta_pdf, "rb") as f:
            firma = f.read(4)
        if firma != b"%PDF":
            log.warning(
                "[%s] El archivo descargado no tiene firma PDF válida "
                "(primeros bytes: %s). Posiblemente se descargó una página HTML.",
                notice_uid, firma,
            )
            ruta_pdf.unlink()  # Eliminar el archivo inválido
            return "error_descarga"

        return "ok"


# ── Extracción de campos estructurados del PDF ────────────────────────────────

def extraer_campos_pdf(ruta_pdf: Path) -> dict:
    """
    Extrae campos estructurados del PDF del SECOP II.

    El PDF organiza la información en una tabla de dos columnas. PyMuPDF
    lee primero toda la columna izquierda (etiquetas) y luego toda la
    columna derecha (valores), por lo que los patrones buscan valores
    directamente por su forma o por marcadores de sección conocidos.

    Mapeo con la API del SECOP II:
        valor_numerico_pdf  ←→  valor_del_contrato
        tipo_proceso_pdf    ←→  modalidad_de_contratacion
        proveedor_pdf       ←→  proveedor_adjudicado
        duracion_pdf        ←→  duraci_n_del_contrato
        fecha_fin_pdf       ←→  fecha_de_fin_del_contrato
    """
    doc   = fitz.open(ruta_pdf)
    texto = "\n".join(
        doc.load_page(i).get_text("text") for i in range(doc.page_count)
    )
    doc.close()

    # Normalizar múltiples saltos de línea a uno solo
    texto = re.sub(r'\n{2,}', '\n', texto).strip()

    def _buscar(patron: str, grupo: int = 1) -> str | None:
        m = re.search(patron, texto, re.IGNORECASE)
        if not m:
            return None
        return m.group(grupo).strip()

    def _proveedor() -> str | None:
        """
        El proveedor adjudicado aparece en la sección 'Información de la selección'
        bajo la etiqueta 'Entidad adjudicataria', con estructura fija:
            Entidad adjudicataria
            Valor del contrato
            Documento(s)
            Evaluación
            [NOMBRE_PROVEEDOR]    ← primer valor de la columna derecha
        """
        m = re.search(
            r'Entidad adjudicataria\nValor del contrato\nDocumento\(s\)\nEvaluaci[oó]n\n([^\n]+)',
            texto,
            re.IGNORECASE,
        )
        return m.group(1).strip() if m else None

    campos: dict[str, object] = {

        # Valor monetario: primer número con separadores de miles (punto)
        # seguido de COP. Ej: "365.000.000.000 COP"
        "valor_pdf": _buscar(r'([\d]+(?:\.\d{3})*(?:,\d+)?)\s*COP'),

        # Referencia interna del proceso en la entidad. Ej: "4131.010.32.1.941-2025"
        "numero_proceso_pdf": _buscar(r'(\d{4}[\.\d]+-\d{4})'),

        # Modalidad de contratación: valores fijos del SECOP II
        "tipo_proceso_pdf": _buscar(
            r'(contrataci[oó]n\s+r[eé]gimen\s+especial(?:\s+\(con\s+ofertas\))?'
            r'|contrataci[oó]n\s+directa(?:\s+\(con\s+ofertas\))?'
            r'|selecci[oó]n\s+abreviada\s+subasta\s+inversa'
            r'|selecci[oó]n\s+abreviada(?:\s+de)?\s+menor\s+cuant[ií]a(?:\s+sin[^\n]*)?'
            r'|licitaci[oó]n\s+p[uú]blica(?:\s+obra\s+p[uú]blica|\s+acuerdo\s+marco\s+de\s+precios)?'
            r'|concurso\s+de\s+m[eé]ritos(?:\s+abierto)?'
            r'|enajenaci[oó]n\s+de\s+bienes\s+con\s+(?:sobre\s+cerrado|subasta)'
            r'|m[ií]nima\s+cuant[ií]a)'
        ),

        # Duración: "N (Año(s))", "N (Mes(es))" o "N (Día(s))"
        # La API usa el mismo formato sin paréntesis alrededor del número:
        # "320 Dia(s)". Solo se compara el valor numérico.
        "duracion_pdf": _buscar(
            r'(\d+\s*\((?:Año\(s\)|Meses|D[ií]as|Semana\(s\))\))'
        ),

        # Fecha de terminación: primera fecha DD/MM/YYYY del documento.
        # Corresponde a 'Fecha de terminación del contrato' en el PDF.
        "fecha_fin_pdf": _buscar(r'(\d{2}/\d{2}/\d{4})'),

        # Proveedor adjudicado
        "proveedor_pdf": _proveedor(),
    }

    # Convertir valor monetario a entero para comparación numérica.
    # Se eliminan todos los caracteres no numéricos (puntos, comas, espacios).
    if campos["valor_pdf"]:
        valor_limpio = re.sub(r'[^\d]', '', str(campos["valor_pdf"]))
        campos["valor_numerico_pdf"] = int(valor_limpio) if valor_limpio else None
    else:
        campos["valor_numerico_pdf"] = None

    return campos

# ── Verificación de consistencia API vs PDF ───────────────────────────────────

def verificar_consistencia(
    fila_api: pd.Series,
    campos_pdf: dict,
) -> tuple[list[ResultadoVerificacion], list[str], str]:
    """
    Compara los datos de un contrato entre la API del SECOP II y el PDF
    en cinco dimensiones verificables.

    Dimensiones excluidas y motivo:
    - estado_contrato: el PDF siempre muestra 'Proceso adjudicado y celebrado'
      (estado al momento de la firma). La API muestra el estado actual
      (Cerrado, En ejecución, Cancelado, etc.). Son momentos distintos
      del ciclo de vida del contrato — la comparación sería estructuralmente
      inválida.
    - fase: no tiene campo equivalente en la API del SECOP II.
    """
    verificaciones: list[ResultadoVerificacion] = []
    inconsistencias: list[str] = []

    def _norm(valor: object) -> str:
        """Elimina caracteres especiales del SECOP (*, /, \) y normaliza espacios."""
        s = str(valor).upper().strip() if valor else ""
        s = re.sub(r'[*/\\]', ' ', s)
        s = re.sub(r'\s+', ' ', s)
        return s.strip()

    def _norm_fecha_api(valor: object) -> str:
        """Extrae YYYY-MM-DD de una fecha ISO de la API."""
        s = str(valor).strip()
        m = re.search(r'(\d{4}-\d{2}-\d{2})', s)
        return m.group(1) if m else ""

    def _norm_fecha_pdf(valor: object) -> str:
        """Convierte DD/MM/YYYY del PDF a YYYY-MM-DD para comparar con la API."""
        m = re.match(r'(\d{2})/(\d{2})/(\d{4})', str(valor).strip())
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}" if m else ""

    def _similitud(a: str, b: str, min_largo: int = 3) -> float:
        """
        Similitud como intersección de palabras sobre el conjunto mayor.
        Ignora palabras cortas (artículos, preposiciones) para reducir
        el ruido en nombres con abreviaturas concatenadas como MINTRABAJO
        o RNECALCALDÍA.
        """
        pa = {w for w in a.split() if len(w) >= min_largo}
        pb = {w for w in b.split() if len(w) >= min_largo}
        if not pa and not pb:
            return 1.0
        if not pa or not pb:
            return 0.0
        return len(pa & pb) / max(len(pa), len(pb))

    def _agregar(rv: ResultadoVerificacion) -> None:
        verificaciones.append(rv)
        if rv.resultado in ("INCONSISTENTE", "ADVERTENCIA"):
            inconsistencias.append(rv.detalle)

    # ── 1. Valor del contrato ─────────────────────────────────────────────
    # API: valor_del_contrato (string numérico, ej: "6400000")
    # PDF: valor_numerico_pdf (int, ej: 365000000000)

    # VALOR INCONGRUENTE CON EL REGISTRADO EN CONTRATO
    # valor_api = fila_api.get("valor_del_contrato")
    
    valor_api = fila_api.get("saldo_cdp") # Se cambia a saldo_cdp ya que este es consistente con el valor.
    valor_pdf = campos_pdf.get("valor_numerico_pdf")

    if valor_api is not None and valor_pdf is not None:
        try:
            v_api   = float(valor_api)
            dif_pct = abs(v_api - valor_pdf) / v_api * 100 if v_api != 0 else 0.0
            if dif_pct < 1.0:
                resultado, detalle = "OK", ""
            elif dif_pct < 5.0:
                resultado = "ADVERTENCIA"
                detalle   = f"Valor: API=${v_api:,.0f} vs PDF=${valor_pdf:,.0f} ({dif_pct:.2f}%)"
            else:
                resultado = "INCONSISTENTE"
                detalle   = f"Valor: API=${v_api:,.0f} vs PDF=${valor_pdf:,.0f} ({dif_pct:.2f}%)"
            _agregar(ResultadoVerificacion(
                campo="valor_contrato",
                valor_api=f"{v_api:,.0f}",
                valor_pdf=f"{valor_pdf:,.0f}",
                resultado=resultado,
                detalle=detalle,
            ))
        except (ValueError, TypeError):
            _agregar(ResultadoVerificacion(
                "valor_contrato", str(valor_api), str(valor_pdf), "NO_VERIFICABLE"
            ))
    else:
        _agregar(ResultadoVerificacion(
            "valor_contrato", str(valor_api), str(valor_pdf), "DATO_AUSENTE"
        ))

    # ── 2. Modalidad de contratación ──────────────────────────────────────
    # API: modalidad_de_contratacion (ej: "Contratación directa")
    # PDF: tipo_proceso_pdf (ej: "Contratación directa (con ofertas)")
    # Se verifica inclusión parcial porque el PDF puede tener el sufijo
    # "(con ofertas)" que no aparece en la API.
    modal_api = _norm(fila_api.get("modalidad_de_contratacion"))
    modal_pdf = _norm(campos_pdf.get("tipo_proceso_pdf"))

    if modal_api and modal_pdf:
        consistente = (modal_api in modal_pdf) or (modal_pdf in modal_api)
        resultado   = "OK" if consistente else "ADVERTENCIA"
        detalle     = "" if consistente else \
                      f"Modalidad: API='{modal_api}' vs PDF='{modal_pdf}'"
        _agregar(ResultadoVerificacion(
            "modalidad_contratacion", modal_api, modal_pdf, resultado, detalle
        ))
    else:
        _agregar(ResultadoVerificacion(
            "modalidad_contratacion", modal_api or None, modal_pdf or None, "DATO_AUSENTE"
        ))

    # ── 3. Proveedor adjudicado ───────────────────────────────────────────
    # API: proveedor_adjudicado (ej: "SONIA MILENA GARAVITO ROMERO")
    # PDF: proveedor_pdf / Entidad adjudicataria (ej: "BANCOLOMBIA")
    # Puede ser persona natural o jurídica. Similitud por palabras.
    prov_api = _norm(fila_api.get("proveedor_adjudicado"))
    prov_pdf = _norm(campos_pdf.get("proveedor_pdf"))

    if prov_api and prov_pdf:
        sim = _similitud(prov_api, prov_pdf)
        if sim >= 0.4:
            resultado, detalle = "OK", ""
        elif sim >= 0.2:
            resultado = "ADVERTENCIA"
            detalle   = f"Proveedor: sim={sim:.0%} | API='{prov_api}' | PDF='{prov_pdf}'"
        else:
            resultado = "INCONSISTENTE"
            detalle   = f"Proveedor: sim={sim:.0%} | API='{prov_api}' | PDF='{prov_pdf}'"
        _agregar(ResultadoVerificacion(
            "proveedor_adjudicado", prov_api, prov_pdf, resultado, detalle
        ))
    else:
        _agregar(ResultadoVerificacion(
            "proveedor_adjudicado", prov_api or None, prov_pdf or None, "DATO_AUSENTE"
        ))

    # ── 4. Duración del contrato ──────────────────────────────────────────
    # Normalización de unidades antes de comparar:
    #
    #   API             PDF              Canónica
    #   Dia(s)      ←→  (Días)       →   dia
    #   Semana(s)   ←→  (Semana(s))  →   semana
    #   Mes(es)     ←→  (Meses)      →   mes
    #   Año(s)      ←→  (Año(s))     →   año
    #
    # Si las unidades no coinciden se marca ADVERTENCIA en lugar de
    # comparar números sin contexto, evitando falsos positivos como
    # "12 Año(s)" vs "144 Mes(es)" que son equivalentes pero distintos
    # en representación.

    def _parsear_duracion(texto: str) -> tuple[int | None, str | None]:
        """
        Extrae (número, unidad_canónica) de una cadena de duración.
        Retorna (None, None) si no puede parsear.
        'No definido' se trata por fuera de esta función.
        """
        if not texto:
            return None, None

        m = re.search(r'(\d+)', texto)
        if not m:
            return None, None
        numero = int(m.group(1))

        t = texto.lower()
        if re.search(r'a[ñn]o', t):
            unidad = "año"
        elif re.search(r'mes', t):
            unidad = "mes"
        elif re.search(r'semana', t):
            unidad = "semana"
        elif re.search(r'd[ií]a', t):
            unidad = "dia"
        else:
            unidad = None

        return numero, unidad

    duracion_api_raw = str(fila_api.get("duraci_n_del_contrato", "")).strip()
    duracion_pdf_raw = str(campos_pdf.get("duracion_pdf", "")).strip()

    api_no_definida = (
        not duracion_api_raw
        or duracion_api_raw.lower() == "no definido"
        or duracion_api_raw.lower() == "none"
    )

    num_api, uni_api = _parsear_duracion(duracion_api_raw)
    num_pdf, uni_pdf = _parsear_duracion(duracion_pdf_raw)

    if api_no_definida and num_pdf is not None:
        # La API tiene un error de registro humano. El PDF es la fuente
        # confiable — se registra su valor como referencia y se marca
        # ADVERTENCIA para que quede trazado en el comprobante.
        _agregar(ResultadoVerificacion(
            campo     = "duracion_contrato",
            valor_api = "No definido (error de registro)",
            valor_pdf = duracion_pdf_raw,
            resultado = "ADVERTENCIA",
            detalle   = (
                f"Duración ausente en la API — valor del contrato: "
                f"'{duracion_pdf_raw}'"
            ),
        ))

    elif api_no_definida and num_pdf is None:
        # Ninguna de las dos fuentes tiene el dato
        _agregar(ResultadoVerificacion(
            "duracion_contrato", None, None, "DATO_AUSENTE"
        ))

    elif num_api is not None and num_pdf is not None:
        if uni_api is None or uni_pdf is None:
            resultado = "ADVERTENCIA"
            detalle   = (
                f"Duración: unidad no identificada | "
                f"API='{duracion_api_raw}' | PDF='{duracion_pdf_raw}'"
            )
        elif uni_api != uni_pdf:
            resultado = "ADVERTENCIA"
            detalle   = (
                f"Duración: unidades distintas | "
                f"API='{duracion_api_raw}' ({uni_api}) | "
                f"PDF='{duracion_pdf_raw}' ({uni_pdf})"
            )
        elif num_api == num_pdf:
            resultado, detalle = "OK", ""
        else:
            resultado = "INCONSISTENTE"
            detalle   = (
                f"Duración: API='{duracion_api_raw}' vs PDF='{duracion_pdf_raw}'"
            )
        _agregar(ResultadoVerificacion(
            "duracion_contrato",
            duracion_api_raw,
            duracion_pdf_raw,
            resultado,
            detalle,
        ))

    else:
        # API tiene valor pero PDF no pudo extraerlo
        _agregar(ResultadoVerificacion(
            "duracion_contrato",
            duracion_api_raw or None,
            duracion_pdf_raw or None,
            "DATO_AUSENTE",
        ))
        

    # ── 5. Fecha de terminación del contrato ──────────────────────────────
    # Una diferencia significativa entre API y PDF no necesariamente indica
    # un error. Si la API muestra una fecha posterior a la del PDF, el
    # contrato probablemente fue prorrogado. El SECOP II publica las
    # modificaciones como nuevas filas, por lo que la API refleja el estado
    # vigente y el PDF refleja las condiciones originales de la firma.
    # Ambos valores se registran en el comprobante para trazabilidad.

    fecha_api = _norm_fecha_api(fila_api.get("fecha_de_fin_del_contrato"))
    fecha_pdf = _norm_fecha_pdf(campos_pdf.get("fecha_fin_pdf", ""))

    if fecha_api and fecha_pdf:
        try:
            dt_api   = datetime.strptime(fecha_api, "%Y-%m-%d")
            dt_pdf   = datetime.strptime(fecha_pdf, "%Y-%m-%d")
            diff_dias = (dt_api - dt_pdf).days

            if abs(diff_dias) <= 1:
                # Diferencia de un día: margen por la convención del SECOP II
                # de publicar la hora límite como 11:59 PM del día anterior.
                resultado, detalle = "OK", ""
            elif diff_dias > 1:
                # API tiene fecha posterior: probable prórroga
                resultado = "ADVERTENCIA"
                detalle   = (
                    f"Fecha fin: PDF='{fecha_pdf}' (firma original) | "
                    f"API='{fecha_api}' (estado vigente, +{diff_dias} días) | "
                    f"Probable prórroga registrada en la API"
                )
            else:
                # API tiene fecha anterior al PDF: situación inusual
                resultado = "INCONSISTENTE"
                detalle   = (
                    f"Fecha fin: API='{fecha_api}' anterior al PDF='{fecha_pdf}' "
                    f"({abs(diff_dias)} días) — revisar manualmente"
                )

        except ValueError:
            resultado = "ADVERTENCIA"
            detalle   = (
                f"Fecha fin: API='{fecha_api}' vs PDF='{fecha_pdf}' — "
                f"no se pudo calcular diferencia"
            )

        _agregar(ResultadoVerificacion(
            "fecha_fin_contrato", fecha_api, fecha_pdf, resultado, detalle
        ))
    else:
        _agregar(ResultadoVerificacion(
            "fecha_fin_contrato", fecha_api or None, fecha_pdf or None, "DATO_AUSENTE"
        ))


    # ── Clasificación global ──────────────────────────────────────────────
    resultados_set = {v.resultado for v in verificaciones}

    if "INCONSISTENTE" in resultados_set:
        clasificacion = "INCONSISTENTE"
    elif "ADVERTENCIA" in resultados_set:
        clasificacion = "ADVERTENCIA"
    elif all(r in ("OK", "DATO_AUSENTE") for r in resultados_set):
        clasificacion = "CONSISTENTE" if "OK" in resultados_set else "NO_VERIFICABLE"
    else:
        clasificacion = "NO_VERIFICABLE"

    return verificaciones, inconsistencias, clasificacion


# ── Hashing del documento ─────────────────────────────────────────────────────

def calcular_hash_sha256(ruta_pdf: Path) -> str:
    """
    Calcula el hash SHA-256 sobre los bytes crudos del archivo PDF.
    El hash binario es sensible a cualquier modificación del documento,
    incluyendo cambios en metadatos o estructura interna, lo que lo hace
    más robusto para notarización que el hash sobre texto extraído.
    """
    return hashlib.sha256(ruta_pdf.read_bytes()).hexdigest()


# ── Pipeline integrado ────────────────────────────────────────────────────────

def pipeline_contrato(
    fila_api: pd.Series,
    carpeta:  Path = Path("./docs_descargados"),
    headless: bool = True,
) -> RegistroNotarizacion:
    """
    Ejecuta el pipeline completo para un contrato:
        1. Extrae el notice_uid de la fila de la API.
        2. Descarga el PDF via Playwright.
        3. Calcula el hash SHA-256.
        4. Extrae campos estructurados del PDF.
        5. Verifica la consistencia API vs PDF en 8 dimensiones.
        6. Retorna un RegistroNotarizacion listo para la capa blockchain.
    """
    notice_uid  = str(fila_api.get("notice_uid", ""))
    id_contrato = str(fila_api.get("id_contrato", ""))

    log.info("=" * 60)
    log.info("Procesando: %s | Contrato: %s", notice_uid, id_contrato)
    log.info("=" * 60)

    timestamp = datetime.now(timezone.utc).isoformat()

    # Paso 1: validar que existe notice_uid
    if not notice_uid or notice_uid == "None":
        log.warning("[%s] notice_uid no disponible — omitiendo.", id_contrato)
        return RegistroNotarizacion(
            notice_uid="N/A", id_contrato=id_contrato,
            hash_sha256="", timestamp_pipeline=timestamp,
            n_paginas=0, clasificacion="NO_VERIFICABLE",
            n_inconsistencias=0, error="notice_uid no disponible",
        )

    # Paso 2: descargar PDF
    ruta_pdf = descargar_pdf_playwright(notice_uid, carpeta, headless)
    if ruta_pdf is None:
        return RegistroNotarizacion(
            notice_uid=notice_uid, id_contrato=id_contrato,
            hash_sha256="", timestamp_pipeline=timestamp,
            n_paginas=0, clasificacion="NO_VERIFICABLE",
            n_inconsistencias=0, error="Descarga del PDF fallida",
        )

    # Paso 3: hash SHA-256
    hash_sha256 = calcular_hash_sha256(ruta_pdf)
    log.info("[%s] SHA-256: %s", notice_uid, hash_sha256)

    # Paso 4: extraer campos del PDF
    doc_fitz  = fitz.open(ruta_pdf)
    n_paginas = doc_fitz.page_count
    doc_fitz.close()
    campos_pdf = extraer_campos_pdf(ruta_pdf)

    # Paso 5: verificar consistencia
    verificaciones, inconsistencias, clasificacion = verificar_consistencia(fila_api, campos_pdf)

    log.info(
        "[%s] Clasificación: %s | Inconsistencias: %d",
        notice_uid, clasificacion, len(inconsistencias),
    )
    for inc in inconsistencias:
        log.warning("  ↳ %s", inc)

    return RegistroNotarizacion(
        notice_uid        = notice_uid,
        id_contrato       = id_contrato,
        hash_sha256       = hash_sha256,
        timestamp_pipeline= timestamp,
        n_paginas         = n_paginas,
        clasificacion     = clasificacion,
        n_inconsistencias = len(inconsistencias),
        verificaciones    = [asdict(v) for v in verificaciones],
        inconsistencias   = inconsistencias,
        campos_pdf        = campos_pdf,
        ruta_local        = str(ruta_pdf),
    )


# ── Exportación de resultados ─────────────────────────────────────────────────

def exportar_resultados(registros: list[RegistroNotarizacion], carpeta: Path) -> None:
    """
    Exporta los resultados del pipeline en dos formatos:
    - JSON completo con toda la información de cada registro (para blockchain).
    - CSV de resumen con las columnas clave para análisis (para el análisis de datos).
    """
    carpeta.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    # JSON completo
    ruta_json = carpeta / f"registros_notarizacion_{ts}.json"
    with open(ruta_json, "w", encoding="utf-8") as f:
        json.dump([asdict(r) for r in registros], f, ensure_ascii=False, indent=2)
    log.info("JSON exportado: %s", ruta_json)

    # CSV de resumen
    filas_resumen = []
    for r in registros:
        fila = {
            "notice_uid":        r.notice_uid,
            "id_contrato":       r.id_contrato,
            "hash_sha256":       r.hash_sha256,
            "timestamp_pipeline":r.timestamp_pipeline,
            "n_paginas":         r.n_paginas,
            "clasificacion":     r.clasificacion,
            "n_inconsistencias": r.n_inconsistencias,
            "error":             r.error,
        }
        # Aplanar resultados individuales de verificación
        for v in r.verificaciones:
            fila[f"rv_{v['campo']}"] = v["resultado"]
        filas_resumen.append(fila)

    df_resumen = pd.DataFrame(filas_resumen)
    ruta_csv   = carpeta / f"resumen_notarizacion_{ts}.csv"
    df_resumen.to_csv(ruta_csv, index=False, encoding="utf-8-sig")
    log.info("CSV exportado: %s", ruta_csv)

    # Resumen en consola
    log.info("\n%s", "=" * 60)
    log.info("RESUMEN DEL PIPELINE")
    log.info("Total procesados : %d", len(registros))
    log.info("  CONSISTENTE    : %d", sum(1 for r in registros if r.clasificacion == "CONSISTENTE"))
    log.info("  ADVERTENCIA    : %d", sum(1 for r in registros if r.clasificacion == "ADVERTENCIA"))
    log.info("  INCONSISTENTE  : %d", sum(1 for r in registros if r.clasificacion == "INCONSISTENTE"))
    log.info("  NO_VERIFICABLE : %d", sum(1 for r in registros if r.clasificacion == "NO_VERIFICABLE"))
    log.info("  Con error      : %d", sum(1 for r in registros if r.error))
    log.info("%s", "=" * 60)


# ── Punto de entrada ──────────────────────────────────────────────────────────

def main():
    """
    Ejecuta el pipeline completo:
        1. Extrae contratos de la API con los filtros configurados.
        2. Procesa cada contrato con el pipeline de notarización.
        3. Exporta los resultados.

    Para modificar los contratos a procesar, ajustar los parámetros de
    extraer_contratos_api() según el subconjunto de interés.
    """
    CARPETA_DOCS     = Path("./docs_descargados")
    CARPETA_RESULTS  = Path("./resultados")
    N_CONTRATOS      = 5        # Ajustar según disponibilidad de cookies y tiempo
    HEADLESS         = True     # False para ver el navegador durante el desarrollo

    # Paso 1: extracción de contratos de la API
    df_contratos = extraer_contratos_api(
        n_registros   = N_CONTRATOS,
        modalidad     = "Contratación directa",
        valor_minimo  = 15000000,
        solo_vigentes = False,
    )

    if df_contratos.empty:
        log.error("No se obtuvieron contratos de la API. Verificar filtros y conectividad.")
        return

    # Paso 2: pipeline por contrato
    registros: list[RegistroNotarizacion] = []
    for idx, fila in df_contratos.iterrows():
        try:
            registro = pipeline_contrato(fila, CARPETA_DOCS, HEADLESS)
            registros.append(registro)
            # Pausa entre contratos para no sobrecargar el portal
            time.sleep(2)
        except Exception as exc:
            log.exception("Error inesperado procesando %s: %s", fila.get("notice_uid"), exc)
            registros.append(RegistroNotarizacion(
                notice_uid        = str(fila.get("notice_uid", "N/A")),
                id_contrato       = str(fila.get("id_contrato", "N/A")),
                hash_sha256       = "",
                timestamp_pipeline= datetime.now(timezone.utc).isoformat(),
                n_paginas         = 0,
                clasificacion     = "NO_VERIFICABLE",
                n_inconsistencias = 0,
                error             = str(exc),
            ))

    # Paso 3: exportación
    exportar_resultados(registros, CARPETA_RESULTS)


if __name__ == "__main__":
    main()
