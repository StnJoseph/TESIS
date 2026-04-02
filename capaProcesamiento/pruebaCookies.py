"""
Documento de prueba para descargar el PDF de un contrato del SECOP II a partir de la URL pública
del proceso, extrae su texto y calcula el hash SHA-256 para notarización.

Flujo:
    1. GET a la página del proceso  →  extrae el mkey del botón Imprimir
    2. GET a PrintPDF?mkey=...      →  descarga el PDF
    3. Extracción de texto con PyMuPDF
    4. Cálculo de hash SHA-256 binario

Tesis: Capa de Notarización Digital sobre SECOP II
Universidad de los Andes - Ingeniería de Sistemas y Computación
Joseph Steven Linares Gutierrez
2026

Dependencias: requests, pymupdf, beautifulsoup4
    pip install requests pymupdf beautifulsoup4
"""

import re
import hashlib
import requests
import fitz  # PyMuPDF
from pathlib import Path


# ── Configuración — actualizar cookies cuando expiren ─────────────────────────

COOKIES = {
    "PublicSessionCookie": "4nbmhu0rijry2ym5kh4xkj3k",
    "ROUTEID":             ".fe_com_04",
    "STSSessionCookie":    "ffvva4wfs3ogdyd2v0i1qv4w",
}

HEADERS_PAGINA = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;"
        "q=0.9,image/avif,image/webp,image/apng,*/*;"
        "q=0.8,application/signed-exchange;v=b3;q=0.7"
    ),
    "Accept-Language":        "es-CO,es;q=0.9,en-AU;q=0.8,en;q=0.7",
    "Connection":             "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest":         "document",
    "Sec-Fetch-Mode":         "navigate",
    "Sec-Fetch-Site":         "same-origin",
    "Sec-Fetch-User":         "?1",
    "sec-ch-ua":              '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
    "sec-ch-ua-mobile":       "?0",
    "sec-ch-ua-platform":     '"Windows"',
}

# Referer exacto que el navegador usa al llamar PrintPDF
# Incluye isModal=true&asPopupView=true porque el mkey se genera en esa vista
REFERER_PDF = (
    "https://community.secop.gov.co/Public/Tendering/"
    "OpportunityDetail/Index"
    "?noticeUID={notice_uid}"
    "&isFromPublicArea=True"
    "&isModal=true"
    "&asPopupView=true"
)

BASE_URL   = "https://community.secop.gov.co"
PRINT_PATH = "/Public/Tendering/OpportunityDetail/PrintPDF"


# ── Paso 1: obtener el mkey desde la página del proceso ───────────────────────

def obtener_mkey(url_proceso: str) -> tuple[str | None, str | None]:
    """
    Visita la página pública del proceso y extrae el mkey del botón Imprimir.
    Retorna (mkey, url_final) donde url_final se usa como Referer en la descarga.

    El mkey está en el onclick del botón btnTbPrint con este formato:
        onclick="javascript:window.location.href =
            '/Public/Tendering/OpportunityDetail/PrintPDF' + '?mkey=XXXX'"
    """
    resp = requests.get(
        url_proceso,
        cookies=COOKIES,
        headers=HEADERS_PAGINA,
        timeout=30,
    )
    resp.raise_for_status()
    html = resp.text

    # Estrategia 1 — patrón exacto del onclick del botón Imprimir
    # Cubre: PrintPDF' + '?mkey=XXX  y  PrintPDF?mkey=XXX
    patron_boton = r"PrintPDF[^?]*\?mkey=([\w\-]+)"
    match = re.search(patron_boton, html)
    if match:
        mkey = match.group(1)
        print(f"[mkey] Encontrado vía botón PrintPDF: {mkey}")
        return mkey, resp.url

    # Estrategia 2 — atributo data-mkey en cualquier elemento HTML
    soup = BeautifulSoup(html, "html.parser")
    elem = soup.find(attrs={"data-mkey": True})
    if elem:
        mkey = elem["data-mkey"]
        print(f"[mkey] Encontrado vía data-mkey: {mkey}")
        return mkey, resp.url

    print("[mkey] No se encontró el mkey.")
    print(html[:3000])
    return None, None


# ── Paso 2: descargar el PDF usando el mkey ───────────────────────────────────

def descargar_pdf(mkey: str, notice_uid: str,
                  id_contrato: str, carpeta: Path) -> Path | None:
    """
    Descarga el PDF del endpoint PrintPDF replicando exactamente
    los headers que envía Chrome, incluyendo el Referer correcto.
    """
    url_pdf   = f"{BASE_URL}{PRINT_PATH}?mkey={mkey}"
    referer   = REFERER_PDF.format(notice_uid=notice_uid)

    headers_descarga = {
        **HEADERS_PAGINA,
        "Referer": referer,
    }

    resp = requests.get(
        url_pdf,
        cookies=COOKIES,
        headers=headers_descarga,
        timeout=60,
    )
    resp.raise_for_status()

    content_type = resp.headers.get("Content-Type", "")
    if "pdf" not in content_type.lower():
        print(f"[descarga] Content-Type inesperado: '{content_type}'")
        print(f"[descarga] Tamaño: {len(resp.content)} bytes")
        print(f"[descarga] Primeros bytes: {resp.content[:200]}")
        return None

    carpeta.mkdir(parents=True, exist_ok=True)
    ruta = carpeta / f"{id_contrato}.pdf"
    ruta.write_bytes(resp.content)
    print(f"[descarga] PDF guardado: {ruta.name} ({len(resp.content):,} bytes)")
    return ruta


# ── Pasos 3 y 4: extracción de texto y cálculo de hash ───────────────────────

def extraer_y_hashear(ruta_pdf: Path) -> dict:
    """
    Extrae el texto del PDF con PyMuPDF y calcula el hash SHA-256 binario.
    El hash se calcula sobre los bytes crudos del archivo para máxima
    integridad — sensible a cualquier modificación, incluso en metadatos.
    """
    doc = fitz.open(ruta_pdf)
    texto= "\n".join(doc.load_page(i).get_text("text") for i in range(doc.page_count)) # type: ignore
    n_paginas = doc.page_count
    doc.close()

    hash_sha256 = hashlib.sha256(ruta_pdf.read_bytes()).hexdigest()

    print(f"[hash] Páginas  : {n_paginas}")
    print(f"[hash] SHA-256  : {hash_sha256}")
    print(f"[hash] Preview  :\n{texto[:400]}")

    return {
        "n_paginas":     n_paginas,
        "hash_sha256":   hash_sha256,
        "texto_preview": texto[:400],
    }
    
def diagnosticar_descarga(mkey: str, url_origen: str):
    """
    Muestra todos los headers de respuesta del endpoint PrintPDF
    para identificar por qué rechaza la petición.
    """
    url_pdf = f"{BASE_URL}{PRINT_PATH}?mkey={mkey}"

    headers_descarga = {**HEADERS_PAGINA, "Referer": url_origen}

    resp = requests.get(
        url_pdf,
        cookies=COOKIES,
        headers=headers_descarga,
        timeout=60,
        allow_redirects=True,   # seguir redirects si los hay
    )

    print(f"Status code     : {resp.status_code}")
    print(f"URL final        : {resp.url}")
    print(f"Tamaño respuesta : {len(resp.content):,} bytes")
    print("\nResponse headers:")
    for k, v in resp.headers.items():
        print(f"  {k}: {v}")

    print(f"\nPrimeros 500 bytes del cuerpo:")
    print(resp.content[:500])

    # Guardar respuesta completa para inspección
    Path("respuesta_pdf_debug.bin").write_bytes(resp.content)
    print("\nRespuesta guardada en: respuesta_pdf_debug.bin")


# ── Pipeline completo ─────────────────────────────────────────────────────────

def procesar_contrato(url_proceso: str, id_contrato: str,
                      carpeta: Path = Path("./docs_descargados")) -> dict:

    print(f"\n{'='*60}")
    print(f"Procesando contrato: {id_contrato}")
    print(f"{'='*60}")

    # Extraer notice_uid de la URL para construir el Referer correcto
    match_uid = re.search(r'noticeUID=([\w\.]+)', url_proceso)
    notice_uid = match_uid.group(1) if match_uid else id_contrato

    # Paso 1: extraer mkey
    mkey, _ = obtener_mkey(url_proceso)
    if mkey is None:
        return {"id_contrato": id_contrato, "error": "No se pudo obtener el mkey"}

    # Paso 2: descargar PDF con Referer y headers correctos
    ruta_pdf = descargar_pdf(mkey, notice_uid, id_contrato, carpeta)
    if ruta_pdf is None:
        return {"id_contrato": id_contrato, "error": "Descarga del PDF fallida"}

    # Pasos 3 y 4: texto y hash
    resultado = extraer_y_hashear(ruta_pdf)

    return {
        "id_contrato":   id_contrato,
        "url_proceso":   url_proceso,
        "mkey":          mkey,
        "ruta_local":    str(ruta_pdf),
        **resultado,
    }


# ── Diagnóstico ───────────────────────────────────────────────────────────────

def diagnosticar_respuesta(url_proceso: str):
    resp = requests.get(url_proceso, cookies=COOKIES, headers=HEADERS_PAGINA, timeout=30)
    print(f"Status code     : {resp.status_code}")
    print(f"Content-Type    : {resp.headers.get('Content-Type', 'N/A')}")
    print(f"Tamaño respuesta: {len(resp.text):,} caracteres")
    Path("respuesta_debug.html").write_text(resp.text, encoding="utf-8")
    print("HTML guardado en: respuesta_debug.html")
    for termino in ["mkey", "PrintPDF", "btnTbPrint"]:
        idx = resp.text.lower().find(termino.lower())
        if idx != -1:
            print(f"\nTérmino '{termino}' en posición {idx}:")
            print(f"  ...{resp.text[max(0,idx-60):idx+140]}...")
        else:
            print(f"Término '{termino}': NO encontrado")


# ── Ejecución ─────────────────────────────────────────────────────────────────
            
if __name__ == "__main__":

    URL_PROCESO = (
        "https://community.secop.gov.co/Public/Tendering/"
        "OpportunityDetail/Index"
        "?noticeUID=CO1.NTC.9089204"
        "&isFromPublicArea=True"
    )

    # Paso 1: obtener mkey
    mkey, url_origen = obtener_mkey(URL_PROCESO)
    print(f"mkey obtenido: {mkey}")
    print(f"url_origen   : {url_origen}")

    # Paso 2: diagnosticar la descarga antes de intentar guardar
    if mkey:
        diagnosticar_descarga(mkey, url_origen) # type: ignore