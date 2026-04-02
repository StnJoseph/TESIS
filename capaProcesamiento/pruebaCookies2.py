import re
import time
import hashlib
import requests
import fitz
from pathlib import Path

COOKIES = {
    "PublicSessionCookie": "4nbmhu0rijry2ym5kh4xkj3k",
    "ROUTEID":             ".fe_com_04",
    "STSSessionCookie":    "ffvva4wfs3ogdyd2v0i1qv4w",
}

HEADERS_BASE = {
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
    "Accept-Language":           "es-CO,es;q=0.9,en-AU;q=0.8,en;q=0.7",
    "Connection":                "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest":            "document",
    "Sec-Fetch-Mode":            "navigate",
    "Sec-Fetch-Site":            "same-origin",
    "Sec-Fetch-User":            "?1",
    "sec-ch-ua":        '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
}

BASE_URL   = "https://community.secop.gov.co"
PRINT_PATH = "/Public/Tendering/OpportunityDetail/PrintPDF"


def procesar_contrato(notice_uid: str,
                      carpeta: Path = Path("./docs_descargados")) -> dict:
    """
    Usa una única sesión requests para ambas peticiones, manteniendo
    las cookies de sesión y minimizando el tiempo entre obtener el mkey
    y usarlo para descargar el PDF.
    """

    url_proceso = (
        f"{BASE_URL}/Public/Tendering/OpportunityDetail/Index"
        f"?noticeUID={notice_uid}&isFromPublicArea=True"
    )

    # Referer exacto que el navegador usa al llamar PrintPDF
    referer_pdf = (
        f"{BASE_URL}/Public/Tendering/OpportunityDetail/Index"
        f"?noticeUID={notice_uid}"
        f"&isFromPublicArea=True&isModal=true&asPopupView=true"
    )

    # Sesión persistente: reutiliza la misma conexión TCP y las cookies
    sesion = requests.Session()
    sesion.cookies.update(COOKIES)
    sesion.headers.update(HEADERS_BASE)

    # ── Paso 1: cargar la página y extraer el mkey ────────────────────────
    print(f"[1/4] Cargando página del proceso...")
    resp_pagina = sesion.get(url_proceso, timeout=30)
    resp_pagina.raise_for_status()

    patron = r"PrintPDF[^?]*\?mkey=([\w\-]+)"
    match  = re.search(patron, resp_pagina.text)
    if not match:
        print("[error] mkey no encontrado en la página.")
        return {"notice_uid": notice_uid, "error": "mkey no encontrado"}

    mkey = match.group(1)
    print(f"[2/4] mkey extraído: {mkey}")

    # ── Paso 2: descargar el PDF inmediatamente ───────────────────────────
    url_pdf = f"{BASE_URL}{PRINT_PATH}?mkey={mkey}"

    # Actualizar Referer para la petición del PDF
    sesion.headers.update({"Referer": referer_pdf})

    print(f"[3/4] Descargando PDF...")
    t0       = time.time()
    resp_pdf = sesion.get(url_pdf, timeout=60)
    elapsed  = time.time() - t0

    print(f"      Status: {resp_pdf.status_code} | "
          f"Tamaño: {len(resp_pdf.content):,} bytes | "
          f"Tiempo: {elapsed:.2f}s | "
          f"Content-Type: {resp_pdf.headers.get('Content-Type', 'N/A')}")

    if len(resp_pdf.content) == 0:
        print("[error] El servidor devolvió respuesta vacía.")
        print("        Posibles causas:")
        print("        - mkey expirado (raro con sesión única, pero posible)")
        print("        - Cookies de sesión expiradas — renovar en el navegador")
        print("        - El servidor requiere que la petición venga de una")
        print("          navegación real con JavaScript (usar Playwright)")
        return {"notice_uid": notice_uid, "error": "PDF vacío"}

    if "pdf" not in resp_pdf.headers.get("Content-Type", "").lower():
        print(f"[error] Content-Type inesperado.")
        return {"notice_uid": notice_uid, "error": "Content-Type no es PDF"}

    # ── Paso 3: guardar y hashear ─────────────────────────────────────────
    carpeta.mkdir(parents=True, exist_ok=True)
    ruta_pdf = carpeta / f"{notice_uid}.pdf"
    ruta_pdf.write_bytes(resp_pdf.content)
    print(f"[4/4] PDF guardado: {ruta_pdf.name}")

    doc         = fitz.open(ruta_pdf)
    texto       = "\n".join(doc.load_page(i).get_text("text") for i in range(doc.page_count)) # type: ignore
    n_paginas   = doc.page_count
    doc.close()
    hash_sha256 = hashlib.sha256(ruta_pdf.read_bytes()).hexdigest()

    print(f"\n      Páginas  : {n_paginas}")
    print(f"      SHA-256  : {hash_sha256}")

    return {
        "notice_uid":    notice_uid,
        "mkey":          mkey,
        "ruta_local":    str(ruta_pdf),
        "n_paginas":     n_paginas,
        "hash_sha256":   hash_sha256,
        "texto_preview": texto[:400],
    }


if __name__ == "__main__":
    resultado = procesar_contrato("CO1.NTC.9089204")

    print(f"\n{'='*60}")
    print("RESULTADO FINAL:")
    for k, v in resultado.items():
        if k != "texto_preview":
            print(f"  {k}: {v}")