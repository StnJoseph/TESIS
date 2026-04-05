"""
Documento de prueba para descargar el PDF de un contrato del SECOP II usando Playwright.
Usa expect_download() para capturar el archivo que genera el botón Imprimir.

Tesis: Capa de Notarización Digital sobre SECOP II
Universidad de los Andes - Ingeniería de Sistemas y Computación
Joseph Steven Linares Gutierrez
2026

Dependencias:
    pip install playwright pymupdf
    python -m playwright install chromium
"""

import hashlib
import fitz
from pathlib import Path
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout


COOKIES_SESION = [
    {
        "name": "PublicSessionCookie", 
        "value": "jrmesebiat2mhaq4qg45xj4j",
        "domain": "community.secop.gov.co", 
        "path": "/"
    },
    {
        "name": "ROUTEID",             
        "value": ".fe_com_03",
        "domain": "community.secop.gov.co", 
        "path": "/"
    },
    {
        "name": "STSSessionCookie",    
        "value": "ffvva4wfs3ogdyd2v0i1qv4w",
        "domain": "community.secop.gov.co", 
        "path": "/"},
]

BASE_URL = "https://community.secop.gov.co"


def descargar_pdf_playwright(notice_uid: str,
                             carpeta: Path = Path("./docs_descargados"),
                             headless: bool = False) -> Path | None:

    url_proceso = (
        f"{BASE_URL}/Public/Tendering/OpportunityDetail/Index"
        f"?noticeUID={notice_uid}&isFromPublicArea=True"
    )

    carpeta.mkdir(parents=True, exist_ok=True)
    ruta_pdf = carpeta / f"{notice_uid}.pdf"

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/146.0.0.0 Safari/537.36"
            ),
            accept_downloads=True,
        )
        context.add_cookies(COOKIES_SESION) # type: ignore

        page = context.new_page()

        print(f"[1/3] Cargando página del proceso...")
        page.goto(url_proceso, wait_until="networkidle", timeout=30000)

        print(f"[2/3] Haciendo clic en Imprimir...")
        try:
            boton = page.locator("#btnTbPrint")
            boton.wait_for(state="visible", timeout=10000)

            # Intentar capturar como descarga directa primero
            try:
                with page.expect_download(timeout=8000) as dl_info:
                    boton.click()
                descarga = dl_info.value
                descarga.save_as(ruta_pdf)
                print(f"[3/3] Descarga directa exitosa: {ruta_pdf.name} "
                      f"({ruta_pdf.stat().st_size:,} bytes)")

            except PlaywrightTimeout:
                # No hubo descarga directa — el clic abre una nueva pestaña
                print("      Sin descarga directa — esperando nueva pestaña...")

                with context.expect_page(timeout=10000) as nueva_info:
                    boton.click()

                nueva = nueva_info.value
                nueva.wait_for_load_state("networkidle", timeout=30000)
                url_nueva = nueva.url
                print(f"      Nueva pestaña: {url_nueva}")

                # Intentar capturar descarga desde la nueva pestaña
                try:
                    with nueva.expect_download(timeout=8000) as dl_info:
                        # Si el PDF está embebido, forzar descarga via JS
                        nueva.evaluate("""
                            () => {
                                const a = document.createElement('a');
                                a.href = window.location.href;
                                a.download = 'contrato.pdf';
                                document.body.appendChild(a);
                                a.click();
                            }
                        """)
                    descarga = dl_info.value
                    descarga.save_as(ruta_pdf)
                    print(f"[3/3] PDF descargado desde nueva pestaña: "
                          f"{ruta_pdf.name} ({ruta_pdf.stat().st_size:,} bytes)")

                except PlaywrightTimeout:
                    # Último recurso: leer los bytes directamente con fetch
                    print("      Intentando fetch directo del PDF...")
                    pdf_bytes = nueva.evaluate("""
                        async () => {
                            const resp = await fetch(window.location.href, {
                                credentials: 'include'
                            });
                            const buf = await resp.arrayBuffer();
                            return Array.from(new Uint8Array(buf));
                        }
                    """)

                    if pdf_bytes and len(pdf_bytes) > 1000:
                        ruta_pdf.write_bytes(bytes(pdf_bytes))
                        print(f"[3/3] PDF obtenido via fetch: "
                              f"{ruta_pdf.name} ({len(pdf_bytes):,} bytes)")
                    else:
                        print("[error] fetch devolvió respuesta vacía o muy pequeña.")
                        nueva.screenshot(path="debug_nueva_pestana.png")
                        print("        Screenshot: debug_nueva_pestana.png")
                        browser.close()
                        return None

                nueva.close()

        except PlaywrightTimeout:
            print("[error] Timeout esperando el botón o la respuesta.")
            page.screenshot(path="debug_screenshot.png")
            browser.close()
            return None

        browser.close()

    if not ruta_pdf.exists() or ruta_pdf.stat().st_size == 0:
        print("[error] PDF vacío o no creado.")
        return None

    return ruta_pdf


def extraer_y_hashear(ruta_pdf: Path) -> dict:
    doc       = fitz.open(ruta_pdf)
    texto     = "\n".join(doc.load_page(i).get_text("text") for i in range(doc.page_count)) # type: ignore
    n_paginas = doc.page_count
    doc.close()

    hash_sha256 = hashlib.sha256(ruta_pdf.read_bytes()).hexdigest()

    print(f"\n      Páginas  : {n_paginas}")
    print(f"      SHA-256  : {hash_sha256}")
    print(f"      Preview  :\n{texto[:400]}")

    return {
        "n_paginas":     n_paginas,
        "hash_sha256":   hash_sha256,
        "texto_preview": texto[:400],
    }


def procesar_contrato(notice_uid: str,
                      carpeta: Path = Path("./docs_descargados"),
                      headless: bool = False) -> dict:

    print(f"\n{'='*60}")
    print(f"Procesando contrato: {notice_uid}")
    print(f"{'='*60}")

    ruta_pdf = descargar_pdf_playwright(notice_uid, carpeta, headless)
    if ruta_pdf is None:
        return {"notice_uid": notice_uid, "error": "Descarga fallida"}

    resultado = extraer_y_hashear(ruta_pdf)

    return {
        "notice_uid":  notice_uid,
        "ruta_local":  str(ruta_pdf),
        **resultado,
    }


if __name__ == "__main__":
    resultado = procesar_contrato("CO1.NTC.9089204", headless=False)

    print(f"\n{'='*60}")
    print("RESULTADO FINAL:")
    for k, v in resultado.items():
        if k != "texto_preview":
            print(f"  {k}: {v}")