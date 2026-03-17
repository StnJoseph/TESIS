import hashlib
import fitz  # PyMuPDF
from pathlib import Path

PDF_PATH = Path("C:/Users/josep/OneDrive/Documentos/Uniandes/9no/TESIS/proyecto/docs/ContratoPruebaA.pdf")

def abrir_pdf(ruta: Path) -> fitz.Document:
    if not ruta.exists():
        raise FileNotFoundError(f"No se encontró el archivo: {ruta.resolve()}")
    doc = fitz.open(ruta)
    print(f"Archivo : {ruta.name}")
    print(f"Páginas : {doc.page_count}")
    print(f"Tamaño  : {ruta.stat().st_size:,} bytes")
    return doc


def extraer_texto(doc: fitz.Document, preview_chars: int = 1000) -> str:
    """
    Extrae el texto completo del documento concatenando todas las páginas.
    Retorna el texto como string.
    """
    partes = []
    for i in range(doc.page_count):
        texto_pagina = doc.load_page(i).get_text("text")
        partes.append(texto_pagina)

    texto_completo = "\n".join(partes)

    print(f"\n--- Vista previa del texto extraído (primeros {preview_chars} caracteres) ---")
    print(texto_completo[:preview_chars])

    return texto_completo


def extraer_enlaces(doc: fitz.Document, max_paginas: int = 5) -> list[dict]:
    """
    Extrae los hipervínculos de las primeras páginas del documento.
    Útil para identificar anexos o referencias externas en el contrato.
    """
    enlaces = []
    for i in range(min(max_paginas, doc.page_count)):
        for enlace in doc.load_page(i).get_links():
            uri = enlace.get("uri")
            if uri:
                enlaces.append({"pagina": i + 1, "uri": uri})

    print(f"\n--- Enlaces detectados en las primeras {max_paginas} páginas: {len(enlaces)} ---")
    for e in enlaces[:10]:
        print(f"  Pág. {e['pagina']}: {e['uri']}")

    return enlaces


def calcular_hashes(ruta: Path, texto: str) -> dict:
    """
    Calcula dos variantes de hash SHA-256:

    - hash_binario  : sobre los bytes crudos del archivo PDF.
                      Recomendado para notarización: identifica el documento
                      de forma única incluyendo su estructura interna, metadatos
                      y formato. Dos PDFs con el mismo texto pero distinta
                      estructura producirán hashes distintos.

    Retorna un dict con ambos valores.
    """
    # Hash sobre bytes crudos del archivo
    bytes_pdf = ruta.read_bytes()
    hash_binario = hashlib.sha256(bytes_pdf).hexdigest()

    # Hash sobre texto extraído
    bytes_texto = texto.encode("utf-8")

    print("\n--- Hashes SHA-256 ---")
    print(f"  Binario (bytes del PDF) : {hash_binario}")

    return {
        "hash_binario": hash_binario,
    }
    

def inspeccionar_metadatos(ruta: Path) -> dict:
    doc = fitz.open(ruta)
    meta = doc.metadata
    doc.close()
    print(f"\n--- Metadatos: {ruta.name} ---")
    for k, v in meta.items(): # type: ignore
        print(f"  {k}: {v}")
    return meta # type: ignore



if __name__ == "__main__":
    doc    = abrir_pdf(PDF_PATH)
    texto  = extraer_texto(doc, preview_chars=1000)
    links  = extraer_enlaces(doc, max_paginas=5)
    hashes = calcular_hashes(PDF_PATH, texto)
    m1 = inspeccionar_metadatos(Path("C:/Users/josep/OneDrive/Documentos/Uniandes/9no/TESIS/proyecto/docs/ContratoPrueba.pdf"))
    m2 = inspeccionar_metadatos(Path("C:/Users/josep/OneDrive/Documentos/Uniandes/9no/TESIS/proyecto/docs/ContratoPruebaA.pdf"))
    doc.close()