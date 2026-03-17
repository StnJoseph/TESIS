import fitz  # PyMuPDF
from pathlib import Path

PDF_PATH = Path("C:/Users/josep/OneDrive/Documentos/Uniandes/9no/TESIS/proyecto/docs/ContratoPrueba.pdf")

if not PDF_PATH.exists():
    raise FileNotFoundError(f"No se encontró el archivo: {PDF_PATH.resolve()}")

doc = fitz.open(PDF_PATH)

print(f"Archivo: {PDF_PATH.name}")
print(f"Paginas: {doc.page_count}")

print("\n--- Texto de la primera página ---")
page0 = doc.load_page(0)  # índice basado en 0
text = page0.get_text("text")  # "text": extrae texto plano
print(text[:10000])  # muestra primeros 10000 caracteres

# Extraer enlaces (por ejemplo, si la página contiene links a documentos)
links = page0.get_links()
print(f"\nEnlaces detectados en la página 1: {len(links)}")
for i, ln in enumerate(links[:10], 1):
    print(f"{i}. {ln.get('uri')}")
    
doc.close()