import fitz
from pathlib import Path

ruta = Path("./docs_descargados/CO1.NTC.8768333.pdf")  # usar cualquier archivo existente
doc = fitz.open(ruta)
texto = "\n".join(doc.load_page(i).get_text("text") for i in range(doc.page_count))
doc.close()
print(texto[:10000])