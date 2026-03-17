from pypdf import PdfReader
import hashlib

reader = PdfReader("C:/Users/josep/OneDrive/Documentos/Uniandes/9no/TESIS/proyecto/docs/ContratoPruebaB.pdf")
parts = []
for i, page in enumerate(reader.pages):
    parts.append(page.extract_text() or "")
text_bytes = "\n".join(parts).encode("utf-8")
print("SHA-256 del texto extraído:", hashlib.sha256(text_bytes).hexdigest())