import spacy
import json
import os
from pathlib import Path
import fitz  # PyMuPDF
import logging

# === CONFIGURAÇÕES ===
PDF_FOLDER = Path(r"C:\Users\Lucas\Desktop\dgtm_project\src\estarcy\pdf")
ENTRADA_TXT = "entrada.txt"
SIMBOLOS_PATH = "simbolos.json"

# === LOGGING ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# === MODELO spaCy ===
nlp = spacy.load("pt_core_news_lg")

# === CARREGAR OU INICIAR SIMBOLÁRIO ===
if SIMBOLOS_PATH and Path(SIMBOLOS_PATH).exists():
    with open(SIMBOLOS_PATH, "r", encoding="utf-8") as f:
        simbolos = json.load(f)
    logging.info(f"Simbolário carregado de {SIMBOLOS_PATH} com {len(simbolos)} entradas.")
else:
    simbolos = {"_contador": 1}
    logging.info("Novo simbolário iniciado.")

if "_contador" not in simbolos:
    usados = [int(s[1:]) for s in simbolos.values() if isinstance(s, str) and s.startswith("#") and s[1:].isdigit()]
    simbolos["_contador"] = max(usados, default=1) + 1
    logging.info(f"Contador de símbolos ajustado para {simbolos['_contador'].}")

def gerar_simbolo(texto, categoria):
    chave = f"{categoria}:{texto.lower()}"
    if chave not in simbolos:
        simbolo = f"#{simbolos['_contador']:04d}"
        simbolos[chave] = simbolo
        simbolos['_contador'] += 1
        logging.info(f"Novo símbolo criado: {simbolo} para '{chave}'")
    return simbolos[chave]

def extrair_texto_dos_pdfs(pasta_pdf):
    textos = []
    for arquivo in pasta_pdf.glob("*.pdf"):
        logging.info(f"Lendo PDF: {arquivo}")
        doc = fitz.open(str(arquivo))
        for pagina in doc:
            textos.append(pagina.get_text())
    return "\n".join(textos)

def extrair_campos(sentenca):
    doc = nlp(sentenca)
    campos = []

    sujeito = next((t for t in doc if t.dep_ in ("nsubj", "nsubj:pass")), None)
    verbo = next((t for t in doc if t.dep_ == "ROOT"), None)
    objeto = next((t for t in doc if t.dep_ in ("obj", "dobj", "obl")), None)
    adjunto = next((t for t in doc if t.dep_ == "amod"), None)

    campos.append(gerar_simbolo(sujeito.text if sujeito else "desconhecido", "main_node"))
    campos.append(gerar_simbolo(verbo.text if verbo else "fazer", "sub_node"))
    campos.append(gerar_simbolo(objeto.text if objeto else "algo", "variable"))
    campos.append(gerar_simbolo(adjunto.text if adjunto else "neutro", "sub_variable"))

    cond = next((t for t in doc if t.text.lower() in ("se", "quando", "caso")), None)
    campos.append(gerar_simbolo(cond.text if cond else "nenhuma", "condition"))
    campos.append(gerar_simbolo("detalhe", "sub_condition"))

    tempo = "passado" if verbo and "Past" in verbo.morph.get("Tense") else "presente"
    campos.append(gerar_simbolo(tempo, "tense"))
    campos.append(gerar_simbolo("habitual" if any(t.text.lower() in ("sempre", "costuma") for t in doc) else "pontual", "aspect"))

    campos.append("080")  # intensidade
    campos.append("0100") # certeza
    campos.append("1" if any(t.text.lower() in ("não", "nunca") for t in doc) else "0")

    campos.append(gerar_simbolo("neutro", "emotion"))
    campos.append(gerar_simbolo("informar", "intention"))
    campos.append(gerar_simbolo("motivo-desconhecido", "cause"))
    campos.append(gerar_simbolo("efeito-desconhecido", "consequence"))

    campos.append(gerar_simbolo("afirmacao", "speech_act"))
    campos.append(gerar_simbolo("publico", "receiver"))
    campos.append(gerar_simbolo("neutro", "tone"))

    campos.append("1" if "se" in sentenca.lower() else "0")
    campos.append(gerar_simbolo("neutro", "memory_trace"))
    campos.append("1" if "eu" in sentenca.lower() else "0")

    # Campos expandidos (placeholders e heurística)
    campos.extend([
        gerar_simbolo("crença", "belief_state"),
        gerar_simbolo("desejo", "desire_state"),
        gerar_simbolo("valor", "value_alignment"),
        "0", "0",        # conflito moral / interno
        "50", "60",      # pressão / reflexão
        "2",             # estágio narrativo
        "70", "80",      # agência / responsabilidade
        gerar_simbolo("visão", "perception_modality"),
        gerar_simbolo("casa", "spatial_context"),
        gerar_simbolo("ontem", "temporal_context"),
        gerar_simbolo("irmão", "relational_context"),
        gerar_simbolo("estudante", "identity_aspect"),
        gerar_simbolo("liberdade", "symbolic_theme"),
        gerar_simbolo("respeito", "cultural_code"),
        "30", "40",      # urgência / risco
        "00001234"       # id da narrativa
    ])
    return campos

def processar_texto_pdf():
    logging.info("🔎 Extraindo texto dos PDFs...")
    texto_completo = extrair_texto_dos_pdfs(PDF_FOLDER)

    with open(ENTRADA_TXT, "w", encoding="utf-8") as f:
        f.write(texto_completo)
    logging.info(f"Texto extraído salvo em {ENTRADA_TXT}")

    frases = [linha.strip() for linha in texto_completo.split("\n") if linha.strip()]
    frases_unicas = list(dict.fromkeys(frases))  # Remove repetições mantendo ordem
    logging.info(f"{len(frases_unicas)} frases únicas encontradas para processamento.")

    linhas_dgtm = []
    for idx, frase in enumerate(frases_unicas, 1):
        campos = extrair_campos(frase)
        linhas_dgtm.append(" ".join(campos))
        logging.info(f"Extração: frase {idx}/{len(frases_unicas)} processada.")
        if idx % 100 == 0 or idx == len(frases_unicas):
            logging.info(f"{idx}/{len(frases_unicas)} frases processadas.")

    with open("dgtm_output.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(linhas_dgtm))
    logging.info(f"Saída DGTM salva em dgtm_output.txt")

    with open(SIMBOLOS_PATH, "w", encoding="utf-8") as f:
        json.dump(simbolos, f, ensure_ascii=False, indent=2)
    logging.info(f"Simbolário atualizado salvo em {SIMBOLOS_PATH}")

    logging.info(f"✅ Processamento concluído: {len(linhas_dgtm)} frases salvas em dgtm_output.txt")

# === EXECUÇÃO ===
if __name__ == "__main__":
    processar_texto_pdf()
