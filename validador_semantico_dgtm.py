import os
import json
import logging
import uuid
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from datetime import datetime
from typing import Tuple, Dict, List, Optional
from jsonschema import validate, ValidationError
from jsonschema.exceptions import best_match

# =============== CONFIGURAÇÃO DE CAMINHOS ===============
BASE_DIR = r"C:\Users\Lucas\Desktop\projeto-dgtm"
INPUT_DIR = os.path.join(BASE_DIR, "input")
SCHEMA_PATH = os.path.join(BASE_DIR, "data", "dgtm_fields_schema.jsonc")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "dgtm_categorizadas.parquet")
LOG_FILE = os.path.join(OUTPUT_DIR, "validacao_semantica.jsonl")
PROCESS_LOG = os.path.join(OUTPUT_DIR, "processamento.log")

# =============== CONFIGURAÇÃO DE LOGGING ===============
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(PROCESS_LOG, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger()

# =============== ESTADO GLOBAL DO SCHEMA ===============
SCHEMA_STATE = {
    "dynamic_enums": {
        "emotion_values": [],
        "tone_values": []
    },
    "compatibility_matrix": {
        "emotion_tone": [],
        "intention_emotion": []
    }
}

# =============== FUNÇÕES DE VALIDAÇÃO SEMÂNTICA ===============
def validar_intensidade_emocao(item: dict) -> Tuple[bool, str]:
    """Valida regras de intensidade emocional conforme schema"""
    emocao = item.get("emotion", {}).get("value")
    intensidade = item.get("emotion", {}).get("intensity", 0.0)
    
    # Regra para neutralidade
    if emocao == "neutral" and intensidade > 0.3:
        return False, "Intensidade > 0.3 não permitida para neutralidade"
    
    # Regra para domínio literário
    dominio = item.get("usage_context", {}).get("domain", "")
    if dominio == "literary" and intensidade < 0.7:
        return False, "Domínio literário requer intensidade mínima de 0.7"
    
    return True, ""

def validar_formalidade_tom(item: dict) -> Tuple[bool, str]:
    """Valida compatibilidade entre formalidade e tom"""
    contexto = item.get("usage_context", {})
    formalidade = contexto.get("formality_level", 0)
    tom = item.get("tone", {}).get("value", "")
    
    # Regra geral de formalidade
    if formalidade >= 4 and tom in ["informal", "colloquial", "humorous"]:
        return False, f"Tom '{tom}' incoerente com formalidade ≥4"
    
    # Regra específica para domínio jurídico
    if contexto.get("domain") == "legal" and tom not in ["formal", "authoritative", "technical"]:
        return False, f"Tom '{tom}' não permitido em contexto jurídico"
    
    return True, ""

def validar_regras_dominio(item: dict) -> Tuple[bool, str]:
    """Aplica regras específicas por domínio"""
    contexto = item.get("usage_context", {})
    dominio = contexto.get("domain", "")
    tom = item.get("tone", {}).get("value", "")
    emocao = item.get("emotion", {}).get("value", "")
    
    erros = []
    
    # Regras para domínio técnico
    if dominio == "technical":
        if tom in ["humorous", "colloquial"]:
            erros.append(f"Tom '{tom}' não permitido em contexto técnico")
        if emocao not in ["neutral", "precision", "clarity"]:
            erros.append(f"Emoção '{emocao}' não permitida em contexto técnico")
        if "intention" not in item:
            erros.append("Domínio técnico requer campo 'intention'")
        if "semantic_evolution" not in item:
            erros.append("Domínio técnico requer campo 'semantic_evolution'")
    
    # Regras para domínio jurídico
    elif dominio == "legal":
        formalidade = contexto.get("formality_level", 0)
        if formalidade < 4:
            erros.append("Nível de formalidade <4 não permitido em contexto jurídico")
    
    return (True, "") if not erros else (False, "; ".join(erros))

def validar_compatibilidade_emocao_tom(item: dict) -> Tuple[bool, str]:
    """Valida matriz de compatibilidade entre emoção e tom"""
    emocao = item.get("emotion", {}).get("value")
    tom = item.get("tone", {}).get("value")
    
    if not emocao or not tom:
        return True, ""
    
    for regra in SCHEMA_STATE["compatibility_matrix"]["emotion_tone"]:
        if regra.get("emotion") == emocao:
            if tom not in regra.get("allowed_tones", []):
                return False, f"Incompatibilidade: {emocao} + {tom}"
    
    return True, ""

def validar_compatibilidade_intencao_emocao(item: dict) -> Tuple[bool, str]:
    """Valida matriz de compatibilidade entre intenção e emoção"""
    intencao = item.get("intention", {}).get("value")
    emocao = item.get("emotion", {}).get("value")
    
    if not intencao or not emocao:
        return True, ""
    
    for regra in SCHEMA_STATE["compatibility_matrix"]["intention_emotion"]:
        if regra.get("intention") == intencao:
            if emocao not in regra.get("compatible_emotions", []):
                return False, f"Incompatibilidade: {intencao} + {emocao}"
    
    return True, ""

def validar_homografos(item: dict) -> Tuple[bool, str]:
    """Valida presença de forma canônica para homógrafos"""
    if "homograph_key" in item and not item.get("canonical_form"):
        return False, "Homógrafos requerem 'canonical_form'"
    return True, ""

def validar_curacao(item: dict) -> Tuple[bool, str]:
    """Valida necessidade de informações de curadoria"""
    score = item.get("validation_info", {}).get("validation_score", 1.0)
    if score <= 0.6 and "curation_info" not in item:
        return False, "Curadoria obrigatória para score ≤ 0.6"
    return True, ""

def registrar_enum_dinamico(item: dict, campo: str) -> Tuple[bool, str]:
    """Registra novos valores em enums dinâmicos"""
    valor = item.get(campo, {}).get("value")
    if not valor:
        return True, ""
    
    chave_enum = f"{campo}_values"
    valores_base = SCHEMA_STATE["dynamic_enums"].get(chave_enum, [])
    
    if valor not in valores_base:
        SCHEMA_STATE["dynamic_enums"][chave_enum].append(valor)
        return True, f"Novo valor '{valor}' em {campo}"
    
    return True, ""

# =============== FUNÇÕES AUXILIARES ===============
def inicializar_item(termo: str) -> dict:
    """Cria estrutura inicial do item DGTM"""
    return {
        "id": str(uuid.uuid4()),
        "term": termo,
        "definition": "",
        "last_modified": datetime.now().isoformat(),
        "usage_context": {
            "domain": "general",
            "formality_level": 3
        },
        "dynamic_enums": {
            "emotion_values": SCHEMA_STATE["dynamic_enums"]["emotion_values"].copy(),
            "tone_values": SCHEMA_STATE["dynamic_enums"]["tone_values"].copy()
        }
    }

def obter_definicao_externa(termo: str, fontes: list) -> Optional[str]:
    """Obtém definição de fontes externas com fallback"""
    for fonte in fontes:
        try:
            url = fonte["url"].replace("{term}", termo)
            resposta = requests.get(url, timeout=3)
            if resposta.status_code == 200:
                return resposta.text[:500]  # Limite seguro para definições
        except Exception:
            continue
    return None

def aplicar_padroes_dominio(item: dict):
    """Aplica padrões específicos por domínio"""
    dominio = item.get("usage_context", {}).get("domain", "")
    
    if dominio == "technical":
        if "tone" not in item:
            item["tone"] = {"value": "technical", "context_weight": 0.9}
        if "intention" not in item:
            item["intention"] = {"value": "inform", "certainty": 0.95}

# =============== NÚCLEO DE VALIDAÇÃO ===============
def validar_item_dgtm(item: dict, schema: dict) -> Tuple[dict, list]:
    """Executa todas as validações no item DGTM"""
    erros = []
    alertas = []
    
    # Validação estrutural do schema
    try:
        validate(instance=item, schema=schema)
    except ValidationError as e:
        erro_principal = best_match([e]).message
        erros.append(f"Erro de schema: {erro_principal}")
    
    # Validações condicionais
    for validacao in [validar_homografos, validar_curacao]:
        valido, mensagem = validacao(item)
        if not valido:
            erros.append(mensagem)
    
    # Validações semânticas
    validacoes_semanticas = [
        validar_intensidade_emocao,
        validar_formalidade_tom,
        validar_regras_dominio,
        validar_compatibilidade_emocao_tom,
        validar_compatibilidade_intencao_emocao
    ]
    
    for validacao in validacoes_semanticas:
        valido, mensagem = validacao(item)
        if not valido:
            erros.append(mensagem)
    
    # Registro de enums dinâmicos
    for campo in ["emotion", "tone"]:
        _, mensagem = registrar_enum_dinamico(item, campo)
        if mensagem:
            alertas.append(mensagem)
    
    # Cálculo do score de validação
    score = max(0.0, 1.0 - (len(erros) * 0.15))
    item.setdefault("validation_info", {}).update({
        "last_validated": datetime.now().isoformat(),
        "validation_score": round(score, 2),
        "warnings": alertas
    })
    
    # Aplicar padrões de domínio
    aplicar_padroes_dominio(item)
    
    return item, erros

# =============== FUNÇÃO DE CRIAÇÃO DE ITEM ===============
def criar_item_dgtm(termo: str, schema: dict, fontes_externas: list) -> Tuple[dict, dict]:
    """Gera e valida item DGTM completo"""
    log = {"termo": termo, "erros": [], "alertas": []}
    item = inicializar_item(termo)
    
    try:
        # Obter definição externa
        definicao = obter_definicao_externa(termo, fontes_externas)
        if definicao:
            item["definition"] = definicao
        
        # Validar item
        item, erros = validar_item_dgtm(item, schema)
        log["erros"] = erros
        
        # Adicionar curadoria se necessário
        if item["validation_info"]["validation_score"] <= 0.6:
            item["curation_info"] = {
                "curator": "validador_auto",
                "validation_date": datetime.now().isoformat(),
                "confidence": item["validation_info"]["validation_score"],
                "notes": "Gerado automaticamente pelo validador semântico"
            }
    except Exception as e:
        logger.exception(f"Erro crítico no termo '{termo}'")
        log["erros"].append(f"ERRO CRÍTICO: {str(e)}")
    
    return item, log

# =============== FUNÇÃO PRINCIPAL ===============
def main():
    """Fluxo principal de execução do validador"""
    # Garantir existência dos diretórios
    os.makedirs(INPUT_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(os.path.dirname(SCHEMA_PATH), exist_ok=True)
    
    logger.info("=" * 60)
    logger.info("INICIANDO VALIDADOR SEMÂNTICO DGTM v5.2")
    logger.info(f"Diretório de entrada: {INPUT_DIR}")
    logger.info(f"Schema de validação: {SCHEMA_PATH}")
    logger.info(f"Saída Parquet: {OUTPUT_FILE}")
    logger.info(f"Log de validação: {LOG_FILE}")
    logger.info("=" * 60)

    # Carregar schema com suporte a JSONC
    try:
        # Tentar carregadores com suporte a comentários
        try:
            import json5
            carregador = json5.load
            logger.info("Usando json5 para JSONC")
        except ImportError:
            try:
                import commentjson
                carregador = commentjson.load
                logger.info("Usando commentjson para JSONC")
            except ImportError:
                carregador = json.load
                logger.warning("Usando json padrão - comentários serão ignorados")
        
        with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
            schema = carregador(f)
        
        # Inicializar estado global
        SCHEMA_STATE["dynamic_enums"] = schema.get("dynamic_enums", {})
        SCHEMA_STATE["compatibility_matrix"] = schema.get("compatibility_matrix", {})
        
        # Configurar fontes externas
        fontes_externas = schema.get("x-external-sources", [])
        logger.info(f"Schema carregado: {len(fontes_externas)} fontes externas configuradas")
    except FileNotFoundError:
        logger.error(f"ARQUIVO DE SCHEMA NÃO ENCONTRADO: {SCHEMA_PATH}")
        return
    except Exception as e:
        logger.error(f"ERRO NO SCHEMA: {str(e)}")
        return

    # Coletar termos para processamento
    termos = []
    for arquivo in os.listdir(INPUT_DIR):
        if arquivo.lower().endswith((".txt", ".dic")):
            caminho = os.path.join(INPUT_DIR, arquivo)
            try:
                with open(caminho, "r", encoding="utf-8") as f:
                    termos.extend([ln.strip() for ln in f if ln.strip()])
                logger.info(f"Arquivo processado: {arquivo} ({len(termos)} termos)")
            except Exception as e:
                logger.error(f"Erro ao ler {arquivo}: {str(e)}")
    
    if not termos:
        logger.warning("NENHUM TERMO ENCONTRADO PARA PROCESSAMENTO")
        return

    # Configurar schema Parquet
    schema_parquet = pa.schema([
        ("id", pa.string()),
        ("term", pa.string()),
        ("canonical_form", pa.string()),
        ("definition", pa.string()),
        ("last_modified", pa.string()),
        ("usage_context_domain", pa.string()),
        ("usage_context_subdomain", pa.string()),
        ("emotion_value", pa.string()),
        ("emotion_intensity", pa.float32()),
        ("tone_value", pa.string()),
        ("tone_context_weight", pa.float32()),
        ("intention_value", pa.string()),
        ("validation_score", pa.float32())
    ])

    # Processar termos
    total_processados = 0
    com_erros = 0
    
    with pq.ParquetWriter(OUTPUT_FILE, schema_parquet, compression="ZSTD") as writer, \
         open(LOG_FILE, "w", encoding="utf-8") as log_file:
        
        for termo in termos:
            try:
                item, log = criar_item_dgtm(termo, schema, fontes_externas)
                total_processados += 1
                
                if log["erros"]:
                    com_erros += 1
                
                # Registrar log estruturado
                registro = {
                    "timestamp": datetime.now().isoformat(),
                    "termo": termo,
                    "item_id": item["id"],
                    "dominio": item.get("usage_context", {}).get("domain", ""),
                    "score": item.get("validation_info", {}).get("validation_score", 1.0),
                    "erros": log["erros"],
                    "alertas": log.get("alertas", [])
                }
                log_file.write(json.dumps(registro, ensure_ascii=False) + "\n")
                
                # Preparar dados para Parquet
                dados_parquet = {
                    "id": item["id"],
                    "term": item["term"],
                    "canonical_form": item.get("canonical_form", ""),
                    "definition": item.get("definition", ""),
                    "last_modified": item["last_modified"],
                    "usage_context_domain": item.get("usage_context", {}).get("domain", ""),
                    "usage_context_subdomain": item.get("usage_context", {}).get("subdomain", ""),
                    "emotion_value": item.get("emotion", {}).get("value", ""),
                    "emotion_intensity": item.get("emotion", {}).get("intensity", 0.0),
                    "tone_value": item.get("tone", {}).get("value", ""),
                    "tone_context_weight": item.get("tone", {}).get("context_weight", 0.0),
                    "intention_value": item.get("intention", {}).get("value", ""),
                    "validation_score": item.get("validation_info", {}).get("validation_score", 1.0)
                }
                
                # Escrever no Parquet
                tabela = pa.Table.from_pydict(dados_parquet, schema=schema_parquet)
                writer.write_table(tabela)
                
                # Log interativo
                if total_processados % 100 == 0:
                    logger.info(f"Processados: {total_processados} | Último: {termo}")
                    
            except Exception:
                logger.exception(f"FALHA NO TERMO: {termo}")
                com_erros += 1

    # Relatório final
    logger.info("=" * 60)
    logger.info("PROCESSO CONCLUÍDO")
    logger.info(f"Total de termos: {len(termos)}")
    logger.info(f"Processados com sucesso: {total_processados - com_erros}")
    logger.info(f"Processados com erros: {com_erros}")
    logger.info(f"Arquivo de saída: {OUTPUT_FILE}")
    logger.info(f"Log de validação: {LOG_FILE}")
    logger.info("=" * 60)

if __name__ == "__main__":
    main()