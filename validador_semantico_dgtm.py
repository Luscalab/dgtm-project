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
AUDIT_FILE = os.path.join(OUTPUT_DIR, "auditoria_detalhada.jsonl")
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
logger.info("=" * 80)
logger.info("INICIALIZANDO VALIDADOR SEMÂNTICO DGTM v5.2")
logger.info("=" * 80)

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

# =============== FUNÇÕES DE AUDITORIA ===============
def registrar_auditoria(item: dict, acao: str, campo: str = "", 
                         valor_antes: Any = None, valor_depois: Any = None, 
                         mensagem: str = "") -> dict:
    """Registra evento de auditoria detalhado"""
    return {
        "timestamp": datetime.now().isoformat(),
        "item_id": item.get("id", ""),
        "termo": item.get("term", ""),
        "acao": acao,
        "campo": campo,
        "valor_antes": valor_antes,
        "valor_depois": valor_depois,
        "mensagem": mensagem
    }

def gerar_log_campo(item: dict, campo: str, subcampo: str = "") -> dict:
    """Gera registro detalhado de um campo para auditoria"""
    caminho = campo
    valor = item.get(campo, None)
    
    if subcampo:
        caminho = f"{campo}.{subcampo}"
        if isinstance(item.get(campo, {}), dict):
            valor = item.get(campo, {}).get(subcampo, None)
    
    return {
        "campo": caminho,
        "valor": valor,
        "tipo": type(valor).__name__,
        "status": "presente" if valor is not None else "ausente"
    }

# =============== FUNÇÕES DE VALIDAÇÃO SEMÂNTICA ===============
def validar_intensidade_emocao(item: dict, auditoria: list) -> Tuple[bool, str]:
    """Valida regras de intensidade emocional conforme schema"""
    emocao = item.get("emotion", {}).get("value")
    intensidade = item.get("emotion", {}).get("intensity", 0.0)
    
    auditoria.append(gerar_log_campo(item, "emotion", "value"))
    auditoria.append(gerar_log_campo(item, "emotion", "intensity"))
    
    # Regra para neutralidade
    if emocao == "neutral" and intensidade > 0.3:
        msg = "Intensidade > 0.3 não permitida para neutralidade"
        auditoria.append(registrar_auditoria(item, "VALIDACAO", "emotion.intensity", intensidade, intensidade, msg))
        return False, msg
    
    # Regra para domínio literário
    dominio = item.get("usage_context", {}).get("domain", "")
    if dominio == "literary" and intensidade < 0.7:
        msg = "Domínio literário requer intensidade mínima de 0.7"
        auditoria.append(registrar_auditoria(item, "VALIDACAO", "emotion.intensity", intensidade, intensidade, msg))
        return False, msg
    
    return True, ""

def validar_formalidade_tom(item: dict, auditoria: list) -> Tuple[bool, str]:
    """Valida compatibilidade entre formalidade e tom"""
    contexto = item.get("usage_context", {})
    formalidade = contexto.get("formality_level", 0)
    tom = item.get("tone", {}).get("value", "")
    
    auditoria.append(gerar_log_campo(item, "usage_context", "formality_level"))
    auditoria.append(gerar_log_campo(item, "tone", "value"))
    
    # Regra geral de formalidade
    if formalidade >= 4 and tom in ["informal", "colloquial", "humorous"]:
        msg = f"Tom '{tom}' incoerente com formalidade ≥4"
        auditoria.append(registrar_auditoria(item, "VALIDACAO", "tone.value", tom, tom, msg))
        return False, msg
    
    # Regra específica para domínio jurídico
    if contexto.get("domain") == "legal" and tom not in ["formal", "authoritative", "technical"]:
        msg = f"Tom '{tom}' não permitido em contexto jurídico"
        auditoria.append(registrar_auditoria(item, "VALIDACAO", "tone.value", tom, tom, msg))
        return False, msg
    
    return True, ""

def validar_regras_dominio(item: dict, auditoria: list) -> Tuple[bool, str]:
    """Aplica regras específicas por domínio"""
    contexto = item.get("usage_context", {})
    dominio = contexto.get("domain", "")
    tom = item.get("tone", {}).get("value", "")
    emocao = item.get("emotion", {}).get("value", "")
    
    auditoria.append(gerar_log_campo(item, "usage_context", "domain"))
    
    erros = []
    
    # Regras para domínio técnico
    if dominio == "technical":
        # Log de campos específicos
        auditoria.append(gerar_log_campo(item, "tone", "value"))
        auditoria.append(gerar_log_campo(item, "emotion", "value"))
        auditoria.append(gerar_log_campo(item, "intention"))
        auditoria.append(gerar_log_campo(item, "semantic_evolution"))
        
        if tom in ["humorous", "colloquial"]:
            msg = f"Tom '{tom}' não permitido em contexto técnico"
            erros.append(msg)
            auditoria.append(registrar_auditoria(item, "VALIDACAO", "tone.value", tom, tom, msg))
            
        if emocao not in ["neutral", "precision", "clarity"]:
            msg = f"Emoção '{emocao}' não permitida em contexto técnico"
            erros.append(msg)
            auditoria.append(registrar_auditoria(item, "VALIDACAO", "emotion.value", emocao, emocao, msg))
            
        if "intention" not in item:
            msg = "Domínio técnico requer campo 'intention'"
            erros.append(msg)
            auditoria.append(registrar_auditoria(item, "VALIDACAO", "intention", None, None, msg))
            
        if "semantic_evolution" not in item:
            msg = "Domínio técnico requer campo 'semantic_evolution'"
            erros.append(msg)
            auditoria.append(registrar_auditoria(item, "VALIDACAO", "semantic_evolution", None, None, msg))
    
    # Regras para domínio jurídico
    elif dominio == "legal":
        formalidade = contexto.get("formality_level", 0)
        auditoria.append(gerar_log_campo(item, "usage_context", "formality_level"))
        
        if formalidade < 4:
            msg = "Nível de formalidade <4 não permitido em contexto jurídico"
            erros.append(msg)
            auditoria.append(registrar_auditoria(item, "VALIDACAO", "usage_context.formality_level", formalidade, formalidade, msg))
    
    return (True, "") if not erros else (False, "; ".join(erros))

def validar_compatibilidade_emocao_tom(item: dict, auditoria: list) -> Tuple[bool, str]:
    """Valida matriz de compatibilidade entre emoção e tom"""
    emocao = item.get("emotion", {}).get("value")
    tom = item.get("tone", {}).get("value")
    
    auditoria.append(gerar_log_campo(item, "emotion", "value"))
    auditoria.append(gerar_log_campo(item, "tone", "value"))
    
    if not emocao or not tom:
        return True, ""
    
    for regra in SCHEMA_STATE["compatibility_matrix"]["emotion_tone"]:
        if regra.get("emotion") == emocao:
            if tom not in regra.get("allowed_tones", []):
                msg = f"Incompatibilidade: {emocao} + {tom}"
                auditoria.append(registrar_auditoria(item, "VALIDACAO", "compatibilidade", 
                                                   f"{emocao}+{tom}", f"{emocao}+{tom}", msg))
                return False, msg
    
    return True, ""

def validar_compatibilidade_intencao_emocao(item: dict, auditoria: list) -> Tuple[bool, str]:
    """Valida matriz de compatibilidade entre intenção e emoção"""
    intencao = item.get("intention", {}).get("value")
    emocao = item.get("emotion", {}).get("value")
    
    auditoria.append(gerar_log_campo(item, "intention", "value"))
    auditoria.append(gerar_log_campo(item, "emotion", "value"))
    
    if not intencao or not emocao:
        return True, ""
    
    for regra in SCHEMA_STATE["compatibility_matrix"]["intention_emotion"]:
        if regra.get("intention") == intencao:
            if emocao not in regra.get("compatible_emotions", []):
                msg = f"Incompatibilidade: {intencao} + {emocao}"
                auditoria.append(registrar_auditoria(item, "VALIDACAO", "compatibilidade", 
                                                   f"{intencao}+{emocao}", f"{intencao}+{emocao}", msg))
                return False, msg
    
    return True, ""

def validar_homografos(item: dict, auditoria: list) -> Tuple[bool, str]:
    """Valida presença de forma canônica para homógrafos"""
    auditoria.append(gerar_log_campo(item, "homograph_key"))
    auditoria.append(gerar_log_campo(item, "canonical_form"))
    
    if "homograph_key" in item and not item.get("canonical_form"):
        msg = "Homógrafos requerem 'canonical_form'"
        auditoria.append(registrar_auditoria(item, "VALIDACAO", "homograph_key", 
                                           item.get("homograph_key"), item.get("homograph_key"), msg))
        return False, msg
    return True, ""

def validar_curacao(item: dict, auditoria: list) -> Tuple[bool, str]:
    """Valida necessidade de informações de curadoria"""
    auditoria.append(gerar_log_campo(item, "validation_info", "validation_score"))
    auditoria.append(gerar_log_campo(item, "curation_info"))
    
    score = item.get("validation_info", {}).get("validation_score", 1.0)
    if score <= 0.6 and "curation_info" not in item:
        msg = "Curadoria obrigatória para score ≤ 0.6"
        auditoria.append(registrar_auditoria(item, "VALIDACAO", "curation_info", None, None, msg))
        return False, msg
    return True, ""

def registrar_enum_dinamico(item: dict, campo: str, auditoria: list) -> Tuple[bool, str]:
    """Registra novos valores em enums dinâmicos"""
    valor = item.get(campo, {}).get("value")
    if not valor:
        return True, ""
    
    chave_enum = f"{campo}_values"
    valores_base = SCHEMA_STATE["dynamic_enums"].get(chave_enum, [])
    
    if valor not in valores_base:
        SCHEMA_STATE["dynamic_enums"][chave_enum].append(valor)
        msg = f"Novo valor '{valor}' em {campo}"
        auditoria.append(registrar_auditoria(item, "ENUM_DINAMICO", campo, valores_base, 
                                           SCHEMA_STATE["dynamic_enums"][chave_enum], msg))
        return True, msg
    
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

def obter_definicao_externa(termo: str, fontes: list, item: dict, auditoria: list) -> Optional[str]:
    """Obtém definição de fontes externas com fallback"""
    if not fontes:
        return None
        
    for fonte in fontes:
        try:
            url = fonte["url"].replace("{term}", termo)
            auditoria.append(registrar_auditoria(item, "CONSULTA_EXTERNA", "definition", 
                                               None, None, f"Consultando: {fonte['name']}"))
            
            resposta = requests.get(url, timeout=3)
            status = resposta.status_code
            
            if status == 200:
                definicao = resposta.text[:500]
                auditoria.append(registrar_auditoria(item, "CONSULTA_EXTERNA", "definition", 
                                                   item.get("definition"), definicao, "Sucesso"))
                return definicao
            else:
                auditoria.append(registrar_auditoria(item, "CONSULTA_EXTERNA", "definition", 
                                                   None, None, f"Erro HTTP {status}"))
                
        except Exception as e:
            auditoria.append(registrar_auditoria(item, "CONSULTA_EXTERNA", "definition", 
                                               None, None, f"Erro: {str(e)}"))
    return None

def aplicar_padroes_dominio(item: dict, auditoria: list):
    """Aplica padrões específicos por domínio"""
    dominio = item.get("usage_context", {}).get("domain", "")
    
    if dominio == "technical":
        if "tone" not in item:
            novo_tom = {"value": "technical", "context_weight": 0.9}
            auditoria.append(registrar_auditoria(item, "PADRAO_DOMINIO", "tone", 
                                               None, novo_tom, "Padrão técnico aplicado"))
            item["tone"] = novo_tom
            
        if "intention" not in item:
            nova_intencao = {"value": "inform", "certainty": 0.95}
            auditoria.append(registrar_auditoria(item, "PADRAO_DOMINIO", "intention", 
                                               None, nova_intencao, "Padrão técnico aplicado"))
            item["intention"] = nova_intencao

# =============== NÚCLEO DE VALIDAÇÃO ===============
def validar_item_dgtm(item: dict, schema: dict, auditoria: list) -> Tuple[dict, list]:
    """Executa todas as validações no item DGTM"""
    erros = []
    alertas = []
    
    # 1. Log de campos antes da validação
    campos_principais = ["id", "term", "definition", "last_modified", 
                         "synonyms", "homograph_key", "validation_info"]
    
    for campo in campos_principais:
        auditoria.append(gerar_log_campo(item, campo))
    
    # 2. Validação estrutural do schema
    try:
        validate(instance=item, schema=schema)
        auditoria.append(registrar_auditoria(item, "VALIDACAO_SCHEMA", "", 
                                           "", "", "Validação estrutural bem-sucedida"))
    except ValidationError as e:
        erro_principal = best_match([e]).message
        erros.append(f"Erro de schema: {erro_principal}")
        auditoria.append(registrar_auditoria(item, "ERRO_SCHEMA", "", 
                                           "", "", erro_principal))
    
    # 3. Validações condicionais
    for validacao in [validar_homografos, validar_curacao]:
        valido, mensagem = validacao(item, auditoria)
        if not valido:
            erros.append(mensagem)
    
    # 4. Validações semânticas
    validacoes_semanticas = [
        validar_intensidade_emocao,
        validar_formalidade_tom,
        validar_regras_dominio,
        validar_compatibilidade_emocao_tom,
        validar_compatibilidade_intencao_emocao
    ]
    
    for validacao in validacoes_semanticas:
        valido, mensagem = validacao(item, auditoria)
        if not valido:
            erros.append(mensagem)
    
    # 5. Registro de enums dinâmicos
    for campo in ["emotion", "tone"]:
        _, mensagem = registrar_enum_dinamico(item, campo, auditoria)
        if mensagem:
            alertas.append(mensagem)
    
    # 6. Cálculo do score de validação
    score = max(0.0, 1.0 - (len(erros) * 0.15))
    score_anterior = item.get("validation_info", {}).get("validation_score", 1.0)
    
    item.setdefault("validation_info", {}).update({
        "last_validated": datetime.now().isoformat(),
        "validation_score": round(score, 2),
        "warnings": alertas
    })
    
    if score_anterior != score:
        auditoria.append(registrar_auditoria(item, "ATUALIZACAO", "validation_info.validation_score", 
                                           score_anterior, score, f"Score atualizado: {score}"))
    
    # 7. Aplicar padrões de domínio
    aplicar_padroes_dominio(item, auditoria)
    
    return item, erros

# =============== FUNÇÃO DE CRIAÇÃO DE ITEM ===============
def criar_item_dgtm(termo: str, schema: dict, fontes_externas: list) -> Tuple[dict, dict, list]:
    """Gera e valida item DGTM completo"""
    log = {"termo": termo, "erros": [], "alertas": []}
    auditoria = []
    
    item = inicializar_item(termo)
    auditoria.append(registrar_auditoria(item, "CRIACAO", "item", None, item, "Item inicializado"))
    
    try:
        # Obter definição externa
        definicao = obter_definicao_externa(termo, fontes_externas, item, auditoria)
        if definicao:
            item["definition"] = definicao
            auditoria.append(registrar_auditoria(item, "ATUALIZACAO", "definition", 
                                               "", definicao, "Definição externa aplicada"))
        
        # Validar item
        item, erros = validar_item_dgtm(item, schema, auditoria)
        log["erros"] = erros
        
        # Adicionar curadoria se necessário
        if item["validation_info"]["validation_score"] <= 0.6:
            item["curation_info"] = {
                "curator": "validador_auto",
                "validation_date": datetime.now().isoformat(),
                "confidence": item["validation_info"]["validation_score"],
                "notes": "Gerado automaticamente pelo validador semântico"
            }
            auditoria.append(registrar_auditoria(item, "CURADORIA", "curation_info", 
                                               None, item["curation_info"], "Curadoria automática aplicada"))
            
    except Exception as e:
        logger.exception(f"Erro crítico no termo '{termo}'")
        log["erros"].append(f"ERRO CRÍTICO: {str(e)}")
        auditoria.append(registrar_auditoria(item, "ERRO_CRITICO", "", 
                                           None, None, f"Exceção: {str(e)}"))
    
    return item, log, auditoria

# =============== FUNÇÃO PRINCIPAL ===============
def main():
    """Fluxo principal de execução do validador"""
    # Garantir existência dos diretórios
    os.makedirs(INPUT_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(os.path.dirname(SCHEMA_PATH), exist_ok=True)
    
    logger.info("=" * 80)
    logger.info("CONFIGURAÇÃO DE CAMINHOS")
    logger.info(f"Diretório de entrada: {INPUT_DIR}")
    logger.info(f"Schema de validação: {SCHEMA_PATH}")
    logger.info(f"Saída Parquet: {OUTPUT_FILE}")
    logger.info(f"Log de validação: {LOG_FILE}")
    logger.info(f"Auditoria detalhada: {AUDIT_FILE}")
    logger.info("=" * 80)

    # Carregar schema com suporte a JSONC
    try:
        # Tentar carregadores com suporte a comentários
        try:
            import json5
            carregador = json5.load
            logger.info("Biblioteca JSON5 encontrada - suporte a comentários ativo")
        except ImportError:
            try:
                import commentjson
                carregador = commentjson.load
                logger.info("Biblioteca COMMENTJSON encontrada - suporte a comentários ativo")
            except ImportError:
                carregador = json.load
                logger.warning("Bibliotecas JSONC não encontradas - usando json padrão")
        
        with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
            schema = carregador(f)
        
        # Inicializar estado global
        SCHEMA_STATE["dynamic_enums"] = schema.get("dynamic_enums", {})
        SCHEMA_STATE["compatibility_matrix"] = schema.get("compatibility_matrix", {})
        
        # Configurar fontes externas
        fontes_externas = schema.get("x-external-sources", [])
        logger.info(f"Schema carregado: {len(fontes_externas)} fontes externas configuradas")
    except FileNotFoundError:
        logger.error(f"ERRO: Arquivo de schema não encontrado: {SCHEMA_PATH}")
        return
    except Exception as e:
        logger.error(f"ERRO AO CARREGAR SCHEMA: {str(e)}")
        return

    # Coletar termos para processamento
    termos = []
    arquivos_processados = []
    
    for arquivo in os.listdir(INPUT_DIR):
        if arquivo.lower().endswith((".txt", ".dic")):
            caminho = os.path.join(INPUT_DIR, arquivo)
            try:
                with open(caminho, "r", encoding="utf-8") as f:
                    termos_arquivo = [ln.strip() for ln in f if ln.strip()]
                    termos.extend(termos_arquivo)
                    arquivos_processados.append({
                        "arquivo": arquivo,
                        "termos": len(termos_arquivo)
                    })
                logger.info(f"Arquivo processado: {arquivo} ({len(termos_arquivo)} termos)")
            except Exception as e:
                logger.error(f"ERRO AO LER ARQUIVO {arquivo}: {str(e)}")
    
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
         open(LOG_FILE, "w", encoding="utf-8") as log_file, \
         open(AUDIT_FILE, "w", encoding="utf-8") as audit_file:
        
        for termo in termos:
            try:
                # Processar item
                item, log, auditoria = criar_item_dgtm(termo, schema, fontes_externas)
                total_processados += 1
                
                if log["erros"]:
                    com_erros += 1
                
                # Registrar log de validação
                registro_log = {
                    "timestamp": datetime.now().isoformat(),
                    "termo": termo,
                    "item_id": item["id"],
                    "dominio": item.get("usage_context", {}).get("domain", ""),
                    "score": item.get("validation_info", {}).get("validation_score", 1.0),
                    "erros": log["erros"],
                    "alertas": log.get("alertas", [])
                }
                log_file.write(json.dumps(registro_log, ensure_ascii=False) + "\n")
                
                # Registrar auditoria completa
                for evento in auditoria:
                    audit_file.write(json.dumps(evento, ensure_ascii=False) + "\n")
                
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
                    logger.info(f"Processados: {total_processados} termos | Último: {termo}")
                    
            except Exception:
                logger.exception(f"FALHA CRÍTICA NO TERMO: {termo}")
                com_erros += 1

    # Relatório final
    logger.info("=" * 80)
    logger.info("PROCESSO CONCLUÍDO")
    logger.info(f"Arquivos processados: {len(arquivos_processados)}")
    for arq in arquivos_processados:
        logger.info(f"  - {arq['arquivo']}: {arq['termos']} termos")
        
    logger.info(f"Total de termos: {len(termos)}")
    logger.info(f"Processados com sucesso: {total_processados - com_erros}")
    logger.info(f"Processados com erros: {com_erros}")
    logger.info(f"Taxa de sucesso: {(total_processados - com_erros)/max(1, total_processados)*100:.1f}%")
    logger.info("")
    logger.info(f"Arquivo de saída: {OUTPUT_FILE}")
    logger.info(f"Log de validação: {LOG_FILE}")
    logger.info(f"Auditoria detalhada: {AUDIT_FILE}")
    logger.info(f"Log de processamento: {PROCESS_LOG}")
    logger.info("=" * 80)

if __name__ == "__main__":
    main()