import os
import json
import logging
import uuid
import pyarrow as pa
import pyarrow.parquet as pq
import requests
import pandas as pd
import time
import numpy as np
from datetime import datetime
from typing import Tuple, Dict, List, Optional, Any
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
VAD_CACHE_PATH = os.path.join(BASE_DIR, "data", "vad_cache.json")

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
logger.info("INICIALIZANDO VALIDADOR SEMÂNTICO DGTM v5.2 + VAD")
logger.info("=" * 80)

# =============== ESTADO GLOBAL ===============
SCHEMA_STATE = {
    "dynamic_enums": {
        "emotion_values": [],
        "tone_values": []
    },
    "compatibility_matrix": {
        "emotion_tone": [],
        "intention_emotion": []
    },
    "vad_cache": {}
}

# =============== API VAD CONFIG ===============
VAD_SERVICES = [
    {
        "name": "OpenFeelings",
        "url": "https://api.openfeelings.org/vad?text={term}&lang=pt",
        "mapping": {
            "valence": "valence",
            "arousal": "arousal",
            "dominance": "dominance"
        }
    },
    {
        "name": "SenticAPI",
        "url": "https://sentic-api.com/api/vad?text={term}",
        "mapping": {
            "valence": "val",
            "arousal": "aro",
            "dominance": "dom"
        }
    }
]

# =============== FUNÇÕES VAD ===============
def load_vad_cache():
    """Carrega cache VAD do disco"""
    if os.path.exists(VAD_CACHE_PATH):
        try:
            with open(VAD_CACHE_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_vad_cache():
    """Salva cache VAD no disco"""
    try:
        with open(VAD_CACHE_PATH, "w", encoding="utf-8") as f:
            json.dump(SCHEMA_STATE["vad_cache"], f, ensure_ascii=False)
    except Exception as e:
        logger.error(f"Erro ao salvar cache VAD: {str(e)}")

def get_vad_values(term: str, item: dict, auditoria: list) -> Optional[Dict[str, float]]:
    """Obtém valores VAD de serviços externos com fallback para cache"""
    
    # Verificar cache primeiro
    if term in SCHEMA_STATE["vad_cache"]:
        auditoria.append(registrar_auditoria(
            item, "VAD_CACHE", "vad_values", 
            None, SCHEMA_STATE["vad_cache"][term], 
            "Valores VAD obtidos do cache"
        ))
        return SCHEMA_STATE["vad_cache"][term]
    
    # Tentar serviços externos
    for service in VAD_SERVICES:
        try:
            url = service["url"].format(term=term)
            auditoria.append(registrar_auditoria(
                item, "VAD_REQUEST", "vad_values", 
                None, None, 
                f"Consultando serviço: {service['name']}"
            ))
            
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                data = response.json()
                mapping = service["mapping"]
                
                vad_values = {
                    "valence": float(data[mapping["valence"]]),
                    "arousal": float(data[mapping["arousal"]]),
                    "dominance": float(data[mapping["dominance"]])
                }
                
                # Validar valores
                if all(0.0 <= v <= 1.0 for v in vad_values.values()):
                    # Atualizar cache
                    SCHEMA_STATE["vad_cache"][term] = vad_values
                    save_vad_cache()
                    
                    auditoria.append(registrar_auditoria(
                        item, "VAD_SUCCESS", "vad_values", 
                        None, vad_values, 
                        f"Valores VAD obtidos de {service['name']}"
                    ))
                    return vad_values
        except Exception as e:
            auditoria.append(registrar_auditoria(
                item, "VAD_ERROR", "vad_values", 
                None, None, 
                f"Erro no serviço {service['name']}: {str(e)}"
            ))
    
    # Fallback para modelo estatístico simples
    vad_values = calculate_fallback_vad(term, item, auditoria)
    if vad_values:
        SCHEMA_STATE["vad_cache"][term] = vad_values
        save_vad_cache()
        return vad_values
    
    return None

def calculate_fallback_vad(term: str, item: dict, auditoria: list) -> Dict[str, float]:
    """Calcula valores VAD aproximados baseados em características da palavra"""
    # Implementação simplificada baseada em:
    # - Comprimento da palavra
    # - Presença de caracteres fortes (r, t, p, etc)
    # - Número de sílabas
    # - Terminações comuns
    
    # Contar sílabas aproximadas
    syllable_count = max(1, len(term) // 3)
    
    # Verificar caracteres "fortes"
    strong_chars = sum(1 for c in term if c.lower() in 'rrtkpdgb')
    strong_ratio = strong_chars / len(term)
    
    # Verificar terminações
    soft_endings = ['o', 'a', 'e', 'm']
    hard_endings = ['r', 's', 'z']
    
    # Calcular componentes
    valence = 0.5
    arousal = 0.3 + min(0.7, syllable_count * 0.15)
    dominance = 0.4 + min(0.5, strong_ratio * 0.6)
    
    # Ajustar baseado em terminação
    if term[-1] in soft_endings:
        valence += 0.15
        arousal -= 0.1
    elif term[-1] in hard_endings:
        valence -= 0.1
        arousal += 0.15
        dominance += 0.1
    
    # Garantir limites
    vad_values = {
        "valence": max(0.0, min(1.0, valence)),
        "arousal": max(0.0, min(1.0, arousal)),
        "dominance": max(0.0, min(1.0, dominance))
    }
    
    auditoria.append(registrar_auditoria(
        item, "VAD_FALLBACK", "vad_values", 
        None, vad_values, 
        "Valores VAD calculados por fallback"
    ))
    
    return vad_values

def map_vad_to_dgtm_fields(vad_values: Dict[str, float], item: dict, auditoria: list):
    """Mapeia valores VAD para campos DGTM"""
    v = vad_values["valence"]
    a = vad_values["arousal"]
    d = vad_values["dominance"]
    
    # Mapear emoção
    emotion_map = {
        (0.7, 1.0, 0.7, 1.0): "joy",         # Alta valência + alta ativação
        (0.7, 1.0, 0.0, 0.3): "contentment",  # Alta valência + baixa ativação
        (0.0, 0.3, 0.7, 1.0): "anger",        # Baixa valência + alta ativação
        (0.0, 0.3, 0.0, 0.3): "sadness",     # Baixa valência + baixa ativação
        (0.3, 0.7, 0.7, 1.0): "excitement",  # Valência média + alta ativação
        (0.3, 0.7, 0.0, 0.3): "calm",        # Valência média + baixa ativação
    }
    
    emotion = "neutral"
    intensity = 0.0
    
    for (v_min, v_max, a_min, a_max), emo in emotion_map.items():
        if v_min <= v <= v_max and a_min <= a <= a_max:
            emotion = emo
            # Intensidade baseada na distância do ponto neutro (0.5, 0.5)
            intensity = min(1.0, np.sqrt((v - 0.5)**2 + (a - 0.5)**2) * 1.8)
            break
    
    # Registrar novo valor de emoção se necessário
    if emotion not in SCHEMA_STATE["dynamic_enums"]["emotion_values"]:
        SCHEMA_STATE["dynamic_enums"]["emotion_values"].append(emotion)
        auditoria.append(registrar_auditoria(
            item, "ENUM_DINAMICO", "emotion.value", 
            SCHEMA_STATE["dynamic_enums"]["emotion_values"], 
            SCHEMA_STATE["dynamic_enums"]["emotion_values"], 
            f"Novo valor de emoção: {emotion}"
        ))
    
    # Atualizar item
    item["emotion"] = {
        "value": emotion,
        "intensity": round(intensity, 2),
        "vad_source": "calculated"
    }
    
    # Mapear tom baseado na dominância
    if d > 0.7:
        tone = "authoritative"
    elif d > 0.5:
        tone = "assertive"
    elif d > 0.3:
        tone = "neutral"
    else:
        tone = "submissive"
    
    # Registrar novo valor de tom se necessário
    if tone not in SCHEMA_STATE["dynamic_enums"]["tone_values"]:
        SCHEMA_STATE["dynamic_enums"]["tone_values"].append(tone)
        auditoria.append(registrar_auditoria(
            item, "ENUM_DINAMICO", "tone.value", 
            SCHEMA_STATE["dynamic_enums"]["tone_values"], 
            SCHEMA_STATE["dynamic_enums"]["tone_values"], 
            f"Novo valor de tom: {tone}"
        ))
    
    item["tone"] = {
        "value": tone,
        "context_weight": round(d, 2),
        "vad_source": "calculated"
    }
    
    auditoria.append(registrar_auditoria(
        item, "VAD_MAPPING", "emotion/tone", 
        None, {"emotion": emotion, "tone": tone}, 
        "Campos mapeados a partir de VAD"
    ))

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
        "tipo": type(valor).__name__ if valor is not None else "NoneType",
        "status": "presente" if valor is not None else "ausente"
    }

def registrar_estado_completo(item: dict, acao: str) -> dict:
    """Registra o estado completo do item para auditoria"""
    return {
        "timestamp": datetime.now().isoformat(),
        "item_id": item.get("id", ""),
        "termo": item.get("term", ""),
        "acao": acao,
        "estado": item.copy()
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
    """Valida e corrige presença de forma canônica para homógrafos"""
    auditoria.append(gerar_log_campo(item, "homograph_key"))
    auditoria.append(gerar_log_campo(item, "canonical_form"))
    
    if "homograph_key" in item and not item.get("canonical_form"):
        # CORREÇÃO AUTOMÁTICA: Define canonical_form como o próprio termo
        termo = item.get("term", "")
        if termo:
            item["canonical_form"] = termo
            msg = f"Campo 'canonical_form' definido automaticamente como o termo: '{termo}'"
            auditoria.append(registrar_auditoria(item, "CORREÇÃO", "canonical_form", 
                                               None, termo, msg))
            return True, msg
        else:
            msg = "Homógrafos requerem 'canonical_form' mas o termo está vazio"
            auditoria.append(registrar_auditoria(item, "VALIDACAO", "homograph_key", 
                                               item.get("homograph_key"), item.get("homograph_key"), msg))
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
    """Cria estrutura inicial do item DGTM com valores padrão"""
    return {
        "id": str(uuid.uuid4()),
        "term": termo,
        "canonical_form": termo,  # Valor padrão para forma canônica
        "definition": "",
        "last_modified": datetime.now().isoformat(),
        "usage_context": {
            "domain": "general",
            "formality_level": 3
        },
        "emotion": {  # Estrutura padrão para emoção
            "value": "neutral",
            "intensity": 0.0
        },
        "tone": {  # Estrutura padrão para tom
            "value": "neutral",
            "context_weight": 1.0
        },
        "semantic_relations": [],  # Campo obrigatório adicionado
        "compatibility_matrix": {  # Campo obrigatório adicionado
            "emotion_tone": [],
            "intention_emotion": []
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
            
        if "semantic_evolution" not in item:
            evolucao_padrao = {
                "timeline": [{
                    "period": {"start": "2000-01-01"},
                    "dominant_meaning": "Significado técnico padrão"
                }]
            }
            auditoria.append(registrar_auditoria(item, "PADRAO_DOMINIO", "semantic_evolution", 
                                               None, evolucao_padrao, "Padrão técnico aplicado"))
            item["semantic_evolution"] = evolucao_padrao

# =============== NÚCLEO DE VALIDAÇÃO ===============
def validar_item_dgtm(item: dict, schema: dict, auditoria: list) -> Tuple[dict, list]:
    """Executa todas as validações no item DGTM com pré-validação"""
    erros = []
    alertas = []
    
    # PRÉ-VALIDAÇÃO: Garante estruturas obrigatórias
    campos_obrigatorios = ["emotion", "tone"]
    for campo in campos_obrigatorios:
        if campo not in item:
            # Cria estrutura padrão para campos obrigatórios ausentes
            if campo == "emotion":
                item[campo] = {"value": "neutral", "intensity": 0.0}
            else:  # tone
                item[campo] = {"value": "neutral", "context_weight": 1.0}
                
            auditoria.append(registrar_auditoria(
                item, "PRE_VALIDACAO", campo, None, item[campo], 
                f"Estrutura padrão adicionada para {campo}"
            ))
    
    # Garante canonical_form padrão se não existir
    if not item.get("canonical_form"):
        item["canonical_form"] = item["term"]
        auditoria.append(registrar_auditoria(
            item, "PRE_VALIDACAO", "canonical_form", None, item["term"],
            "Definido como termo principal por padrão"
        ))
    
    # 1. Log de campos antes da validação
    campos_principais = ["id", "term", "definition", "last_modified", 
                         "synonyms", "homograph_key", "validation_info"]
    
    for campo in campos_principais:
        auditoria.append(gerar_log_campo(item, campo))
    
    # 2. Aplicar padrões de domínio ANTES das validações
    aplicar_padroes_dominio(item, auditoria)
    
    # 3. Validações condicionais
    for validacao in [validar_homografos]:
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
    
    # 7. Adicionar curadoria se necessário (APÓS validação estrutural)
    score = item.get("validation_info", {}).get("validation_score", 1.0)
    
    # 8. Validação estrutural do schema
    try:
        validate(instance=item, schema=schema)
        auditoria.append(registrar_auditoria(item, "VALIDACAO_SCHEMA", "", 
                                           "", "", "Validação estrutural bem-sucedida"))
    except ValidationError as e:
        erro_principal = best_match([e]).message
        erros.append(f"Erro de schema: {erro_principal}")
        auditoria.append(registrar_auditoria(item, "ERRO_SCHEMA", "", 
                                           "", "", erro_principal))
    
    # Adicionar curadoria APÓS validação estrutural
    if score <= 0.6 and "curation_info" not in item:
        item["curation_info"] = {
            "curator": "validador_auto",
            "validation_date": datetime.now().isoformat(),
            "confidence": score,
            "notes": "Gerado automaticamente pelo validador semântico"
        }
        auditoria.append(registrar_auditoria(item, "CURADORIA", "curation_info", 
                                           None, item["curation_info"], "Curadoria automática aplicada"))

    # 9. Log do estado final do item
    auditoria.append(registrar_estado_completo(item, "ESTADO_FINAL"))
    
    return item, erros

# =============== FUNÇÃO DE CRIAÇÃO DE ITEM ===============
def criar_item_dgtm(termo: str, schema: dict, fontes_externas: list) -> Tuple[dict, dict, list]:
    """Gera e valida item DGTM completo"""
    log = {"termo": termo, "erros": [], "alertas": []}
    auditoria = []
    
    item = inicializar_item(termo)
    auditoria.append(registrar_auditoria(item, "CRIACAO", "item", None, item, "Item inicializado"))
    auditoria.append(registrar_estado_completo(item, "ESTADO_INICIAL"))
    
    try:
        # Obter definição externa
        definicao = obter_definicao_externa(termo, fontes_externas, item, auditoria)
        if definicao:
            item["definition"] = definicao
            auditoria.append(registrar_auditoria(item, "ATUALIZACAO", "definition", 
                                               "", definicao, "Definição externa aplicada"))
        
        # Obter valores VAD e mapear para campos DGTM
        vad_values = get_vad_values(termo, item, auditoria)
        if vad_values:
            map_vad_to_dgtm_fields(vad_values, item, auditoria)
        else:
            log["alertas"].append("Valores VAD não disponíveis - usando padrões")
            auditoria.append(registrar_auditoria(
                item, "VAD_FALLBACK", "emotion/tone", 
                None, None, 
                "Valores VAD não disponíveis - mantendo valores padrão"
            ))
        
        # Validar item
        item, erros = validar_item_dgtm(item, schema, auditoria)
        log["erros"] = erros
        
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
    
    # Carregar cache VAD
    SCHEMA_STATE["vad_cache"] = load_vad_cache()
    
    logger.info("=" * 80)
    logger.info("CONFIGURAÇÃO DE CAMINHOS")
    logger.info(f"Diretório de entrada: {INPUT_DIR}")
    logger.info(f"Schema de validação: {SCHEMA_PATH}")
    logger.info(f"Saída Parquet: {OUTPUT_FILE}")
    logger.info(f"Log de validação: {LOG_FILE}")
    logger.info(f"Auditoria detalhada: {AUDIT_FILE}")
    logger.info(f"Log de processamento: {PROCESS_LOG}")
    logger.info(f"Cache VAD: {VAD_CACHE_PATH} ({len(SCHEMA_STATE['vad_cache'])} termos em cache)")
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
        if "compatibility_matrix" in schema:
            SCHEMA_STATE["compatibility_matrix"] = schema["compatibility_matrix"]
        
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
    writer = None  # Inicializa o escritor como None
    
    try:
        with open(LOG_FILE, "w", encoding="utf-8") as log_file, \
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
                        "id": [item["id"]],
                        "term": [item["term"]],
                        "canonical_form": [item.get("canonical_form", "")],
                        "definition": [item.get("definition", "")],
                        "last_modified": [item["last_modified"]],
                        "usage_context_domain": [item.get("usage_context", {}).get("domain", "")],
                        "usage_context_subdomain": [item.get("usage_context", {}).get("subdomain", "")],
                        "emotion_value": [item.get("emotion", {}).get("value", "")],
                        "emotion_intensity": [item.get("emotion", {}).get("intensity", 0.0)],
                        "tone_value": [item.get("tone", {}).get("value", "")],
                        "tone_context_weight": [item.get("tone", {}).get("context_weight", 0.0)],
                        "intention_value": [item.get("intention", {}).get("value", "")],
                        "validation_score": [item.get("validation_info", {}).get("validation_score", 1.0)]
                    }
                    
                    # Se o escritor ainda não foi criado, crie-o
                    if writer is None:
                        writer = pq.ParquetWriter(OUTPUT_FILE, schema_parquet)
                    
                    # Escrever no Parquet
                    tabela = pa.Table.from_pydict(dados_parquet, schema=schema_parquet)
                    writer.write_table(tabela)
                    
                    # Definição do tamanho do lote para log e pausa
                    lote_log = 100
                    lote_pausa = 30000
                    if total_processados % lote_log == 0:
                        logger.info(f"{total_processados} palavras categorizadas até o momento.")
                    if total_processados % lote_pausa == 0 and total_processados != 0:
                        logger.info("Pausa de 30s após 30.000 palavras categorizadas para garantir integridade do Parquet. Pode interromper com segurança.")
                        time.sleep(30)
                except KeyboardInterrupt:
                    logger.warning("Execução interrompida pelo usuário. Fechando arquivos com segurança...")
                    break
                except Exception as e:
                    logger.exception(f"FALHA CRÍTICA NO TERMO: {termo}")
                    com_erros += 1
    except KeyboardInterrupt:
        logger.warning("Execução interrompida pelo usuário. Arquivos fechados com segurança.")
    finally:
        # Fechar o escritor Parquet se foi criado
        if writer is not None:
            writer.close()
        # Salvar cache VAD ao final
        save_vad_cache()

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
    logger.info(f"Cache VAD atualizado: {VAD_CACHE_PATH} ({len(SCHEMA_STATE['vad_cache'])} termos)")
    logger.info("=" * 80)

def gerar_parquet(dados, caminho_parquet):
    """
    Gera um arquivo Parquet a partir de uma lista de dicionários ou DataFrame.
    """
    if isinstance(dados, pd.DataFrame):
        df = dados
    else:
        df = pd.DataFrame(dados)
    df.to_parquet(caminho_parquet, index=False)
    print(f"Arquivo Parquet gerado em: {caminho_parquet}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nExecução interrompida pelo usuário. Fechando arquivos com segurança...")
        logger.info("Execução interrompida pelo usuário. Fechando arquivos com segurança...")