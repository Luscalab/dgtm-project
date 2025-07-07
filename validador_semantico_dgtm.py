import os
import json
import logging
import uuid
import re
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from datetime import datetime
from typing import Tuple, Dict, List, Any, Optional, Union
from jsonschema import validate, ValidationError
from jsonschema.exceptions import best_match

# Configuração de caminhos
BASE_DIR = r"C:\Users\Lucas\Desktop\projeto-dgtm"
INPUT_DIR = os.path.join(BASE_DIR, "input")
SCHEMA_PATH = os.path.join(BASE_DIR, "data", "dgtm_fields_schema.jsonc")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "dgtm_output.parquet")
LOG_FILE = os.path.join(OUTPUT_DIR, "validation_log.jsonl")

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(OUTPUT_DIR, "processamento.log")),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger()

# Estrutura de dados para manter o estado do schema
SCHEMA_STATE = {
    "dynamic_enums": {
        "emotion_values": [],
        "tone_values": []
    },
    "compatibility_matrix": {
        "emotion_tone": [],
        "intention_emotion": []
    },
    "homographs_index": {},
    "unique_index": {}
}

# ----------- Funções de validação semântica -----------
def validate_synonym_semantic_consistency(item: dict, all_items: dict) -> Tuple[bool, str]:
    """Verifica consistência semântica entre o item e seus sinônimos"""
    if not item.get("synonyms"):
        return True, ""
    
    warnings = []
    main_emotion = item.get("emotion", {}).get("value", "neutral")
    main_tone = item.get("tone", {}).get("value", "neutral")
    main_domain = item.get("usage_context", {}).get("domain", "general")
    
    for synonym in item["synonyms"]:
        term_id = synonym["term_id"]
        if term_id not in all_items:
            continue
            
        ref_item = all_items[term_id]
        ref_emotion = ref_item.get("emotion", {}).get("value", "neutral")
        ref_tone = ref_item.get("tone", {}).get("value", "neutral")
        ref_domain = ref_item.get("usage_context", {}).get("domain", "general")
        
        # Verificar consistência emocional
        if main_emotion != ref_emotion:
            warnings.append(f"Sinônimo {term_id} tem emoção diferente ({ref_emotion} vs {main_emotion})")
        
        # Verificar consistência de tom
        if main_tone != ref_tone:
            warnings.append(f"Sinônimo {term_id} tem tom diferente ({ref_tone} vs {main_tone})")
        
        # Verificar consistência de domínio
        if main_domain != ref_domain:
            warnings.append(f"Sinônimo {term_id} tem domínio diferente ({ref_domain} vs {main_domain})")
    
    return True, " | ".join(warnings) if warnings else ""

def validate_emotion_values(item: dict) -> Tuple[bool, str]:
    emotion = item.get("emotion", {}).get("value")
    if not emotion:
        return True, ""
    
    allowed_emotions = [
        "joy", "anger", "despair", "excitement", 
        "cultural_melancholy", "frenzy", "neutral"
    ]
    
    if emotion not in allowed_emotions:
        return False, f"Valor de emoção inválido: '{emotion}'"
    
    return True, ""

def validate_datetime_format(date_str: str) -> bool:
    pattern = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})?$"
    return bool(re.match(pattern, date_str))

def validate_semantic_evolution(item: dict) -> Tuple[bool, str]:
    evolution = item.get("semantic_evolution", {})
    timeline = evolution.get("timeline", [])
    
    for entry in timeline:
        period = entry.get("period", {})
        start = period.get("start", "")
        end = period.get("end", "")
        
        if not re.match(r"^\d{4}-\d{2}-\d{2}$", start):
            return False, f"Formato inválido para data de início: {start}"
            
        if end and not re.match(r"^\d{4}-\d{2}-\d{2}$", end):
            return False, f"Formato inválido para data de término: {end}"
            
    return True, ""

def validate_emotion_intensity(item: dict) -> Tuple[bool, str]:
    emotion = item.get("emotion", {}).get("value")
    intensity = item.get("emotion", {}).get("intensity", 0)
    
    if emotion == "neutral" and intensity > 0.3:
        return False, "Intensidade > 0.3 não permitida para neutralidade"
    
    domain = item.get("usage_context", {}).get("domain", "")
    if domain == "literary" and intensity < 0.7:
        return False, "Domínio literário requer intensidade mínima de 0.7"
    
    emotion_pattern = ["joy", "anger", "despair", "excitement", "cultural_melancholy", "frenzy"]
    if emotion in emotion_pattern and intensity is None:
        return False, f"Emoção '{emotion}' requer campo 'intensity'"
    
    return True, ""

def validate_tone_for_domain(item: dict) -> Tuple[bool, str]:
    usage_context = item.get("usage_context", {})
    domain = usage_context.get("domain", "")
    tone = item.get("tone", {}).get("value", "")
    
    if domain == "legal" and tone not in ["formal", "authoritative", "technical"]:
        return False, f"Tom '{tone}' não permitido em contexto jurídico"
    
    if domain == "technical" and tone in ["humorous", "colloquial"]:
        return False, f"Tom '{tone}' não permitido em contexto técnico"
    
    return True, ""

def validate_domain_specific_rules(item: dict) -> Tuple[bool, str]:
    context = item.get("usage_context", {})
    domain = context.get("domain", "")
    
    if domain == "technical":
        if "intention" not in item or not item["intention"].get("value"):
            return False, "Domínio técnico requer campo 'intention'"
        if "semantic_evolution" not in item or not item["semantic_evolution"].get("timeline"):
            return False, "Domínio técnico requer campo 'semantic_evolution'"
    
    return True, ""

def validate_emotion_tone_compatibility(item: dict) -> Tuple[bool, str]:
    emotion = item.get("emotion", {}).get("value")
    tone = item.get("tone", {}).get("value")
    
    if not emotion or not tone:
        return True, ""
    
    matrix = SCHEMA_STATE["compatibility_matrix"]["emotion_tone"]
    for rule in matrix:
        if rule.get("emotion") == emotion:
            allowed_tones = rule.get("allowed_tones", [])
            if tone not in allowed_tones:
                return False, f"Incompatibilidade: emoção '{emotion}' não pode combinar com tom '{tone}'"
    
    return True, ""

def validate_intention_emotion_compatibility(item: dict) -> Tuple[bool, str]:
    intention = item.get("intention", {}).get("value")
    emotion = item.get("emotion", {}).get("value")
    
    if not intention or not emotion:
        return True, ""
    
    matrix = SCHEMA_STATE["compatibility_matrix"]["intention_emotion"]
    for rule in matrix:
        if rule.get("intention") == intention:
            compatible_emotions = rule.get("compatible_emotions", [])
            if emotion not in compatible_emotions:
                return False, f"Incompatibilidade: intenção '{intention}' não pode combinar com emoção '{emotion}'"
    
    return True, ""

def validate_homograph(item: dict) -> Tuple[bool, str]:
    if item.get("homograph_key") and not item.get("canonical_form"):
        return False, "Campo 'canonical_form' obrigatório quando 'homograph_key' está presente"
    
    context = item.get("usage_context", {})
    domain = context.get("domain", "")
    subdomain = context.get("subdomain", "")
    term = item["term"]
    
    context_key = (term, domain, subdomain)
    
    if context_key in SCHEMA_STATE["homographs_index"]:
        existing_canonical = SCHEMA_STATE["homographs_index"][context_key]
        if existing_canonical == item["canonical_form"]:
            return False, f"Conflito de homônimo: canonical_form duplicado para '{term}' neste contexto"
    
    SCHEMA_STATE["homographs_index"][context_key] = item["canonical_form"]
    return True, ""

def validate_curation(item: dict) -> Tuple[bool, str]:
    validation_info = item.get("validation_info", {})
    validation_score = validation_info.get("validation_score", 1.0)
    
    if validation_score <= 0.6 and "curation_info" not in item:
        return False, "Campo 'curation_info' obrigatório quando 'validation_score' <= 0.6"
    
    return True, ""

def validate_dynamic_enum(item: dict, field: str) -> Tuple[bool, str]:
    value = item.get(field, {}).get("value")
    if not value:
        return True, ""
    
    enum_type = f"{field}_values"
    base_values = SCHEMA_STATE["dynamic_enums"].get(enum_type, [])
    
    if value not in base_values:
        SCHEMA_STATE["dynamic_enums"][enum_type].append(value)
        return True, f"Novo valor '{value}' registrado em {field}"
    
    return True, ""

def validate_uuid(uuid_str: str) -> bool:
    try:
        uuid.UUID(uuid_str)
        return True
    except ValueError:
        return False

def validate_synonyms(item: dict, all_items: dict) -> Tuple[bool, str]:
    synonyms = item.get("synonyms", [])
    warnings = []
    
    for synonym in synonyms:
        term_id = synonym["term_id"]
        
        if not validate_uuid(term_id):
            return False, f"ID de sinônimo inválido: {term_id}"
        
        if term_id not in all_items:
            return False, f"Sinônimo referencia item inexistente: {term_id}"
        
        relationship = synonym.get("relationship", "")
        valid_relationships = ["exact", "near", "contextual"]
        if relationship not in valid_relationships:
            return False, f"Relação de sinônimo inválida: {relationship}"
        
        ref_domain = all_items[term_id].get("usage_context", {}).get("domain", "")
        current_domain = item.get("usage_context", {}).get("domain", "")
        if ref_domain != current_domain:
            warnings.append(f"Sinônimo {term_id} pertence a domínio diferente")
        
        if relationship == "exact":
            ref_canonical = all_items[term_id].get("canonical_form", "")
            current_canonical = item.get("canonical_form", "")
            if ref_canonical != current_canonical:
                return False, f"Sinônimo exato {term_id} tem canonical_form diferente"
    
    return True, " | ".join(warnings) if warnings else ""

def check_unique_term(item: dict) -> Tuple[bool, str]:
    term = item["term"]
    domain = item["usage_context"]["domain"]
    subdomain = item["usage_context"].get("subdomain", "")
    homograph_key = item.get("homograph_key", "")
    
    unique_key = (term, domain, subdomain, homograph_key)
    
    if unique_key in SCHEMA_STATE["unique_index"]:
        existing_id = SCHEMA_STATE["unique_index"][unique_key]
        return False, f"Termo já existe neste contexto: ID {existing_id}"
    
    SCHEMA_STATE["unique_index"][unique_key] = item["id"]
    return True, ""

# ----------- Funções auxiliares -----------
def initialize_item(term: str) -> dict:
    return {
        "id": str(uuid.uuid4()),
        "term": term,
        "canonical_form": term,
        "homograph_key": "",
        "definition": "",  # Inicializado vazio para busca posterior
        "last_modified": datetime.now().isoformat(),
        "synonyms": [],
        "usage_context": {
            "domain": "general",
            "subdomain": ""
        },
        "emotion": {
            "value": "neutral",
            "intensity": 0.0
        },
        "tone": {
            "value": "neutral",
            "context_weight": 1.0
        },
        "intention": {
            "value": "",
            "certainty": 0.0
        },
        "semantic_evolution": {
            "timeline": []
        },
        "compatibility_matrix": {
            "emotion_tone": [],
            "intention_emotion": []
        },
        "dynamic_enums": {
            "emotion_values": SCHEMA_STATE["dynamic_enums"]["emotion_values"].copy(),
            "tone_values": SCHEMA_STATE["dynamic_enums"]["tone_values"].copy()
        },
        "validation_info": {
            "last_validated": "",
            "validation_score": 1.0
        }
    }

def fetch_external_definition(term: str, sources: list) -> Optional[str]:
    for source in sources:
        try:
            url = source["url"].format(term=requests.utils.quote(term))
            response = requests.get(url, timeout=3)
            if response.status_code == 200:
                return response.text[:500]  # Limitar tamanho
        except Exception:
            continue
    return None

def apply_domain_defaults(item: dict):
    domain = item.get("usage_context", {}).get("domain", "")
    
    if domain == "technical":
        if not item.get("tone", {}).get("value"):
            item["tone"] = {"value": "technical", "context_weight": 0.9}
        if not item.get("intention", {}).get("value"):
            item["intention"] = {"value": "inform", "certainty": 0.95}
        if not item["semantic_evolution"]["timeline"]:
            item["semantic_evolution"]["timeline"] = [
                {
                    "period": {"start": datetime.now().strftime("%Y-%m-%d")},
                    "dominant_meaning": item["definition"][:100] if item.get("definition") else "Significado inicial"
                }
            ]
    
    if domain == "legal":
        if not item.get("tone", {}).get("value"):
            item["tone"] = {"value": "formal", "context_weight": 0.95}

# ----------- Função principal de validação -----------
def validate_dgtm_item(item: dict, schema: dict, all_items: dict) -> Tuple[dict, list]:
    errors = []
    warnings = []
    
    # 1. Validação básica do schema
    try:
        validate(instance=item, schema=schema)
    except ValidationError as e:
        best_error = best_match([e])
        errors.append(f"Erro de schema: {best_error.message}")
    
    # 2. Validações adicionais de formato
    if not validate_datetime_format(item["last_modified"]):
        errors.append("Formato inválido para last_modified")
    
    # 3. Validar regras condicionais
    conditional_validations = [
        validate_homograph,
        validate_curation,
        check_unique_term,
        validate_emotion_values,
        validate_semantic_evolution
    ]
    
    for validation in conditional_validations:
        valid, msg = validation(item)
        if not valid:
            errors.append(msg)
        elif msg:
            warnings.append(msg)
    
    # 4. Validar regras semânticas
    semantic_validations = [
        validate_emotion_intensity,
        validate_tone_for_domain,
        validate_domain_specific_rules,
        validate_emotion_tone_compatibility,
        validate_intention_emotion_compatibility,
        lambda i: validate_synonyms(i, all_items),
        lambda i: validate_synonym_semantic_consistency(i, all_items)
    ]
    
    for validation in semantic_validations:
        valid, msg = validation(item)
        if not valid:
            errors.append(msg)
        elif msg:
            warnings.append(msg)
    
    # 5. Validar enums dinâmicos
    for field in ["emotion", "tone"]:
        valid, msg = validate_dynamic_enum(item, field)
        if msg and "Novo valor" in msg:
            warnings.append(msg)
    
    # 6. Atualizar campos derivados
    validation_score = 1.0 - (len(errors) * 0.1)
    validation_score = max(0.0, min(1.0, validation_score))
    
    item["validation_info"] = {
        "last_validated": datetime.now().isoformat(),
        "validation_score": round(validation_score, 2)
    }
    
    # Manter warnings separadamente
    item["_warnings"] = warnings
    
    # 7. Aplicar padrões de domínio
    apply_domain_defaults(item)
    
    return item, errors

# ----------- Função de criação de item -----------
def create_dgtm_item(term: str, schema: dict, external_sources: list, all_items: dict) -> Tuple[dict, dict]:
    log_info = {'term': term, 'errors': [], 'warnings': []}
    item = initialize_item(term)
    all_items[item["id"]] = item
    
    try:
        # Buscar definição em fontes externas
        definition = fetch_external_definition(term, external_sources)
        if definition:
            item["definition"] = definition
        else:
            # Mantém vazio e gera alerta para adição manual
            log_info['warnings'].append("DEFINIÇÃO_NÃO_ENCONTRADA - requer adição manual")
            item["_missing_definition"] = True  # Flag especial
        
        item, errors = validate_dgtm_item(item, schema, all_items)
        log_info["errors"] = errors
        log_info["warnings"] = item.get("_warnings", [])
        
        if item["validation_info"]["validation_score"] <= 0.6:
            item["curation_info"] = {
                "curator": "auto-validator",
                "validation_date": datetime.now().isoformat(),
                "confidence": item["validation_info"]["validation_score"],
                "notes": "Automaticamente gerado pelo validador"
            }
    
    except Exception as e:
        logger.error(f"Erro crítico: {str(e)}")
        log_info["errors"].append(f"Erro crítico: {str(e)}")
    
    all_items[item["id"]] = item
    return item, log_info

# ----------- Função principal -----------
def main():
    os.makedirs(INPUT_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(os.path.dirname(SCHEMA_PATH), exist_ok=True)
    
    logger.info(f"Iniciando processamento")
    logger.info(f"Input: {INPUT_DIR}")
    logger.info(f"Schema: {SCHEMA_PATH}")
    logger.info(f"Output: {OUTPUT_FILE}")

    # Carregar schema
    try:
        try:
            import json5 as json
        except ImportError:
            try:
                import commentjson as json
            except ImportError:
                import json
        
        with open(SCHEMA_PATH, 'r', encoding='utf-8') as f:
            schema = json.load(f)
        
        # Inicializar estado global do schema
        SCHEMA_STATE["dynamic_enums"] = {
            "emotion_values": schema["properties"]["dynamic_enums"]["properties"]["emotion_values"].get("default", []),
            "tone_values": schema["properties"]["dynamic_enums"]["properties"]["tone_values"].get("default", [])
        }
        SCHEMA_STATE["compatibility_matrix"] = {
            "emotion_tone": schema["properties"]["compatibility_matrix"]["properties"]["emotion_tone"].get("default", []),
            "intention_emotion": schema["properties"]["compatibility_matrix"]["properties"]["intention_emotion"].get("default", [])
        }
        
        external_sources = schema.get("x-external-sources", [])
        logger.info(f"Schema carregado com sucesso")
    except FileNotFoundError:
        logger.error(f"Arquivo de schema não encontrado")
        return
    except Exception as e:
        logger.error(f"Erro ao carregar schema: {str(e)}")
        return

    # Processar arquivos de entrada
    termos = []
    if os.path.isdir(INPUT_DIR):
        logger.info(f"Processando diretório de entrada")
        for filename in os.listdir(INPUT_DIR):
            if filename.endswith(".txt"):
                file_path = os.path.join(INPUT_DIR, filename)
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        termos.extend([line.strip() for line in f if line.strip()])
                except Exception as e:
                    logger.error(f"Erro ao ler {filename}: {str(e)}")
    else:
        logger.error(f"Diretório de entrada não encontrado")
        return

    if not termos:
        logger.warning("Nenhum termo encontrado para processamento")
        return

    # Schema Parquet
    parquet_schema = pa.schema([
        pa.field('id', pa.string()),
        pa.field('term', pa.string()),
        pa.field('canonical_form', pa.string()),
        pa.field('homograph_key', pa.string()),
        pa.field('definition', pa.string()),
        pa.field('last_modified', pa.string()),
        pa.field('missing_definition', pa.bool_()),  # Novo campo para sinalizar definição ausente
        
        pa.field('usage_context', pa.struct([
            pa.field('domain', pa.string()),
            pa.field('subdomain', pa.string())
        ])),
        
        pa.field('emotion', pa.struct([
            pa.field('value', pa.string()),
            pa.field('intensity', pa.float32())
        ])),
        
        pa.field('tone', pa.struct([
            pa.field('value', pa.string()),
            pa.field('context_weight', pa.float32())
        ])),
        
        pa.field('intention', pa.struct([
            pa.field('value', pa.string()),
            pa.field('certainty', pa.float32())
        ])),
        
        pa.field('synonyms', pa.list_(pa.struct([
            pa.field('term_id', pa.string()),
            pa.field('relationship', pa.string())
        ]))),
        
        pa.field('dynamic_enums', pa.struct([
            pa.field('emotion_values', pa.list_(pa.string())),
            pa.field('tone_values', pa.list_(pa.string()))
        ])),
        
        pa.field('semantic_evolution', pa.string()),
        pa.field('compatibility_matrix', pa.string()),
        
        pa.field('validation_info', pa.struct([
            pa.field('last_validated', pa.string()),
            pa.field('validation_score', pa.float32())
        ])),
        
        pa.field('curation_info', pa.struct([
            pa.field('curator', pa.string()),
            pa.field('validation_date', pa.string()),
            pa.field('confidence', pa.float32()),
            pa.field('notes', pa.string())
        ]), nullable=True),
        
        pa.field('warnings', pa.list_(pa.string()))
    ])

    # Processar termos
    all_items = {}
    with open(LOG_FILE, 'w', encoding='utf-8') as log_file, \
         pq.ParquetWriter(OUTPUT_FILE, parquet_schema, compression='ZSTD') as writer:
        
        for term in termos:
            item, log_info = create_dgtm_item(term, schema, external_sources, all_items)
            
            log_entry = {
                "timestamp": datetime.now().isoformat(),
                "term": term,
                "item_id": item.get("id"),
                "errors": log_info.get("errors", []),
                "warnings": log_info.get("warnings", [])
            }
            log_file.write(json.dumps(log_entry, ensure_ascii=False) + '\n')
            
            # Preparar dados para Parquet
            parquet_data = {
                'id': item['id'],
                'term': item['term'],
                'canonical_form': item.get('canonical_form', ''),
                'homograph_key': item.get('homograph_key', ''),
                'definition': item.get('definition', ''),
                'last_modified': item.get('last_modified', ''),
                'missing_definition': item.get('_missing_definition', False),
                
                'usage_context': {
                    'domain': item.get('usage_context', {}).get('domain', ''),
                    'subdomain': item.get('usage_context', {}).get('subdomain', '')
                },
                
                'emotion': {
                    'value': item.get('emotion', {}).get('value', ''),
                    'intensity': item.get('emotion', {}).get('intensity', 0.0)
                },
                
                'tone': {
                    'value': item.get('tone', {}).get('value', ''),
                    'context_weight': item.get('tone', {}).get('context_weight', 0.0)
                },
                
                'intention': {
                    'value': item.get('intention', {}).get('value', ''),
                    'certainty': item.get('intention', {}).get('certainty', 0.0)
                },
                
                'synonyms': item.get('synonyms', []),
                
                'dynamic_enums': {
                    'emotion_values': item.get('dynamic_enums', {}).get('emotion_values', []),
                    'tone_values': item.get('dynamic_enums', {}).get('tone_values', [])
                },
                
                'semantic_evolution': json.dumps(item.get('semantic_evolution', {})),
                'compatibility_matrix': json.dumps(item.get('compatibility_matrix', {})),
                
                'validation_info': {
                    'last_validated': item.get('validation_info', {}).get('last_validated', ''),
                    'validation_score': item.get('validation_info', {}).get('validation_score', 1.0)
                },
                
                'curation_info': None,
                'warnings': item.get('_warnings', [])
            }
            
            if 'curation_info' in item:
                parquet_data['curation_info'] = {
                    'curator': item['curation_info'].get('curator', ''),
                    'validation_date': item['curation_info'].get('validation_date', ''),
                    'confidence': item['curation_info'].get('confidence', 0.0),
                    'notes': item['curation_info'].get('notes', '')
                }
            
            table = pa.Table.from_pydict(parquet_data, schema=parquet_schema)
            writer.write_table(table)
            
            # Log especial para definições ausentes
            if parquet_data['missing_definition']:
                logger.warning(f"Termo sem definição: {term} | ID: {item['id']}")
            
            logger.info(f"Processado: {term} | Score: {parquet_data['validation_info']['validation_score']:.2f}")

    logger.info(f"Processo concluído. Itens processados: {len(termos)}")
    logger.info(f"Arquivo de saída: {OUTPUT_FILE}")
    logger.info(f"Termos sem definição: {sum(1 for item in all_items.values() if item.get('_missing_definition'))}")

if __name__ == "__main__":
    main()