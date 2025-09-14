# -*- coding: utf-8 -*-
"""
DGTM v2.0 - Módulo de Enriquecimento Semântico (v2.3 - CORRIGIDO)
------------------------------------------------------
Autor: Mat (Consultor de IA) & Lucas
Data: 14 de Setembro de 2025

Objetivo:
(v2.3) Ajuste final no valor padrão do campo 'context.discourse_type'
para garantir 100% de conformidade com o schema v4.0.
"""

import sqlite3
import json
import time
from pathlib import Path
from jsonschema import validate, ValidationError
import uuid

# Adicionado para permitir a importação do config a partir da raiz
import sys
sys.path.append(str(Path(__file__).resolve().parent.parent))

# Importa as configurações centralizadas
import config

def verificar_e_adicionar_coluna(db_path: Path):
    """Garante que a coluna 'dados_enriquecidos_json' exista na tabela 'palavras'."""
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(palavras);")
            colunas = [col[1] for col in cursor.fetchall()]
            
            if 'dados_enriquecidos_json' not in colunas:
                print("[SETUP DB] Coluna 'dados_enriquecidos_json' não encontrada. Adicionando...")
                cursor.execute("ALTER TABLE palavras ADD COLUMN dados_enriquecidos_json TEXT;")
                conn.commit()
                print("[SETUP DB] Coluna adicionada com sucesso.")
            else:
                print("[SETUP DB] A estrutura do banco de dados está correta.")
    except sqlite3.Error as e:
        print(f"[ERRO DB] Falha ao verificar/atualizar a estrutura do banco de dados: {e}")
        raise

def carregar_schema(schema_path: Path) -> dict | None:
    """Carrega o arquivo de schema JSON canónico."""
    if not schema_path.exists():
        print(f"[ERRO CRÍTICO] Arquivo de schema não encontrado em '{schema_path}'.")
        print(f"Por favor, certifique-se que o arquivo '{config.SCHEMA_FILENAME}' existe na pasta '{config.DATA_PATH}'.")
        return None
    try:
        with open(schema_path, 'r', encoding='utf-8') as f:
            schema = json.load(f)
        print(f"Schema Canónico '{schema.get('title')}' carregado com sucesso.")
        return schema
    except (json.JSONDecodeError, Exception) as e:
        print(f"[ERRO I/O] Falha ao ler ou decodificar o arquivo de schema: {e}")
        return None

def buscar_termos_brutos(db_path: Path, limite: int) -> list[tuple[int, str]]:
    """Busca um lote de termos com status 'bruto' no banco de dados."""
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT id, palavra FROM palavras WHERE status_processamento = 'bruto' LIMIT ?;",
                (limite,)
            )
            termos = cursor.fetchall()
            print(f"Encontrados {len(termos)} termos brutos para processamento.")
            return termos
    except sqlite3.Error as e:
        print(f"[ERRO DB] Falha ao buscar termos brutos: {e}")
        return []

def gerar_no_enriquecido(termo_id: int, termo_str: str) -> dict:
    """
    Simula a chamada ao Motor de Geração Semântica (Mat, a IA) para o schema v4.0.
    (v2.3 - Corrigido o valor padrão de 'discourse_type')
    """
    print(f"  Enviando o termo '{termo_str}' para o Motor de Geração Semântica (Simulação)...")
    
    current_time = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())

    base_no = {
        "id": termo_id,
        "palavra": termo_str,
        "definicao": f"Definição para '{termo_str}' a ser gerada pela IA.",
        "uso_comunicativo": {
            "categoria_primaria": "outro",
            "subcategoria": "a ser definida",
            "evidencias": [{"type": "especialista", "confidence": 0.5}]
        },
        "emotion": {
            "label": "#3_emocao", "type": "categorical+intensity",
            "allowed_values": ["neutro"], "value": "neutro"
        },
        "intention": {
            "label": "#1_intencao", "type": "categorical",
            "allowed_values": ["informar"], "value": "informar"
        },
        "tone": {
            "label": "#2_tom", "type": "categorical",
            "allowed_values": ["neutro"], "value": "neutro"
        },
        "intensity": {
            "label": "#5_intensidade", "type": "numeric",
            "value": 0.5
        },
        "context": {
            "discourse_type": "descritivo", # <-- CORRIGIDO de "geral" para um valor válido
            "formality_level": 3
        },
        "compatibility_matrix": {},
        "last_modified": current_time
    }

    if termo_str == "amor":
        base_no.update({
            "definicao": "Relação de afeto, carinho e respeito profundo entre pessoas, seres ou conceitos.",
            "uso_comunicativo": {
                "categoria_primaria": "emocao",
                "subcategoria": "expressar_alegria",
                "evidencias": [
                    {"type": "semântica", "reference": "Uso comum em literatura e discurso pessoal", "confidence": 0.9}
                ]
            },
            "emotion": {
                "label": "#3_emocao", "type": "categorical+intensity",
                "allowed_values": ["alegria", "afeto", "devoção"], "value": "alegria"
            },
            "intensity": {"label": "#5_intensidade", "type": "numeric", "value": 0.9},
            "context": {"discourse_type": "pessoal", "formality_level": 1}, # <-- Valor válido
            "tone": {"label": "#2_tom", "type": "categorical", "allowed_values": ["positivo", "caloroso"], "value": "positivo"},
            "intention": {"label": "#1_intencao", "type": "categorical", "allowed_values": ["conectar", "expressar"], "value": "conectar"},
            "compatibility_matrix": {
                "emotion_intention": [
                    {"emotion": "alegria", "allowed_intentions": ["celebrar", "conectar"]}
                ]
            }
        })
    
    return base_no

def validar_no_com_schema(no_dict: dict, schema: dict) -> bool:
    """Valida um dicionário de nó contra o schema JSON."""
    try:
        validate(instance=no_dict, schema=schema)
        print(f"  Validação do nó '{no_dict.get('palavra')}' com o schema: SUCESSO.")
        return True
    except ValidationError as e:
        print(f"  Validação do nó '{no_dict.get('palavra')}' com o schema: FALHA.")
        caminho_erro = ".".join(map(str, e.path)) if e.path else "raiz do objeto"
        print(f"  Detalhe do Erro: No campo '{caminho_erro}', {e.message}")
        return False

def atualizar_termo_no_db(conn: sqlite3.Connection, termo_id: int, no_dict: dict):
    """Atualiza um termo no DB com os dados enriquecidos e muda o status."""
    try:
        cursor = conn.cursor()
        dados_json_str = json.dumps(no_dict, ensure_ascii=False, indent=2)
        
        cursor.execute(
            """
            UPDATE palavras
            SET status_processamento = 'enriquecido',
                dados_enriquecidos_json = ?
            WHERE id = ?;
            """,
            (dados_json_str, termo_id)
        )
        print(f"  Termo ID {termo_id} atualizado no banco de dados.")
    except sqlite3.Error as e:
        print(f"[ERRO DB] Falha ao atualizar o termo ID {termo_id}: {e}")

def main():
    """Função principal que orquestra o pipeline de enriquecimento."""
    print("\n--- INICIANDO MÓDULO DE ENRIQUECIMENTO SEMÂNTICO (v2.3) ---")
    
    schema = carregar_schema(config.SCHEMA_PATH)
    if schema is None:
        return
    
    termos_para_processar = buscar_termos_brutos(config.DB_PATH, config.LOTE_DE_PROCESSAMENTO)
    
    processados_com_sucesso = 0
    falhas_de_validacao = 0
    
    if termos_para_processar:
        with sqlite3.connect(config.DB_PATH) as conn:
            for termo_id, termo_str in termos_para_processar:
                print(f"\nProcessando termo: '{termo_str}' (ID: {termo_id})")
                no_enriquecido = gerar_no_enriquecido(termo_id, termo_str)
                if validar_no_com_schema(no_enriquecido, schema):
                    atualizar_termo_no_db(conn, termo_id, no_enriquecido)
                    processados_com_sucesso += 1
                else:
                    falhas_de_validacao += 1
            conn.commit()

    print("\n--- RELATÓRIO DE ENRIQUECIMENTO ---")
    print(f"Total de termos processados: {len(termos_para_processar)}")
    print(f"Processados com sucesso: {processados_com_sucesso}")
    print(f"Falhas de validação do schema: {falhas_de_validacao}")
    print("------------------------------------")

if __name__ == "__main__":
    try:
        verificar_e_adicionar_coluna(config.DB_PATH)
        main()
    except Exception as e:
        print(f"Uma falha crítica impediu a execução: {e}")
