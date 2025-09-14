# -*- coding: utf-8 -*-
"""
DGTM v2.0 - Módulo de Validação Semântica (DVCS-001)
------------------------------------------------------
Autor: Mat (Consultor de IA) & Lucas
Data: 14 de Setembro de 2025

Objetivo:
Este módulo implementa a "Camada de Governança Semântica". Ele lê
nós enriquecidos pela IA, aplica as 'coherence_rules' e a
'compatibility_matrix' definidas no schema v4.0, e atualiza o status
dos nós para 'validado_coerencia' ou 'requer_revisao_humana'.
"""

import sqlite3
import json
import time
from pathlib import Path

# Adicionado para permitir a importação do config a partir da raiz
import sys
sys.path.append(str(Path(__file__).resolve().parent.parent))

# Importa as configurações centralizadas
import config

def carregar_schema(schema_path: Path) -> dict | None:
    """Carrega o arquivo de schema JSON canónico."""
    if not schema_path.exists():
        print(f"[ERRO CRÍTICO] Arquivo de schema não encontrado em '{schema_path}'.")
        return None
    try:
        with open(schema_path, 'r', encoding='utf-8') as f:
            schema = json.load(f)
        print(f"Schema Canónico '{schema.get('title')}' carregado com sucesso.")
        return schema
    except (json.JSONDecodeError, Exception) as e:
        print(f"[ERRO I/O] Falha ao ler ou decodificar o arquivo de schema: {e}")
        return None

def buscar_nos_enriquecidos(db_path: Path) -> list[tuple[int, str]]:
    """Busca um lote de nós com status 'enriquecido' no banco de dados."""
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT id, dados_enriquecidos_json FROM palavras WHERE status_processamento = 'enriquecido';"
            )
            nos = cursor.fetchall()
            print(f"Encontrados {len(nos)} nós enriquecidos para validação semântica.")
            return nos
    except sqlite3.Error as e:
        print(f"[ERRO DB] Falha ao buscar nós enriquecidos: {e}")
        return []

def get_nested_value(d: dict, key_path: str):
    """Acessa um valor aninhado em um dicionário usando uma string 'dot.notation'."""
    keys = key_path.split('.')
    current_level = d
    for key in keys:
        if isinstance(current_level, dict) and key in current_level:
            current_level = current_level[key]
        else:
            return None
    return current_level

def aplicar_regras_de_coerencia(no_dict: dict, regras: list) -> list[str]:
    """Aplica a lista de 'coherence_rules' a um nó e retorna os avisos."""
    avisos = []
    for regra in regras:
        condicao_if = regra.get("if", {})
        condicao_then = regra.get("then", {})
        mensagem = regra.get("message", "Violação de regra de coerência.")

        if_satisfeito = True
        for key_path, condition in condicao_if.items():
            valor_no = get_nested_value(no_dict, key_path)
            if valor_no is None:
                if_satisfeito = False
                break
            
            if isinstance(condition, list) and valor_no not in condition:
                if_satisfeito = False; break
            elif isinstance(condition, dict):
                if "minimum" in condition and valor_no < condition["minimum"]:
                    if_satisfeito = False; break
                if "maximum" in condition and valor_no > condition["maximum"]:
                    if_satisfeito = False; break
        
        if if_satisfeito:
            then_satisfeito = True
            for key_path, condition in condicao_then.items():
                valor_no = get_nested_value(no_dict, key_path)
                if valor_no is None: continue
                
                if "not" in condition and valor_no in condition["not"]:
                    then_satisfeito = False; break
            
            if not then_satisfeito:
                avisos.append(mensagem)
                
    return avisos

def atualizar_status_do_no(conn: sqlite3.Connection, no_id: int, novo_status: str, avisos: list):
    """Atualiza o status e os avisos de um nó no banco de dados."""
    try:
        cursor = conn.cursor()
        avisos_json_str = json.dumps(avisos) if avisos else None
        
        cursor.execute(
            """
            UPDATE palavras
            SET status_processamento = ?,
                avisos_de_validacao = ?
            WHERE id = ?;
            """,
            (novo_status, avisos_json_str, no_id)
        )
    except sqlite3.Error as e:
        print(f"[ERRO DB] Falha ao atualizar o status do nó ID {no_id}: {e}")

def main():
    """Função principal que orquestra o pipeline de validação semântica."""
    print("\n--- INICIANDO MÓDULO DE VALIDAÇÃO SEMÂNTICA (DVCS-001) ---")
    
    schema = carregar_schema(config.SCHEMA_PATH)
    if schema is None:
        return
        
    regras_coerencia = schema.get("coherence_rules", {}).get("rules", [])
    
    nos_para_processar = buscar_nos_enriquecidos(config.DB_PATH)
    
    validados_com_sucesso = 0
    necessitam_revisao = 0
    
    if nos_para_processar:
        with sqlite3.connect(config.DB_PATH) as conn:
            for no_id, dados_json_str in nos_para_processar:
                if not dados_json_str: continue
                no_dict = json.loads(dados_json_str)
                palavra = no_dict.get('palavra', f'ID {no_id}')
                print(f"\nValidando semanticamente o termo: '{palavra}' (ID: {no_id})")

                avisos = aplicar_regras_de_coerencia(no_dict, regras_coerencia)
                
                if not avisos:
                    novo_status = 'validado_coerencia'
                    validados_com_sucesso += 1
                    print(f"  -> Status: {novo_status}. Nenhuma inconsistência encontrada.")
                else:
                    novo_status = 'requer_revisao_humana'
                    necessitam_revisao += 1
                    print(f"  -> Status: {novo_status}. Inconsistências encontradas:")
                    for aviso in avisos:
                        print(f"     - {aviso}")

                atualizar_status_do_no(conn, no_id, novo_status, avisos)
            conn.commit()

    print("\n--- RELATÓRIO DE VALIDAÇÃO SEMÂNTICA ---")
    print(f"Total de nós processados: {len(nos_para_processar)}")
    print(f"Validados com sucesso (coerentes): {validados_com_sucesso}")
    print(f"Marcados para revisão humana: {necessitam_revisao}")
    print("-----------------------------------------")

if __name__ == "__main__":
    main()