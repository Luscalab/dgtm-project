# -*- coding: utf-8 -*-
"""
DGTM v2.0 - Módulo de Processamento Incremental (v1.1)
------------------------------------------------------
Autor: Mat (Consultor de IA) & Lucas
Data: 14 de Setembro de 2025

Objetivo:
(v1.1) Refatorado para usar uma função 'main()' como ponto de entrada
para consistência com outros módulos do core.
"""

import sqlite3
import time
from pathlib import Path

# Adicionado para permitir a importação do config a partir da raiz
import sys
sys.path.append(str(Path(__file__).resolve().parent.parent))

import config

def normalizar_termo(termo: str) -> str:
    """Realiza a normalização básica de um termo."""
    return termo.strip().lower()

def inserir_termos_no_db(termos: list[str]) -> tuple[int, int]:
    """Insere uma lista de termos no banco de dados, evitando duplicados."""
    termos_inseridos = 0
    termos_duplicados = 0
    
    try:
        with sqlite3.connect(config.DB_PATH) as conn:
            cursor = conn.cursor()
            conn.execute("BEGIN TRANSACTION;")
            for termo in termos:
                try:
                    cursor.execute("INSERT INTO palavras (palavra) VALUES (?);", (termo,))
                    termos_inseridos += 1
                except sqlite3.IntegrityError:
                    termos_duplicados += 1
            conn.commit()
    except sqlite3.Error as e:
        print(f"\n[ERRO DB] Ocorreu um erro durante a inserção: {e}")
        conn.rollback()
        return 0, len(termos)
    return termos_inseridos, termos_duplicados

def main():
    """Função principal que orquestra o processo de leitura e ingestão."""
    print("--- INICIANDO PROCESSADOR INCREMENTAL DGTM v2.0 ---")
    start_time = time.time()

    if not config.DB_PATH.exists():
        print(f"[ERRO CRÍTICO] O arquivo do DB não foi encontrado em '{config.DB_PATH}'.")
        print("Execute 'scripts/inicializar_db.py' primeiro.")
        return

    if not config.TERMOS_INPUT_PATH.exists():
        print(f"[AVISO] Arquivo de entrada '{config.TERMOS_INPUT_PATH}' não encontrado.")
        config.TERMOS_INPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(config.TERMOS_INPUT_PATH, 'w', encoding='utf-8') as f:
            f.write("Amor\n   Justiça \nCOMPUTADOR\nCorrer\nEfêmero\nDemocracia\nSaudade\nAmor\n")
        print("Arquivo de exemplo criado. Edite-o e execute o script novamente.")
        return

    try:
        with open(config.TERMOS_INPUT_PATH, 'r', encoding='utf-8') as f:
            termos_brutos = f.readlines()
            termos_normalizados = [normalizar_termo(t) for t in termos_brutos if t.strip()]
        print(f"Total de {len(termos_normalizados)} termos lidos e normalizados.")
    except Exception as e:
        print(f"[ERRO I/O] Não foi possível ler o arquivo de entrada: {e}")
        return

    if termos_normalizados:
        print("Iniciando inserção no banco de dados...")
        inseridos, duplicados = inserir_termos_no_db(termos_normalizados)
        print("Processo de inserção concluído.")
    else:
        inseridos, duplicados = 0, 0
        print("Nenhum termo válido encontrado no arquivo de entrada.")

    end_time = time.time()
    duracao = end_time - start_time
    
    print("\n--- RELATÓRIO DE PROCESSAMENTO ---")
    print(f"Tempo de execução: {duracao:.4f} segundos")
    print(f"Total de termos lidos: {len(termos_normalizados)}")
    print(f"Novos termos inseridos no DB: {inseridos}")
    print(f"Termos duplicados (já existentes): {duplicados}")
    print("------------------------------------")

if __name__ == "__main__":
    main()