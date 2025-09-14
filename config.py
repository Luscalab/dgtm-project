# -*- coding: utf-8 -*-
"""
DGTM v2.0 - Arquivo de Configuração Central (v1.3)
------------------------------------------------------
Autor: Mat (Consultor de IA) & Lucas
Data: 14 de Setembro de 2025

Objetivo:
Centralizar todas as constantes, caminhos de arquivos e parâmetros
de configuração do projeto DGTM v2.0.
v1.3 - Sincronizado para usar o nome de arquivo de schema exato
confirmado pelo usuário.
"""

from pathlib import Path

# --- CAMINHOS PRINCIPAIS ---
BASE_PATH = Path(__file__).resolve().parent

# --- SUBDIRETÓRIOS ---
DATA_PATH = BASE_PATH / "data"
CORE_PATH = BASE_PATH / "core"
SCRIPTS_PATH = BASE_PATH / "scripts"
PAINEL_PATH = BASE_PATH / "painel"
LOGS_PATH = BASE_PATH / "logs"
INPUT_PATH = DATA_PATH / "input"

# --- BANCO DE DADOS ---
DB_PATH = DATA_PATH / "dgtm_v2.db"

# --- ARQUIVOS DE DADOS E SCHEMAS ---

# Define o NOME do arquivo de schema canónico.
# Alinhado com o nome de arquivo real no sistema do usuário.
SCHEMA_FILENAME = "dgtm_v5.2_schema.json"

# Constrói o caminho completo para o schema.
SCHEMA_PATH = DATA_PATH / SCHEMA_FILENAME

# Caminho para o arquivo de entrada de termos brutos.
TERMOS_INPUT_PATH = INPUT_PATH / "termos.txt"

# --- PARÂMETROS DO PIPELINE ---
LOTE_DE_PROCESSAMENTO = 100

print(f"Módulo de configuração 'config.py' (v1.3) carregado. Usando schema: '{SCHEMA_FILENAME}'")