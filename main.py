# -*- coding: utf-8 -*-
"""
DGTM v2.0 - Orquestrador de Pipeline (DOP-001) - v1.3
------------------------------------------------------
Autor: Mat (Consultor de IA) & Lucas
Data: 14 de Setembro de 2025

Objetivo:
(v1.3) Adicionada uma verificação de robustez para lidar com retornos
inesperados da função de verificação de sanidade.
"""

import argparse
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent
sys.path.append(str(project_root))

try:
    from scripts.verificar_ambiente import verificar_sanidade
    from core.processador_incremental import main as main_processador
    from core.enriquecedor_semantico import main as main_enriquecedor
    from core.validador_semantico import main as main_validador
    import config
except ImportError as e:
    print(f"[ERRO DE IMPORTAÇÃO] Não foi possível encontrar um módulo necessário: {e}")
    sys.exit(1)

def main():
    """Função principal que parseia os argumentos da linha de comando."""
    parser = argparse.ArgumentParser(
        prog="DGTM v2.0 Maestro",
        description="Orquestrador central para o pipeline de processamento semântico DGTM.",
        epilog="Use 'python main.py <comando> --help' para mais informações."
    )
    subparsers = parser.add_subparsers(dest="command", required=True, help="Comandos disponíveis")

    parser_status = subparsers.add_parser("status", help="Executa a verificação de sanidade do ambiente (DVSA-001).")
    parser_status.set_defaults(func=verificar_sanidade)

    parser_popular = subparsers.add_parser("popular", help="Executa o processador incremental para adicionar termos brutos ao DB.")
    parser_popular.set_defaults(func=main_processador)
    
    parser_enriquecer = subparsers.add_parser("enriquecer", help="Executa o módulo de enriquecimento semântico nos termos brutos.")
    parser_enriquecer.set_defaults(func=main_enriquecedor)

    parser_validar = subparsers.add_parser("validar", help="Executa o módulo de validação de coerência semântica (DVCS-001).")
    parser_validar.set_defaults(func=main_validador)

    args = parser.parse_args()
    
    print(f"\nExecutando comando: '{args.command}'...")
    
    if args.command != 'status':
      print("Executando verificação de sanidade pré-execução (DVSA-001)...")
      erros = verificar_sanidade()
      
      # Verificação de robustez: Garante que 'erros' é um número antes de comparar
      if not isinstance(erros, int) or erros > 0:
          print(f"\n[EXECUÇÃO ABORTADA] A verificação de sanidade encontrou problemas ou retornou um valor inesperado.")
          print("Por favor, corrija os erros reportados acima antes de prosseguir.")
          return
      
    # Chama a função associada ao comando
    args.func()

if __name__ == "__main__":
    main()