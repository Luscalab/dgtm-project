# DGTM – Pipeline Semântico Modular Integrado


## Visão Geral
O DGTM é um pipeline robusto, modular e auditável para extração, categorização, enriquecimento e serialização de nós semânticos, formando um grafo de conhecimento escalável e simbólico para o português. O projeto integra processamento incremental, enriquecimento heurístico/simbólico, codificação, validação, visualização interativa e painel de controle profissional.

### Bases Teóricas
- **Processamento de Linguagem Natural (PLN):** Utiliza técnicas de NLP clássicas (NLTK, spaCy, TextBlob) e heurísticas simbólicas para análise de palavras, sentimentos, emoções e contexto.
- **Grafo de Conhecimento:** Estrutura os dados em grafos semânticos, permitindo inferência, expansão e validação cruzada.
- **Validação Semântica:** Inspira-se em ontologias, taxonomias e modelos de coerência semântica para garantir integridade e plausibilidade dos nós.
- **Auditoria e Transparência:** Todo processamento é logado e auditável, com painéis visuais e relatórios detalhados.

### Principais Funcionalidades
- **Processamento incremental e modular de dados textuais**
- **Categorização automática e manual de palavras**
- **Enriquecimento simbólico e heurístico dos nós**
- **Validação semântica e auditoria avançada**
- **Visualização interativa do grafo**
- **Painel de controle moderno (Streamlit) com chat IA**
- **Playground para testes e prototipação de componentes**


## Pipeline Unificado: Etapas e Integração


1. **Processamento Incremental** (`core/processador_incremental.py`)
   - Extração, normalização, categorização, filtragem e estruturação em camadas.
   - Salva resultados em `data/palavras_categorizadas.json` e `data/words.db`.
   - Pode ser executado em paralelo com outras etapas.

2. **Enriquecimento e Validação**
   - **Enriquecedor DGTM** (`core/enriquecedor_dgtm.py`): Enriquecimento de grafos e regras, validação e expansão.
   - **Geração de Nós Enriquecidos** (`grafo_dgtm/gerar_nos_enriquecidos.py`): Validação, enriquecimento semântico (heurísticas, NLP spaCy opcional), coerência cruzada, codificação simbólica e logging detalhado.
   - Saídas: `data/dgtm_rules_expandido.json`, `data/nos_enriquecidos.json`.

3. **Construção e Serialização do Grafo**
   - **Geração do Grafo** (`grafo_dgtm/gerar_grafo.py`): Validação, ligação dos nós, serialização/compressão (`grafo_dgtm.zst`), geração de dicionário simbólico (`dicionario_simbolico.json`) e logs.


4. **Orquestração e Relatórios** (`categorizer.py`)
   - Coordena execuções, gera relatórios, pode disparar subprocessos das etapas acima.

5. **Visualização e Painel de Controle**
   - **Visualizador** (`grafo_dgtm/visualizador.py`): Gera visualização HTML interativa do grafo (Pyvis).
   - **Painel Streamlit** (`painel_controle.py`): Interface profissional para controle do pipeline, logs, visualização do grafo e chat contextual.


## Instalação

1. Clone o repositório e acesse a pasta do projeto:
   ```powershell
   git clone <url-do-repo>
   cd projeto-dgtm
   ```
2. Instale as dependências:
   ```powershell
   pip install -r requirements.txt
   ```
3. (Opcional) Instale dependências de desenvolvimento:
   ```powershell
   pip install -r requirements-dev.txt
   ```

## Execução Recomendada

1. Processamento incremental:
   ```powershell
   python core/processador_incremental.py
   ```
2. Enriquecimento DGTM:
   ```powershell
   python core/enriquecedor_dgtm.py
   ```
3. Geração de nós enriquecidos:
   ```powershell
   python grafo_dgtm/gerar_nos_enriquecidos.py
   ```
4. Geração do grafo simbólico:
   ```powershell
   python grafo_dgtm/gerar_grafo.py
   ```
5. Visualização e painel:
   ```powershell
   streamlit run painel/painel_controle.py
   ```

- Cada etapa pode ser executada separadamente e em paralelo.
- Comunicação via arquivos padronizados (JSON, SQLite, ZST).
- Logs detalhados em `logs/`.
- Fácil manutenção, expansão e auditoria.

## Painel de Controle (Streamlit)
O painel `painel_controle.py` oferece uma interface moderna, responsiva e auditável para:
- Visualizar status dos módulos, logs e execuções
- Auditar e recalcular campos de palavras do banco
- Visualizar e filtrar nós enriquecidos
- Explorar o grafo semântico interativo
- Utilizar chat IA contextual
- Prototipar componentes no Playground

### Como rodar o painel
```powershell
streamlit run painel_controle.py
```

### Funcionalidades do Painel
- **Visão Geral:** KPIs, status dos módulos, uptime
- **Processos:** Histórico, métricas, gráficos interativos
- **Logs:** Visualização e download de logs
- **Estrutura:** Árvore do projeto
- **Grafo DGTM:** Visualização interativa do grafo
- **Auditoria Visual:** Filtros avançados, heatmap de plausibilidade
- **Auditoria Palavras (DB):** Auditoria, recalculo, feedback visual
- **Chat IA:** Chat contextual com IA
- **Configurações:** Parâmetros e preferências
- **Playground:** Teste isolado de componentes, gráficos, cards, tabelas

### Design e Experiência
- Tema escuro moderno, gradientes, responsividade
- Cards, botões e badges com transições e hover
- Gráficos interativos (Plotly)
- Feedback visual (toasts, alerts)
- Filtros, busca e paginação
- Auditoria visual e semântica avançada
- Cada etapa pode ser executada separadamente e em paralelo.
- Comunicação via arquivos padronizados (JSON, SQLite, ZST).
- Logs detalhados em `logs/`.
- Fácil manutenção, expansão e auditoria.


## Estrutura de Pastas

```
projeto-dgtm/
│
├── agente_ia.py
├── painel_controle.py
├── grafo_dgtm/
│   ├── gerar_nos_enriquecidos.py
│   ├── gerar_grafo.py
│   ├── visualizador.py
│   ├── utils/
│   └── README.md
├── data/
│   ├── entrada_bruta.json
│   ├── nos_enriquecidos.json
│   ├── dicionario_simbolico.json
│   └── grafo_dgtm.zst
├── logs/
├── requirements.txt
└── requirements-dev.txt
```

- **Nós**: `data/nos_enriquecidos.json`
- **Regras**: `data/dgtm_rules.json`, `data/dgtm_rules_expandido.json`
- **Schema**: `data/dgtm_fields_schema.json`
- **Grafo Serializado**: `data/grafo_dgtm.zst`
- **Dicionário Simbólico**: `data/dicionario_simbolico.json`

## Estrutura dos Dados
- **Palavras:** Banco SQLite (`data/words.db`) e JSON (`data/palavras_categorizadas.json`)
- **Nós enriquecidos:** `data/nos_enriquecidos.json`
- **Regras:** `data/dgtm_rules.json`, `data/dgtm_rules_expandido.json`
- **Schema:** `data/dgtm_fields_schema.json`
- **Grafo Serializado:** `data/grafo_dgtm.zst`
- **Dicionário Simbólico:** `data/dicionario_simbolico.json`
- **Nós**: `data/nos_enriquecidos.json`
- **Regras**: `data/dgtm_rules.json`, `data/dgtm_rules_expandido.json`
- **Schema**: `data/dgtm_fields_schema.json`
- **Grafo Serializado**: `data/grafo_dgtm.zst`
- **Dicionário Simbólico**: `data/dicionario_simbolico.json`

## Exemplo de Nó (nos_enriquecidos.json)
```json
{
  "id": "n123",
  "palavra": "amizade",
  "categoria": "sentimento",
  "classe_gramatical": "substantivo",
  "contexto": ["relacionamento", "positivo"],
  "exemplos": ["amizade verdadeira", "amizade de infância"],
  "intencao": "proximidade",
  "emocao": "alegria",
  "estado_mental": "aberto",
  "intensidade": 70,
  "plausibilidade": 85,
  "consequencia": "apoio mútuo",
  "emotion_code": "#2xxx"
}
```

## Exemplo de Regra (dgtm_rules.json)
```json
{
  "if": {"palavra": "raiva", "contexto": "conflito"},
  "then": {"categoria": "emoção negativa", "tom": "agressivo"}
}
```

## Validação Semântica e Auditoria
- O sistema valida coerência entre emoção, intenção, tom, intensidade, consequência, etc.
- Auditoria visual e semântica avançada no painel
- Logs detalhados em `logs/`
- Recomenda-se manter campos de versão nos arquivos JSON:
```json
"versao": "2.0.0",
"ultima_atualizacao": "2025-06-28"
```

## Visualização Esquemática do Grafo
```
[palavra]──(relacao)──>[palavra]
   |                     |
 [emoção]             [categoria]
   |                     |
[intenção]           [contexto]
```


## Como Usar
1. Edite ou adicione nós, palavras e regras nos arquivos JSON/DB.
2. Execute os scripts do pipeline conforme necessário para validar, enriquecer e gerar o grafo.
3. Use o painel para explorar, auditar, validar e enriquecer o grafo, além de consultar o chat contextual e testar componentes no Playground.

---

## Documentação das Funções Principais

### `painel_controle.py` (Streamlit)
- **Sidebar Navigation:** Menu lateral para navegação entre abas.
- **Visão Geral:** Exibe KPIs, status dos módulos, uptime.
- **Processos:** Mostra histórico, métricas, gráficos (matplotlib/plotly), logs e status dos módulos.
- **Logs:** Visualização e download dos logs dos módulos.
- **Estrutura:** Árvore do projeto.
- **Grafo DGTM:** Visualização interativa do grafo (HTML/pyvis).
- **Auditoria Visual:** Filtros por emoção, plausibilidade, heatmap, cards coloridos.
- **Auditoria Palavras (DB):** Auditoria automática, recalculo de campos, feedback visual, status, tooltips.
- **Chat IA:** Chat contextual com IA, histórico, copiar, limpar, markdown customizado.
- **Configurações:** Parâmetros, caminhos, preferências.
- **Playground:** Teste isolado de cards, gráficos, tabelas, feedbacks, filtros.

#### Funções de Auditoria
- `auditar_palavra(p)`: Retorna lista de problemas semânticos/estruturais de uma palavra.
- `recalcular_campos(palavra)`: Corrige campos de plausibilidade, sentimento e emoção.

#### Funções de Integração
- `enviar_comando(acao, nome)`: Envia comando para execução de módulo.
- `ler_log(nome)`: Lê últimas linhas do log do módulo.
- `ler_status()`: Lê status dos módulos.

#### Funções de Visualização
- Cards, badges, botões, gráficos, tabelas, feedbacks, filtros, heatmaps.

#### Playground
- Espaço seguro para testar e auditar componentes visuais e funções isoladas.

---

## Teoria e Referências
- [Ontologias e Grafos de Conhecimento](https://en.wikipedia.org/wiki/Ontology_(information_science))
- [NLP em Português](https://github.com/nltk/nltk_data)
- [Validação Semântica](https://en.wikipedia.org/wiki/Semantic_analysis_(computing))
- [Streamlit Docs](https://docs.streamlit.io/)
- [Plotly Docs](https://plotly.com/python/)

---


## Novidades e Melhorias Recentes (2025)

- Painel Streamlit totalmente modernizado: visual/UX, temas, responsividade, cards, KPIs, gráficos, heatmaps, badges, tooltips, feedbacks visuais e Playground para prototipação.
- Indicadores da Visão Geral validados por logs reais, com botões de atualização e expanders para auditoria dos dados.
- Processador incremental otimizado: processa lotes de 300 palavras por rodada, sem delay artificial, maior velocidade e eficiência.
- Modularização visual: componentes, temas e CSS customizados em `designer/`.
- Auditoria visual e semântica avançada, recalculo de campos, filtros, heatmaps e cards explicativos.
- Logs detalhados e downloadáveis para todos os módulos.
- Documentação expandida, exemplos de dados, estrutura de pastas e integração entre módulos.
- Playground visual para testes de componentes, gráficos, tabelas e feedbacks.
- Guia de execução, instalação e arquitetura atualizado.

---

Atualizado em 2025 para arquitetura unificada, modular, auditável e visualmente moderna.

## Validador Semântico DGTM (v5.2)

O validador semântico processa automaticamente todos os arquivos de entrada na pasta `input/` (extensões `.txt`, `.dic`, `.bz2`), valida e categoriza conforme o schema DGTM v5.2, e gera logs detalhados e saída pronta para análise.

### Como usar

1. Coloque seus arquivos de entrada na pasta `input/`.
2. Certifique-se de que o schema está em `data/dgtm_fields_schema.json` (JSON puro, sem comentários).
   - O schema editável com comentários é `data/dgtm_fields_schema.jsonc` (não usado diretamente pelo pipeline).
3. Os exemplos incrementais devem estar em `data/exemplos_dgtm.json`.
4. Execute o validador:
   ```powershell
   python validador_semantico_dgtm.py
   ```
5. Resultados e auditoria:
   - Arquivo Parquet: `output/dgtm_categorizadas.parquet` (ideal para análise em Python, Pandas, Power BI, etc.)
   - Log detalhado: `output/auditoria_processamento.log` (legível por humanos, inclui todos os campos categorizados, erros e rastreabilidade de cada termo)

### Observações
- Não é necessário passar argumentos: os caminhos são fixos e padronizados.
- O schema DGTM v5.2 suporta campos avançados, relações semânticas, domínios, subdomínios, cross-reference e regras condicionais.
- O log detalhado permite auditoria manual e rastreabilidade total de cada termo processado.
- O Parquet facilita análise em lote, filtros e integração com ferramentas de dados.

---
