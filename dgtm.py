import re
import json
import random
from typing import List, Dict, Tuple, Optional
from transformers import pipeline
from datetime import datetime

class DynamicGraphTextualModel:
    def __init__(self, model_name: str = "t5-small"):
        """Inicializa o DGTM."""
        # Transformer para extração de relações (substituir por Grok em produção)
        self.neural_model = pipeline("text2text-generation", model=model_name)
        self.text_graph: str = ""  # Texto RGL
        self.token_dict: Dict[str, str] = {}  # Mapeia termos para tokens
        self.reverse_token_dict: Dict[str, str] = {}  # Mapeia tokens para termos
        self.next_token_id: int = 1
        self.max_edges_per_node: int = 10  # Limite para esparsidade
        self.similarity_threshold: float = 0.9  # Limiar para compressão

    def train_model(self, annotated_data: List[Dict], epochs: int = 1):
        """Treina o modelo para gerar texto RGL assertivo."""
        print(f"[{datetime.now()}] Iniciando treinamento com {len(annotated_data)} exemplos...")
        for epoch in range(epochs):
            for data in annotated_data:
                raw_text = data["raw_text"]
                target_rgl = data["target_rgl"]
                # Simula fine-tuning (em produção: treinar transformer)
                generated_rgl = self._generate_rgl_from_text(raw_text)
                loss = self._compute_loss(generated_rgl, target_rgl)
                print(f"[{datetime.now()}] Epoca {epoch + 1}, Exemplo: {raw_text[:50]}... -> Perda: {loss:.4f}")
                # Em produção: self.neural_model.update_weights(loss)
        print(f"[{datetime.now()}] Treinamento concluído.")

    def _generate_rgl_from_text(self, text: str) -> str:
        """Simula geração de RGL a partir de texto."""
        relations = self._extract_relations(text)
        rgl = ""
        for rel in relations:
            main_node = self._get_token(rel["meaning"])
            sub_node = self._get_token(rel["sub_meaning"])
            variable = self._get_token(rel["variable"])
            condition = self._get_token(rel["condition"])
            prob = rel["probability"]
            rgl += f"{main_node}:{sub_node}>{variable}(p{prob},{condition});"
        return rgl

    def _compute_loss(self, generated_rgl: str, target_rgl: str) -> float:
        """Calcula perda (simulada) entre RGL gerado e alvo."""
        # Simula métrica de similaridade (em produção: usar BLEU, F1, etc.)
        generated_statements = set(generated_rgl.split(";"))
        target_statements = set(target_rgl.split(";"))
        precision = len(generated_statements & target_statements) / max(len(generated_statements), 1)
        recall = len(generated_statements & target_statements) / max(len(target_statements), 1)
        return 1 - (2 * precision * recall) / max((precision + recall), 0.01)

    def convert_raw_data(self, raw_data: str) -> str:
        """Converte dados brutos em texto RGL."""
        print(f"[{datetime.now()}] Convertendo dados brutos...")
        relations = self._extract_relations(raw_data)
        self.text_graph = ""
        for rel in relations:
            main_node = self._get_token(rel["meaning"])
            sub_node = self._get_token(rel["sub_meaning"])
            variable = self._get_token(rel["variable"])
            condition = self._get_token(rel["condition"])
            prob = rel["probability"]
            statement = f"{main_node}:{sub_node}>{variable}(p{prob},{condition})"
            self.text_graph += statement + ";"
        self._validate_assertiveness()
        self._optimize_text()
        print(f"[{datetime.now()}] Conversão concluída. RGL: {self.text_graph}")
        return self.text_graph

    def _extract_relations(self, raw_data: str) -> List[Dict]:
        """Extrai relações de dados brutos (simulado)."""
        # Em produção: Usar Grok ou transformer treinado
        relations = []
        if "justiça" in raw_data.lower():
            relations.append({
                "meaning": "justiça",
                "sub_meaning": "justiça distributiva",
                "variable": "necessidade",
                "probability": "0.9",
                "condition": "crise"
            })
            relations.append({
                "meaning": "justiça",
                "sub_meaning": "justiça retributiva",
                "variable": "punição",
                "probability": "0.7",
                "condition": "crime"
            })
        if "crise" in raw_data.lower():
            relations.append({
                "meaning": "economia",
                "sub_meaning": "crise econômica",
                "variable": "auxílio",
                "probability": "0.95",
                "condition": "2025"
            })
        # Simula dados de sensores
        if "temperatura" in raw_data.lower():
            relations.append({
                "meaning": "ambiente",
                "sub_meaning": "temperatura",
                "variable": "conforto",
                "probability": "0.8",
                "condition": "verão"
            })
        return relations

    def _get_token(self, term: str) -> str:
        """Gera token comprimido para um termo."""
        if term not in self.token_dict:
            token = str(self.next_token_id)
            self.token_dict[term] = token
            self.reverse_token_dict[token] = term
            self.next_token_id += 1
        return self.token_dict[term]

    def maintain_graph(self, new_data: str):
        """Atualiza texto RGL com novos dados."""
        print(f"[{datetime.now()}] Verificando necessidade de atualização...")
        if self._needs_update(new_data):
            print(f"[{datetime.now()}] Atualizando RGL...")
            relations = self._extract_relations(new_data)
            for rel in relations:
                main_node = self._get_token(rel["meaning"])
                sub_node = self._get_token(rel["sub_meaning"])
                variable = self._get_token(rel["variable"])
                condition = self._get_token(rel["condition"])
                prob = float(rel["probability"])
                new_statement = f"{main_node}:{sub_node}>{variable}(p{prob},{condition})"
                # Atualização bayesiana simulada
                self.text_graph = self._update_text(new_statement, prob)
            self._validate_assertiveness()
            self._optimize_text()
            print(f"[{datetime.now()}] Atualização concluída. RGL: {self.text_graph}")

    def _needs_update(self, new_data: str) -> bool:
        """Verifica se atualização é necessária."""
        keywords = ["crise", "2025", "auxílio", "temperatura"]
        return any(kw in new_data.lower() for kw in keywords)

    def _update_text(self, new_statement: str, new_prob: float) -> str:
        """Atualiza texto RGL com nova declaração."""
        pattern = r"(\d+):(\d+)>(\d+)\(p([\d.]+),(\d+)\)"
        statements = self.text_graph.split(";")
        updated = False
        for i, stmt in enumerate(statements):
            match = re.match(pattern, stmt)
            if match:
                main_node, sub_node, variable, old_prob, condition = match.groups()
                old_prob = float(old_prob)
                # Simula atualização bayesiana (ex.: média ponderada)
                if f"{main_node}:{sub_node}>{variable}" in new_statement:
                    new_prob = (old_prob * 0.7 + new_prob * 0.3)  # Peso arbitrário
                    statements[i] = f"{main_node}:{sub_node}>{variable}(p{new_prob:.2f},{condition})"
                    updated = True
        if not updated:
            statements.append(new_statement)
        return ";".join(s for s in statements if s)

    def _validate_assertiveness(self):
        """Valida assertividade do texto RGL."""
        pattern = r"(\d+):(\d+)>(\d+)\(p([\d.]+),(\d+)\)"
        statements = self.text_graph.split(";")
        node_edges = {}
        for stmt in statements:
            match = re.match(pattern, stmt)
            if match:
                main_node, _, _, prob, _ = match.groups()
                prob = float(prob)
                if prob < 0 or prob > 1:
                    raise ValueError(f"Probabilidade inválida: {prob}")
                node_edges[main_node] = node_edges.get(main_node, 0) + 1
                if node_edges[main_node] > self.max_edges_per_node:
                    print(f"[{datetime.now()}] Aviso: Nó {main_node} excede {self.max_edges_per_node} arestas.")
                    # Em produção: Podar arestas menos relevantes

    def _optimize_text(self):
        """Otimizando texto RGL (remove redundâncias, comprime)."""
        statements = list(dict.fromkeys(self.text_graph.split(";")))
        # Simula compressão (em produção: usar Huffman)
        self.text_graph = ";".join(s for s in statements if s)
        # Garante esparsidade
        node_edges = {}
        pattern = r"(\d+):(\d+)>(\d+)\(p([\d.]+),(\d+)\)"
        for stmt in statements:
            match = re.match(pattern, stmt)
            if match:
                main_node = match.group(1)
                node_edges[main_node] = node_edges.get(main_node, []) + [stmt]
        for node, edges in node_edges.items():
            if len(edges) > self.max_edges_per_node:
                # Mantém arestas com maior probabilidade
                edges.sort(key=lambda x: float(re.match(pattern, x).group(4)), reverse=True)
                node_edges[node] = edges[:self.max_edges_per_node]
        self.text_graph = ";".join(
            stmt for edges in node_edges.values() for stmt in edges if stmt
        )

    def process_query(self, query: str, context: str) -> Tuple[str, str]:
        """Processa consulta parseando texto RGL."""
        print(f"[{datetime.now()}] Processando consulta: {query} (contexto: {context})")
        pattern = r"(\d+):(\d+)>(\d+)\(p([\d.]+),(\d+)\)"
        relevant_relations = []
        for statement in self.text_graph.split(";"):
            match = re.match(pattern, statement)
            if match:
                main_node, sub_node, variable, prob, condition = match.groups()
                if (context.lower() in self._reverse_token(condition).lower() or
                    context.lower() in self._reverse_token(main_node).lower()):
                    if query.lower() in self._reverse_token(main_node).lower():
                        relevant_relations.append({
                            "sub_meaning": self._reverse_token(sub_node),
                            "variable": self._reverse_token(variable),
                            "probability": float(prob),
                            "condition": self._reverse_token(condition)
                        })
        if relevant_relations:
            # Seleciona relação com maior probabilidade
            top_relation = max(relevant_relations, key=lambda x: x["probability"])
            result = f"Recomendar {top_relation['variable']}"
            explanation = (f"Baseado em {top_relation['sub_meaning']} com "
                          f"probabilidade {top_relation['probability']} no contexto "
                          f"{top_relation['condition']}")
            print(f"[{datetime.now()}] Resultado: {result}")
            return result, explanation
        print(f"[{datetime.now()}] Nenhuma recomendação encontrada.")
        return "Nenhuma recomendação", "Nenhum ramo relevante encontrado"

    def _reverse_token(self, token: str) -> str:
        """Converte token de volta ao termo original."""
        return self.reverse_token_dict.get(token, token)

    def save_model(self, filepath: str):
        """Salva texto RGL e dicionário de tokens."""
        with open(filepath, "w") as f:
            json.dump({
                "text_graph": self.text_graph,
                "token_dict": self.token_dict,
                "reverse_token_dict": self.reverse_token_dict,
                "next_token_id": self.next_token_id
            }, f)
        print(f"[{datetime.now()}] Modelo salvo em {filepath}")

    def load_model(self, filepath: str):
        """Carrega texto RGL e dicionário de tokens."""
        with open(filepath, "r") as f:
            data = json.load(f)
        self.text_graph = data["text_graph"]
        self.token_dict = data["token_dict"]
        self.reverse_token_dict = data["reverse_token_dict"]
        self.next_token_id = data["next_token_id"]
        print(f"[{datetime.now()}] Modelo carregado de {filepath}")

# Função para executar testes
def run_tests():
    print(f"[{datetime.now()}] Iniciando testes do DGTM...")
    dgtm = DynamicGraphTextualModel()

    # 1. Teste de Treinamento
    annotated_data = [
        {
            "raw_text": "Justiça distributiva é essencial em crises econômicas, priorizando necessidade.",
            "target_rgl": "1:2>3(p0.9,4)"
        },
        {
            "raw_text": "Justiça retributiva foca em punição em contextos de crime.",
            "target_rgl": "1:5>6(p0.7,7)"
        }
    ]
    dgtm.train_model(annotated_data, epochs=2)

    # 2. Teste de Conversão
    raw_data = ("Justiça distributiva é prioritária em crises, enquanto justiça retributiva "
                "foca em punição. Temperatura alta afeta conforto no verão.")
    text_graph = dgtm.convert_raw_data(raw_data)
    print(f"[{datetime.now()}] Teste de Conversão - RGL: {text_graph}")
    print(f"[{datetime.now()}] Dicionário de Tokens: {dgtm.token_dict}")

    # 3. Teste de Manutenção
    new_data = "Crise econômica de 2025 reforça a necessidade de auxílio emergencial."
    dgtm.maintain_graph(new_data)
    print(f"[{datetime.now()}] Teste de Manutenção - RGL Atualizado: {dgtm.text_graph}")

    # 4. Teste de Processamento
    queries = [
        ("recomendar política", "crise"),
        ("recomendar política", "crime"),
        ("otimizar ambiente", "verão")
    ]
    for query, context in queries:
        result, explanation = dgtm.process_query(query, context)
        print(f"[{datetime.now()}] Teste de Processamento - Consulta: {query}, Contexto: {context}")
        print(f"[{datetime.now()}] Resultado: {result}")
        print(f"[{datetime.now()}] Explicação: {explanation}")

    # 5. Teste de Salvamento/Carregamento
    dgtm.save_model("dgtm_model.json")
    dgtm_new = DynamicGraphTextualModel()
    dgtm_new.load_model("dgtm_model.json")
    result, explanation = dgtm_new.process_query("recomendar política", "crise")
    print(f"[{datetime.now()}] Teste de Salvamento/Carregamento - Resultado: {result}")
    print(f"[{datetime.now()}] Explicação: {explanation}")

if __name__ == "__main__":
    run_tests()
