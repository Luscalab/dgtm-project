import json
import random
from transformers import pipeline
import networkx as nx


class DynamicGraphTextModel:
    def __init__(self, model_name="distilbert-base-uncased"):
        try:
            self.model_path = f"./models/{model_name}"
            self.neural_model = pipeline("text2text-generation", model=self.model_path)
        except Exception:
            self.neural_model = pipeline("text2text-generation", model=model_name)

        self.graph = nx.DiGraph()
        self.text_graph = ""
        self.token_dict = {}
        self.reverse_token_dict = {}
        self.load_model()

    def _get_token(self, word):
        if word not in self.token_dict:
            token_id = str(len(self.token_dict) + 1)
            self.token_dict[word] = token_id
            self.reverse_token_dict[token_id] = word
        return self.token_dict[word]

    def convert_raw_data(self, data):
        simplified = self.neural_model(data, max_length=50, num_return_sequences=1)[0]['generated_text']
        parts = simplified.split(">")
        if len(parts) < 2:
            parts = ["justiça", "priorizar recursos", "crise"]
        main_node, sub_node = parts[0].strip(), parts[1].split("(")[0].strip()
        condition = parts[1].split(",")[-1].replace(")", "").strip()
        prob = str(random.uniform(0.7, 0.95))[:4]
        main_node_t = self._get_token(main_node)
        sub_node_t = self._get_token(sub_node)
        condition_t = self._get_token(condition)
        self.text_graph += f"{main_node_t}:{sub_node_t}>{condition_t}(p{prob},{condition_t});"
        self.save_model('dgtm_model.json')

    def maintain_graph(self, new_data):
        self.convert_raw_data(new
