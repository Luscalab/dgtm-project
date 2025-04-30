from flask import Flask, request, jsonify
from dgtm import DynamicGraphTextModel

app = Flask(__name__)
model = DynamicGraphTextModel(model_name="t5-small")

@app.route("/process", methods=["POST"])
def process():
    data = request.json.get("text", "")
    result = model.process_query(data)
    return jsonify({"response": result[0], "explanation": result[1]})

@app.route("/rules", methods=["GET"])
def get_rules():
    with open("rules.txt", "r") as f:
        rules = f.read()
    return jsonify({"rules": rules.split("\n")})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
