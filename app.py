from flask import Flask, request, jsonify, render_template_string
from dgtm import DynamicGraphTextModel

app = Flask(__name__)
dgtm = DynamicGraphTextModel(model_name="distilbert-base-uncased")


HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head><title>DGTM Web</title></head>
<body>
<h1>DGTM - Dynamic Graph Text Model</h1>

<h2>Ensinar</h2>
<form id="teach-form">
  <input type="text" id="data-input" placeholder="Digite algo novo..." style="width: 300px;">
  <button type="submit">Ensinar</button>
</form>
<p id="teach-result"></p>

<h2>Perguntar</h2>
<form id="query-form">
  <input type="text" id="query-input" placeholder="Pergunta..." style="width: 200px;">
  <input type="text" id="context-input" placeholder="Contexto..." style="width: 200px;">
  <button type="submit">Perguntar</button>
</form>
<p id="query-result"></p>

<h2>Ver Regras</h2>
<button onclick="fetchRules()">Mostrar Regras</button>
<pre id="rules-display"></pre>

<script>
document.getElementById("teach-form").addEventListener("submit", function(e) {
  e.preventDefault();
  let data = document.getElementById("data-input").value;
  fetch("/teach", {
    method: "POST",
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({data: data})
  }).then(res => res.json()).then(data => {
    document.getElementById("teach-result").innerText = data.message || data.error;
  });
});

document.getElementById("query-form").addEventListener("submit", function(e) {
  e.preventDefault();
  let query = document.getElementById("query-input").value;
  let context = document.getElementById("context-input").value;
  fetch(`/query?query=${encodeURIComponent(query)}&context=${encodeURIComponent(context)}`)
  .then(res => res.json())
  .then(data => {
    document.getElementById("query-result").innerText = `Resultado: ${data.result}\nExplicação: ${data.explanation}`;
  });
});

function fetchRules() {
  fetch("/rules")
  .then(res => res.json())
  .then(data => {
    document.getElementById("rules-display").innerText = data.rules.join("\\n");
  });
}
</script>
</body>
</html>
"""


@app.route("/")
def home():
    return render_template_string(HTML_TEMPLATE)


@app.route("/query", methods=["GET"])
def query():
    query = request.args.get("query", "recomendar política")
    context = request.args.get("context", "default")
    result, explanation = dgtm.process_query(query, context)
    return jsonify({"result": result, "explanation": explanation})


@app.route("/teach", methods=["POST"])
def teach():
    data = request.json.get("data", "")
    if not data:
        return jsonify({"error": "Nenhum dado fornecido"}), 400
    old_rgl = dgtm.text_graph
    if "new graph" in data.lower():
        dgtm.convert_raw_data(data)
    else:
        dgtm.maintain_graph(data)
    dgtm.save_model()
    new_rgl = dgtm.text_graph
    return jsonify({
        "message": f"DGTM aprendeu com: {data}",
        "old_rgl": old_rgl,
        "new_rgl": new_rgl
    })


@app.route("/rules", methods=["GET"])
def get_rules():
    rules = dgtm.text_graph.split(";") if dgtm.text_graph else []
    return jsonify({"rules": rules})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
