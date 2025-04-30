from flask import Flask, request
from dgtm import DynamicGraphTextualModel

app = Flask(__name__)
dgtm = DynamicGraphTextualModel()

@app.route('/query', methods=['GET'])
def query():
    query = request.args.get('query', 'recomendar política')
    context = request.args.get('context', 'crise')
    result, explanation = dgtm.process_query(query, context)
    return {'result': result, 'explanation': explanation}

@app.route('/teach', methods=['POST'])
def teach():
    new_data = request.json.get('data', '')
    if not new_data:
        return {'error': 'Nenhum dado fornecido'}, 400
    try:
        # Guarda o RGL antes de ensinar
        old_rgl = dgtm.text_graph
        # Tenta converter dados brutos ou atualizar o grafo
        if "new graph" in new_data.lower():
            dgtm.convert_raw_data(new_data)
        else:
            dgtm.maintain_graph(new_data)
        # Salva o modelo atualizado
        dgtm.save_model('dgtm_model.json')
        # Mostra o novo RGL
        new_rgl = dgtm.text_graph
        return {
            'message': f'DGTM aprendeu com: {new_data}',
            'old_rgl': old_rgl,
            'new_rgl': new_rgl
        }
    except Exception as e:
        return {'error': str(e)}, 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
