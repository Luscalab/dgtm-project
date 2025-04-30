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

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
