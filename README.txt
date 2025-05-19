DGTM - Deep Graph Thought Mapping
Simulating Human Thought through Semantic Extraction and Cognitive Graphs
Overview
The DGTM (Deep Graph Thought Mapping) is an innovative project that simulates human thought processes using semantic extraction, symbolic representation, and cognitive graphs. Developed by Lucas Santos de Souza (GitHub: Luscalab), it combines Natural Language Processing (NLP), conversational AI, and knowledge graphs to model thoughts, emotions, and intentions. The system extracts entities and relationships, stores them in a compressed graph using Zstandard level 15, and provides adaptive responses.
Objectives

Simulate human mind functioning through text.
Reconstruct reasoning and narratives as semantic graphs.
Deliver coherent and effective conversational responses.
Optimize storage and processing with advanced compression.

Features

Text Extraction: Utilizes spaCy 3.7.2 to extract entities and relationships.
Symbolic Encoding: Entities encoded with 16-bit values, mapped to symbols (e.g., Lucas → #0001).
Compressed Graph: Stored in graph_data.zc with Zstandard level 15, featuring an index for fast node access.
Adaptive Chat: Responses based on intentions, sentiments, and context.

Requirements

Python 3.12
Dependencies: Flask 2.3.3, spaCy 3.7.2, networkx, textblob, zstandard
spaCy Model: pt_core_news_sm

Installation
pip install flask==2.3.3 spacy==3.7.2 networkx textblob zstandard
python -m spacy download pt_core_news_sm

Project Structure
dgtm_project/
├── app.py
├── manual.json
├── graph_data.zc
├── graph_index.json
├── simbolos.json
├── templates/
│   ├── index.html
│   ├── update_manual.html
│   └── semantic_map.html
└── venv/

How to Run

Activate the virtual environment:C:\Users\Lucas\Desktop\dgtm_project\venv\Scripts\activate


Start the server:python app.py


Access:
Chat: http://127.0.0.1:5000/
Semantic Map: http://127.0.0.1:5000/semantic_map
Manual: http://127.0.0.1:5000/update_manual



Usage Example

User: "Hi, I'm happy today!"
Bot: "Hello! I'm glad to see you! I'm happy you're excited! 😊"
User: "My name is Lucas and I live in São Paulo."
Bot: "Got it, you mentioned Lucas! - Entity 'Lucas' (symbol: #0001)..."

Performance

Current Usage: 650 MB storage, 600-800 MB RAM, 20-30% CPU per request.
Scalability: Viable for data comparable to BERT (88 MB compressed) or GPT-4 (20 GB compressed) with node indexing, using <2 GB RAM and 10-20 ms per query.

License
This project is licensed under the MIT License. See the LICENSE file for details.
References

Zstandard Documentation
Knowledge Graphs in NLP @ ACL 2019
Neo4j Conversational AI
DeepPavlov

Contact
For questions or contributions, contact Lucas Santos de Souza at Luscalab.
