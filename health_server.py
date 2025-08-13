#!/usr/bin/env python3
from flask import Flask, jsonify
app = Flask(__name__)
@app.get(/health)
def health():
    return jsonify({"status": "ok"})
if __name__ == "__main__":
    import os
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
