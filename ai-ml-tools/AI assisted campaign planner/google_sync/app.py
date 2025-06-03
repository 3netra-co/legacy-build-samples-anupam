from flask import Flask, jsonify
from trigger_api import run_full_process
from add_details import run_detail_enhancer  # Import your new function

app = Flask(__name__)

@app.route('/generate-campaign', methods=['GET', 'POST'])
def generate_campaign():
    result = run_full_process()
    return jsonify({"status": result})

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000)
