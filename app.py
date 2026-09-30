from flask import Flask, jsonify, render_template

import importlib.util
import os
import sys


# ============================================================
# FLASK APP
# ============================================================

app = Flask(__name__)


# ============================================================
# LOAD KOTAK BOT MODULE
# ============================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

BOT_FILE = os.path.join(
    BASE_DIR,
    "kotak neo bot.py"
)

spec = importlib.util.spec_from_file_location(
    "kotak_bot",
    BOT_FILE
)

if spec is None or spec.loader is None:
    raise ImportError(
        f"Could not load bot file: {BOT_FILE}"
    )

bot = importlib.util.module_from_spec(spec)

sys.modules["kotak_bot"] = bot

spec.loader.exec_module(bot)


# ============================================================
# HOME PAGE
# ============================================================

@app.route("/")
def index():

    return render_template("index.html")


# ============================================================
# BOT STATUS
# ============================================================

@app.route("/api/status", methods=["GET"])
def status():

    try:

        return jsonify(
            bot.get_state()
        )

    except Exception as e:

        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


# ============================================================
# START BOT
# ============================================================

@app.route("/api/start", methods=["POST"])
def start():

    try:

        result = bot.start_bot()

        return jsonify(result)

    except Exception as e:

        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


# ============================================================
# STOP BOT
# ============================================================

@app.route("/api/stop", methods=["POST"])
def stop():

    try:

        result = bot.stop_bot()

        return jsonify(result)

    except Exception as e:

        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


# ============================================================
# LOCAL SERVER
# ============================================================

if __name__ == "__main__":

    print()
    print("==============================================")
    print("        SENSEX OPTION BOT DASHBOARD")
    print("==============================================")
    print()
    print("Open in browser:")
    print("http://127.0.0.1:5000")
    print()

    app.run(
        host="0.0.0.0",
        port=5000,
        debug=False,
        threaded=True
    )
