```python
from flask import Flask, jsonify, render_template
import importlib.util
import os
import sys
import traceback


# ============================================================
# FLASK APP
# ============================================================

app = Flask(__name__)


# ============================================================
# PATHS
# ============================================================

BASE_DIR = os.path.dirname(
    os.path.abspath(__file__)
)

BOT_FILE = os.path.join(
    BASE_DIR,
    "kotak neo bot.py"
)


# ============================================================
# LOAD BOT
# ============================================================

def load_bot():

    if not os.path.exists(BOT_FILE):

        raise FileNotFoundError(
            f"Bot file not found: {BOT_FILE}"
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

    required_functions = [
        "get_state",
        "start_bot",
        "stop_bot"
    ]

    missing = [
        function
        for function in required_functions
        if not hasattr(bot, function)
    ]

    if missing:

        raise AttributeError(
            "Missing bot functions: "
            + ", ".join(missing)
        )

    return bot


# ============================================================
# LOAD BOT MODULE
# ============================================================

try:

    bot = load_bot()

    BOT_LOAD_ERROR = None

except Exception as e:

    bot = None

    BOT_LOAD_ERROR = (
        f"{type(e).__name__}: {str(e)}"
    )


# ============================================================
# HOME
# ============================================================

@app.route("/")
def index():

    return render_template(
        "index.html"
    )


# ============================================================
# STATUS
# ============================================================

@app.route(
    "/api/status",
    methods=["GET"]
)
def status():

    try:

        if bot is None:

            return jsonify({
                "running": False,
                "login_done": False,
                "error": BOT_LOAD_ERROR
            }), 500

        return jsonify(
            bot.get_state()
        )

    except Exception as e:

        return jsonify({
            "running": False,
            "error": str(e)
        }), 500


# ============================================================
# START BOT
# ============================================================

@app.route(
    "/api/start",
    methods=["POST"]
)
def start():

    try:

        if bot is None:

            return jsonify({
                "status": "error",
                "message": BOT_LOAD_ERROR
            }), 500

        result = bot.start_bot()

        return jsonify(result)

    except Exception as e:

        print(
            traceback.format_exc()
        )

        return jsonify({
            "status": "error",
            "message": str(e),
            "type": type(e).__name__
        }), 500


# ============================================================
# STOP BOT
# ============================================================

@app.route(
    "/api/stop",
    methods=["POST"]
)
def stop():

    try:

        if bot is None:

            return jsonify({
                "status": "error",
                "message": BOT_LOAD_ERROR
            }), 500

        result = bot.stop_bot()

        return jsonify(result)

    except Exception as e:

        print(
            traceback.format_exc()
        )

        return jsonify({
            "status": "error",
            "message": str(e),
            "type": type(e).__name__
        }), 500


# ============================================================
# HEALTH
# ============================================================

@app.route(
    "/api/health",
    methods=["GET"]
)
def health():

    return jsonify({

        "server": "running",

        "bot_loaded": bot is not None,

        "bot_load_error": BOT_LOAD_ERROR,

        "get_state": (
            hasattr(bot, "get_state")
            if bot
            else False
        ),

        "start_bot": (
            hasattr(bot, "start_bot")
            if bot
            else False
        ),

        "stop_bot": (
            hasattr(bot, "stop_bot")
            if bot
            else False
        )
    })


# ============================================================
# RUN SERVER
# ============================================================

if __name__ == "__main__":

    print()
    print("=" * 55)
    print("        SENSEX OPTION BOT DASHBOARD")
    print("=" * 55)
    print()

    print(
        f"Bot file: {BOT_FILE}"
    )

    print()

    print(
        "Server starting..."
    )

    app.run(
        host="0.0.0.0",
        port=int(
            os.environ.get(
                "PORT",
                5000
            )
        ),
        debug=False,
        threaded=True
    )
```
