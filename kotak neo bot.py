import os
import csv
import time
import threading
import traceback
from datetime import datetime, time as dt_time
from concurrent.futures import ThreadPoolExecutor

from neo_api_client import NeoAPI

import config


# ============================================================
# CONFIGURATION
# ============================================================

LIVE_TRADING = True

UNDERLYING = "SENSEX"
EXCHANGE = "bse_fo"

TARGET_OPTION_PREMIUM = 10.0

TARGET_PROFIT = 5.0
STOP_LOSS = -20.0
TOTAL_PROFIT_TARGET = 1000.0

LOT_SIZE = 20

LAST_ENTRY_TIME = dt_time(15, 0)
FORCE_EXIT_TIME = dt_time(15, 15)

CHAIN_REFRESH_SECONDS = 5
REST_DELAY = 0.5

BASE_DIR = os.path.dirname(
    os.path.abspath(__file__)
)

TRADE_LOG_FILE = os.path.join(
    BASE_DIR,
    "trade_log.csv"
)


# ============================================================
# GLOBAL STATE
# ============================================================

client = None

bot_running = False
bot_thread = None

state_lock = threading.Lock()

active_trade = None


# ============================================================
# BOT STATE
# ============================================================

bot_state = {
    "running": False,
    "mode": "LIVE" if LIVE_TRADING else "PAPER",
    "status": "Stopped",
    "message": "",
    "last_error": "",
    "last_update": "",

    "spot": 0.0,
    "expiry": "",

    # --------------------------------------------------------
    # CURRENT ₹10 CALL
    # --------------------------------------------------------

    "call_target_strike": 0.0,
    "call_strike": 0.0,
    "call_symbol": "",
    "call_ltp": 0.0,

    # --------------------------------------------------------
    # CURRENT ₹10 PUT
    # --------------------------------------------------------

    "put_target_strike": 0.0,
    "put_strike": 0.0,
    "put_symbol": "",
    "put_ltp": 0.0,

    # --------------------------------------------------------
    # ACTIVE TRADE
    # --------------------------------------------------------

    "trigger_side": "",
    "active_trade": False,

    "trade_number": 0,

    "trade_call_symbol": "",
    "trade_put_symbol": "",

    "call_entry": 0.0,
    "put_entry": 0.0,

    "call_current": 0.0,
    "put_current": 0.0,

    "call_quantity": 0,
    "put_quantity": 0,

    "combined_pnl": 0.0,

    # --------------------------------------------------------
    # PROFIT TRACKING
    # --------------------------------------------------------

    "completed_trades": 0,
    "last_trade_profit": 0.0,
    "total_profit": 0.0,
    "last_trade_time": "",
    "max_trades": 0
}


# ============================================================
# LOGGING
# ============================================================

def log(message):

    timestamp = datetime.now().strftime("%H:%M:%S")

    print(f"[{timestamp}] {message}")

    with state_lock:
        bot_state["message"] = str(message)
        bot_state["last_update"] = timestamp


# ============================================================
# UPDATE STATE
# ============================================================

def update_state(**kwargs):

    with state_lock:

        for key, value in kwargs.items():

            if key in bot_state:
                bot_state[key] = value

        bot_state["last_update"] = (
            datetime.now().strftime("%H:%M:%S")
        )


# ============================================================
# GET STATE
# ============================================================

def get_state():

    with state_lock:
        return dict(bot_state)


# ============================================================
# SAFE FLOAT
# ============================================================

def safe_float(value, default=0.0):

    try:

        if value is None:
            return default

        return float(value)

    except Exception:
        return default


# ============================================================
# LOGIN
# ============================================================

def login():

    global client

    try:

        log("Logging in to NeoAPI...")

        client = NeoAPI(
            environment="prod",
            consumer_key=config.CONSUMER_KEY
        )

        # ----------------------------------------------------
        # TOTP LOGIN
        # ----------------------------------------------------

        response = client.totp_login(
            mobile_number=config.MOBILE_NUMBER,
            ucc=config.UCC,
            totp=config.TOTP
        )

        log(
            f"TOTP login response: {response}"
        )

        # ----------------------------------------------------
        # MPIN VALIDATION
        # ----------------------------------------------------

        response = client.totp_validate(
            mpin=config.MPIN
        )

        log(
            f"TOTP validation response: {response}"
        )

        # ----------------------------------------------------
        # CHECK RESPONSE
        # ----------------------------------------------------

        if isinstance(response, dict):

            if response.get("stat") == "Not_Ok":

                error_message = (
                    response.get("emsg")
                    or response.get("message")
                    or str(response)
                )

                raise Exception(
                    f"MPIN validation failed: {error_message}"
                )

        log("Login successful.")

        update_state(
            status="Logged in",
            last_error=""
        )

        return True

    except Exception as e:

        error_message = (
            f"{type(e).__name__}: {e}"
        )

        log(
            f"LOGIN FAILED: {error_message}"
        )

        update_state(
            status="Login failed",
            last_error=error_message
        )

        client = None

        return False


# ============================================================
# GET NEAREST EXPIRY
# ============================================================

def get_nearest_expiry():

    try:

        if client is None:
            return None

        response = client.expiries(
            exchange=EXCHANGE,
            underlying=UNDERLYING
        )

        log(
            f"Expiry response: {response}"
        )

        if not isinstance(response, dict):
            return None

        expiries = response.get(
            "expiries",
            []
        )

        if not expiries:
            return None

        # ----------------------------------------------------
        # Convert expiry values to strings for safe sorting
        # ----------------------------------------------------

        expiries = [
            str(expiry)
            for expiry in expiries
            if expiry is not None
        ]

        if not expiries:
            return None

        nearest = sorted(expiries)[0]

        log(
            f"Nearest expiry: {nearest}"
        )

        return nearest

    except Exception as e:

        error_message = (
            f"{type(e).__name__}: {e}"
        )

        log(
            f"EXPIRY API ERROR: {error_message}"
        )

        update_state(
            last_error=error_message
        )

        return None


# ============================================================
# GET OPTION CHAIN
# ============================================================

def get_option_chain(expiry=None):

    try:

        if client is None:

            log(
                "Option chain requested but client is None."
            )

            return None

        response = client.option_chain(
            exchange=EXCHANGE,
            underlying=UNDERLYING,
            expiry=expiry,
            instrument_type="option",
            count=100
        )

        log(
            f"Option chain response type: "
            f"{type(response).__name__}"
        )

        # ----------------------------------------------------
        # API ERROR
        # ----------------------------------------------------

        if isinstance(response, dict):

            if response.get("stat") == "Not_Ok":

                log(
                    f"Option chain API error: {response}"
                )

                update_state(
                    last_error=str(response)
                )

                return None

        else:

            log(
                "Option-chain response is not a dictionary."
            )

            return None

        # ----------------------------------------------------
        # DIRECT FORMAT
        # ----------------------------------------------------

        if (
            "calls" in response
            or "puts" in response
            or "spot" in response
        ):

            return response

        # ----------------------------------------------------
        # DATA FORMAT
        # ----------------------------------------------------

        data = response.get("data")

        if isinstance(data, dict):

            return data

        # ----------------------------------------------------
        # DEBUG
        # ----------------------------------------------------

        log(
            "Could not find option-chain data. "
            f"Keys: {list(response.keys())}"
        )

        return None

    except Exception as e:

        error_message = (
            f"{type(e).__name__}: {e}"
        )

        log(
            f"OPTION CHAIN ERROR: {error_message}"
        )

        print(
            traceback.format_exc()
        )

        update_state(
            last_error=error_message
        )

        return None


# ============================================================
# GET SENSEX SPOT
# ============================================================

def get_sensex_spot(chain_data):

    try:

        if not isinstance(chain_data, dict):
            return 0.0

        spot_data = chain_data.get(
            "spot",
            {}
        )

        if isinstance(spot_data, dict):

            spot = safe_float(
                spot_data.get("ltp")
            )

            if spot > 0:
                return spot

        # ----------------------------------------------------
        # Alternative spot formats
        # ----------------------------------------------------

        for key in (
            "spotPrice",
            "underlyingValue",
            "ltp"
        ):

            value = safe_float(
                chain_data.get(key)
            )

            if value > 1000:
                return value

        log(
            "SENSEX spot not found."
        )

        return 0.0

    except Exception as e:

        log(
            f"Spot parsing error: {e}"
        )

        return 0.0


# ============================================================
# PARSE OPTION CHAIN
# ============================================================

def parse_option_chain(chain_data):

    calls = []
    puts = []

    try:

        if not isinstance(chain_data, dict):

            log(
                "ERROR: chain_data is not a dictionary."
            )

            return [], []

        option_records = []

        # ----------------------------------------------------
        # DIRECT CALLS
        # ----------------------------------------------------

        if isinstance(
            chain_data.get("calls"),
            list
        ):

            option_records.extend(
                chain_data["calls"]
            )

        # ----------------------------------------------------
        # DIRECT PUTS
        # ----------------------------------------------------

        if isinstance(
            chain_data.get("puts"),
            list
        ):

            option_records.extend(
                chain_data["puts"]
            )

        # ----------------------------------------------------
        # DATA LIST
        # ----------------------------------------------------

        data = chain_data.get("data")

        if isinstance(data, list):

            option_records.extend(data)

        # ----------------------------------------------------
        # SEARCH NESTED LISTS
        # ----------------------------------------------------

        if not option_records:

            for value in chain_data.values():

                if isinstance(value, list):

                    for item in value:

                        if not isinstance(item, dict):
                            continue

                        if "inst" in item:

                            option_records.append(item)

        log(
            f"Option records discovered: "
            f"{len(option_records)}"
        )

        # ----------------------------------------------------
        # REMOVE DUPLICATES
        # ----------------------------------------------------

        unique_records = []

        seen = set()

        for item in option_records:

            if not isinstance(item, dict):
                continue

            inst = item.get(
                "inst",
                {}
            )

            if not isinstance(inst, dict):
                continue

            neo_symbol = inst.get(
                "neoSymbol",
                ""
            )

            if (
                neo_symbol
                and neo_symbol not in seen
            ):

                seen.add(neo_symbol)

                unique_records.append(item)

        # ----------------------------------------------------
        # PARSE OPTIONS
        # ----------------------------------------------------

        for item in unique_records:

            try:

                inst = item.get(
                    "inst",
                    {}
                )

                quote = item.get(
                    "quote",
                    {}
                )

                if not isinstance(inst, dict):
                    continue

                if not isinstance(quote, dict):
                    quote = {}

                strike = safe_float(
                    inst.get("strkPrc")
                )

                ltp = safe_float(
                    quote.get("ltp")
                )

                symbol = inst.get(
                    "symbol",
                    ""
                )

                neo_symbol = inst.get(
                    "neoSymbol",
                    ""
                )

                opt_type = str(
                    inst.get(
                        "optType",
                        ""
                    )
                ).upper()

                if (
                    strike <= 0
                    or not symbol
                    or not neo_symbol
                ):
                    continue

                if ltp <= 0:
                    continue

                option = {
                    "strike": strike,
                    "ltp": ltp,
                    "neoSymbol": neo_symbol,
                    "symbol": symbol,
                    "optType": opt_type
                }

                if opt_type == "CE":

                    calls.append(option)

                elif opt_type == "PE":

                    puts.append(option)

            except Exception as e:

                log(
                    f"Option item parse error: {e}"
                )

        log(
            f"Parsed option chain | "
            f"CALLs={len(calls)} | "
            f"PUTs={len(puts)}"
        )

        return calls, puts

    except Exception as e:

        log(
            f"OPTION PARSER ERROR: {e}"
        )

        print(
            traceback.format_exc()
        )

        return [], []


# ============================================================
# FIND OTM OPTION CLOSEST TO ₹10
# ============================================================

def find_otm_closest_to_premium(
    options,
    spot,
    option_type,
    target_premium
):

    valid_options = []

    for option in options:

        strike = safe_float(
            option.get("strike")
        )

        ltp = safe_float(
            option.get("ltp")
        )

        if strike <= 0:
            continue

        if ltp <= 0:
            continue

        # ----------------------------------------------------
        # CALL OTM
        # ----------------------------------------------------

        if option_type == "CE":

            if strike > spot:

                valid_options.append(
                    option
                )

        # ----------------------------------------------------
        # PUT OTM
        # ----------------------------------------------------

        elif option_type == "PE":

            if strike < spot:

                valid_options.append(
                    option
                )

    if not valid_options:
        return None

    # --------------------------------------------------------
    # PRIMARY:
    # Premium closest to ₹10
    #
    # SECONDARY:
    # Strike closest to spot
    # --------------------------------------------------------

    return min(
        valid_options,
        key=lambda x: (
            abs(
                x["ltp"] - target_premium
            ),
            abs(
                x["strike"] - spot
            )
        )
    )


# ============================================================
# GET CURRENT LTP
# ============================================================

def get_current_ltp(neo_symbol):

    try:

        if client is None:
            return 0.0

        if not neo_symbol:
            return 0.0

        parts = str(
            neo_symbol
        ).split("|")

        if len(parts) != 2:

            log(
                f"Invalid neoSymbol: {neo_symbol}"
            )

            return 0.0

        exchange_segment = parts[0]
        instrument_token = parts[1]

        instrument_tokens = [
            {
                "instrument_token": str(
                    instrument_token
                ),
                "exchange_segment":
                    exchange_segment
            }
        ]

        response = client.quotes(
            instrument_tokens=instrument_tokens,
            quote_type="ltp"
        )

        # ----------------------------------------------------
        # DICTIONARY RESPONSE
        # ----------------------------------------------------

        if isinstance(response, dict):

            data = response.get(
                "data"
            )

            if isinstance(data, list):

                if data:

                    quote = data[0]

                    if isinstance(
                        quote,
                        dict
                    ):

                        value = (
                            quote.get("ltp")
                            or quote.get("lp")
                            or quote.get(
                                "lastTradedPrice"
                            )
                            or quote.get(
                                "lastPrice"
                            )
                        )

                        price = safe_float(
                            value
                        )

                        if price > 0:
                            return price

            if isinstance(data, dict):

                value = (
                    data.get("ltp")
                    or data.get("lp")
                    or data.get(
                        "lastTradedPrice"
                    )
                    or data.get(
                        "lastPrice"
                    )
                )

                price = safe_float(
                    value
                )

                if price > 0:
                    return price

            value = (
                response.get("ltp")
                or response.get("lp")
                or response.get(
                    "lastTradedPrice"
                )
            )

            price = safe_float(
                value
            )

            if price > 0:
                return price

        # ----------------------------------------------------
        # LIST RESPONSE
        # ----------------------------------------------------

        if isinstance(response, list):

            if response:

                quote = response[0]

                if isinstance(
                    quote,
                    dict
                ):

                    value = (
                        quote.get("ltp")
                        or quote.get("lp")
                        or quote.get(
                            "lastTradedPrice"
                        )
                        or quote.get(
                            "lastPrice"
                        )
                    )

                    price = safe_float(
                        value
                    )

                    if price > 0:
                        return price

        return 0.0

    except Exception as e:

        log(
            f"LTP error for "
            f"{neo_symbol}: {e}"
        )

        update_state(
            last_error=str(e)
        )

        return 0.0


# ============================================================
# LOT SIZE
# ============================================================

def get_lot_size(option):

    return LOT_SIZE


# ============================================================
# PLACE MARKET ORDER
# ============================================================

def place_market_order(
    option,
    quantity,
    transaction_type
):

    if not option:
        return None

    symbol = option.get(
        "symbol",
        ""
    )

    neo_symbol = option.get(
        "neoSymbol",
        ""
    )

    action = (
        "BUY"
        if transaction_type == "B"
        else "SELL"
    )

    log(
        f"{action} {symbol} "
        f"Qty={quantity}"
    )

    # ========================================================
    # PAPER TRADING
    # ========================================================

    if not LIVE_TRADING:

        log(
            f"PAPER ORDER: "
            f"{action} {symbol} x {quantity}"
        )

        return {
            "stat": "Ok",
            "status": "PAPER",
            "symbol": symbol,
            "neoSymbol": neo_symbol,
            "quantity": quantity
        }

    # ========================================================
    # LIVE ORDER
    # ========================================================

    try:

        response = client.place_order(
            exchange_segment=EXCHANGE,
            product="MIS",
            price="0",
            order_type="MKT",
            quantity=str(quantity),
            validity="DAY",
            trading_symbol=symbol,
            transaction_type=transaction_type
        )

        log(
            f"{action} response: {response}"
        )

        if isinstance(
            response,
            dict
        ):

            if response.get(
                "stat"
            ) == "Not_Ok":

                log(
                    f"{action} order failed: "
                    f"{response}"
                )

                return None

        return response

    except Exception as e:

        log(
            f"Order placement error "
            f"for {symbol}: {e}"
        )

        update_state(
            last_error=str(e)
        )

        return None


# ============================================================
# GET ENTRY PRICE
# ============================================================

def get_entry_price(option):

    price = get_current_ltp(
        option["neoSymbol"]
    )

    if price <= 0:

        price = safe_float(
            option.get("ltp")
        )

    return price


# ============================================================
# CALCULATE COMBINED P&L
# ============================================================

def calculate_combined_pnl():

    global active_trade

    if active_trade is None:
        return 0.0

    call_ltp = get_current_ltp(
        active_trade["call"]["neoSymbol"]
    )

    time.sleep(
        REST_DELAY
    )

    put_ltp = get_current_ltp(
        active_trade["put"]["neoSymbol"]
    )

    if call_ltp <= 0 or put_ltp <= 0:

        return None

    active_trade["call_current"] = (
        call_ltp
    )

    active_trade["put_current"] = (
        put_ltp
    )

    call_pnl = (
        call_ltp
        - active_trade["call_entry"]
    ) * active_trade["call_quantity"]

    put_pnl = (
        put_ltp
        - active_trade["put_entry"]
    ) * active_trade["put_quantity"]

    combined_pnl = (
        call_pnl
        + put_pnl
    )

    update_state(
        call_current=call_ltp,
        put_current=put_ltp,
        combined_pnl=round(
            combined_pnl,
            2
        )
    )

    return combined_pnl


# ============================================================
# ENTER PAIR
# ============================================================

def enter_pair(call, put):

    global active_trade

    if active_trade is not None:

        log(
            "Trade already active. "
            "New entry blocked."
        )

        return False

    call_quantity = get_lot_size(
        call
    )

    put_quantity = get_lot_size(
        put
    )

    log(
        "=============================================="
    )

    log(
        "ENTERING CALL + PUT"
    )

    log(
        f"CALL: {call['symbol']} "
        f"@ approx ₹{call['ltp']:.2f}"
    )

    log(
        f"PUT: {put['symbol']} "
        f"@ approx ₹{put['ltp']:.2f}"
    )

    # ========================================================
    # BUY BOTH AS CLOSE TO SIMULTANEOUS AS POSSIBLE
    # ========================================================

    with ThreadPoolExecutor(
        max_workers=2
    ) as executor:

        call_future = executor.submit(
            place_market_order,
            call,
            call_quantity,
            "B"
        )

        put_future = executor.submit(
            place_market_order,
            put,
            put_quantity,
            "B"
        )

        call_order = call_future.result()

        put_order = put_future.result()

    # ========================================================
    # CALL FAILED
    # ========================================================

    if not call_order:

        log(
            "CALL order failed."
        )

        # If PUT unexpectedly succeeded,
        # close it immediately.

        if put_order:

            log(
                "PUT succeeded while CALL failed. "
                "Closing PUT."
            )

            place_market_order(
                put,
                put_quantity,
                "S"
            )

        return False

    # ========================================================
    # PUT FAILED
    # ========================================================

    if not put_order:

        log(
            "PUT order failed."
        )

        log(
            "CALL was already bought. "
            "Closing CALL."
        )

        place_market_order(
            call,
            call_quantity,
            "S"
        )

        return False

    # ========================================================
    # GET ACTUAL ENTRY PRICES
    # ========================================================

    call_entry = get_entry_price(
        call
    )

    time.sleep(
        REST_DELAY
    )

    put_entry = get_entry_price(
        put
    )

    if call_entry <= 0:

        call_entry = safe_float(
            call["ltp"]
        )

    if put_entry <= 0:

        put_entry = safe_float(
            put["ltp"]
        )

    # ========================================================
    # TRADE NUMBER
    # ========================================================

    with state_lock:

        trade_number = (
            bot_state["completed_trades"]
            + 1
        )

    # ========================================================
    # CREATE ACTIVE TRADE
    # ========================================================

    active_trade = {

        "trade_number": trade_number,

        "call": call,

        "put": put,

        "call_entry": call_entry,

        "put_entry": put_entry,

        "call_current": call_entry,

        "put_current": put_entry,

        "call_quantity": call_quantity,

        "put_quantity": put_quantity,

        "entry_time": datetime.now()
    }

    update_state(

        active_trade=True,

        trade_number=trade_number,

        trade_call_symbol=call["symbol"],

        trade_put_symbol=put["symbol"],

        call_entry=call_entry,

        put_entry=put_entry,

        call_current=call_entry,

        put_current=put_entry,

        call_quantity=call_quantity,

        put_quantity=put_quantity,

        combined_pnl=0.0,

        trigger_side="CALL + PUT"
    )

    log(
        f"Trade #{trade_number} ENTERED | "
        f"CALL={call['symbol']} @ ₹{call_entry:.2f} | "
        f"PUT={put['symbol']} @ ₹{put_entry:.2f}"
    )

    return True


# ============================================================
# EXIT ACTIVE PAIR
# ============================================================

def exit_pair(reason):

    global active_trade

    if active_trade is None:

        return False

    trade = active_trade

    log(
        "=============================================="
    )

    log(
        f"EXITING TRADE #{trade['trade_number']}"
    )

    log(
        f"Reason: {reason}"
    )

    # ========================================================
    # SELL BOTH AS CLOSE TO SIMULTANEOUS AS POSSIBLE
    # ========================================================

    with ThreadPoolExecutor(
        max_workers=2
    ) as executor:

        call_future = executor.submit(
            place_market_order,
            trade["call"],
            trade["call_quantity"],
            "S"
        )

        put_future = executor.submit(
            place_market_order,
            trade["put"],
            trade["put_quantity"],
            "S"
        )

        call_order = call_future.result()

        put_order = put_future.result()

    if not call_order:

        log(
            "WARNING: CALL exit order failed."
        )

    if not put_order:

        log(
            "WARNING: PUT exit order failed."
        )

    # ========================================================
    # GET EXIT PRICES
    # ========================================================

    call_exit = get_current_ltp(
        trade["call"]["neoSymbol"]
    )

    time.sleep(
        REST_DELAY
    )

    put_exit = get_current_ltp(
        trade["put"]["neoSymbol"]
    )

    # --------------------------------------------------------
    # FALLBACK
    # --------------------------------------------------------

    if call_exit <= 0:

        call_exit = trade[
            "call_current"
        ]

    if put_exit <= 0:

        put_exit = trade[
            "put_current"
        ]

    # ========================================================
    # FINAL P&L
    # ========================================================

    call_profit = (
        call_exit
        - trade["call_entry"]
    ) * trade["call_quantity"]

    put_profit = (
        put_exit
        - trade["put_entry"]
    ) * trade["put_quantity"]

    combined_profit = (
        call_profit
        + put_profit
    )

    # ========================================================
    # UPDATE TOTAL PROFIT
    # ========================================================

    with state_lock:

        bot_state["completed_trades"] += 1

        bot_state["last_trade_profit"] = (
            round(
                combined_profit,
                2
            )
        )

        bot_state["total_profit"] += (
            combined_profit
        )

        bot_state["last_trade_time"] = (
            datetime.now().strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        )

        bot_state["combined_pnl"] = (
            round(
                combined_profit,
                2
            )
        )

        total_profit = (
            bot_state["total_profit"]
        )

    # ========================================================
    # LOG TRADE
    # ========================================================

    log_trade(
        trade_number=trade[
            "trade_number"
        ],

        call_symbol=trade[
            "call"
        ]["symbol"],

        put_symbol=trade[
            "put"
        ]["symbol"],

        call_entry=trade[
            "call_entry"
        ],

        put_entry=trade[
            "put_entry"
        ],

        call_exit=call_exit,

        put_exit=put_exit,

        call_quantity=trade[
            "call_quantity"
        ],

        put_quantity=trade[
            "put_quantity"
        ],

        combined_profit=combined_profit
    )

    log(
        f"Trade #{trade['trade_number']} CLOSED | "
        f"CALL EXIT=₹{call_exit:.2f} | "
        f"PUT EXIT=₹{put_exit:.2f} | "
        f"Trade P&L=₹{combined_profit:.2f} | "
        f"Total=₹{total_profit:.2f}"
    )

    # ========================================================
    # CLEAR ACTIVE TRADE
    # ========================================================

    active_trade = None

    update_state(

        active_trade=False,

        trade_call_symbol="",

        trade_put_symbol="",

        call_entry=0.0,

        put_entry=0.0,

        call_current=0.0,

        put_current=0.0,

        call_quantity=0,

        put_quantity=0,

        combined_pnl=0.0,

        trigger_side=""
    )

    return True


# ============================================================
# FORCE EXIT
# ============================================================

def force_exit():

    if active_trade is not None:

        exit_pair(
            "Force exit time"
        )


# ============================================================
# RESET OPTION DISPLAY
# ============================================================

def clear_option_display():

    update_state(

        call_target_strike=0.0,

        call_strike=0.0,

        call_symbol="",

        call_ltp=0.0,

        put_target_strike=0.0,

        put_strike=0.0,

        put_symbol="",

        put_ltp=0.0
    )


# ============================================================
# LOG TRADE TO CSV
# ============================================================

def log_trade(
    trade_number,
    call_symbol,
    put_symbol,
    call_entry,
    put_entry,
    call_exit,
    put_exit,
    call_quantity,
    put_quantity,
    combined_profit
):

    file_exists = os.path.exists(
        TRADE_LOG_FILE
    )

    try:

        with open(
            TRADE_LOG_FILE,
            "a",
            newline="",
            encoding="utf-8"
        ) as file:

            writer = csv.writer(file)

            if not file_exists:

                writer.writerow([
                    "trade_number",
                    "date",
                    "time",
                    "call_symbol",
                    "put_symbol",
                    "call_entry",
                    "put_entry",
                    "call_exit",
                    "put_exit",
                    "call_quantity",
                    "put_quantity",
                    "combined_profit",
                    "cumulative_profit"
                ])

            now = datetime.now()

            with state_lock:

                cumulative_profit = (
                    bot_state[
                        "total_profit"
                    ]
                )

            writer.writerow([

                trade_number,

                now.strftime(
                    "%Y-%m-%d"
                ),

                now.strftime(
                    "%H:%M:%S"
                ),

                call_symbol,

                put_symbol,

                round(
                    call_entry,
                    2
                ),

                round(
                    put_entry,
                    2
                ),

                round(
                    call_exit,
                    2
                ),

                round(
                    put_exit,
                    2
                ),

                call_quantity,

                put_quantity,

                round(
                    combined_profit,
                    2
                ),

                round(
                    cumulative_profit,
                    2
                )
            ])

    except Exception as e:

        log(
            f"Trade log error: {e}"
        )


# ============================================================
# BOT WORKER
# ============================================================

def bot_worker():

    global bot_running

    log(
        "=============================================="
    )

    log(
        "SENSEX OPTION BOT WORKER STARTED"
    )

    log(
        f"Mode: "
        f"{'LIVE' if LIVE_TRADING else 'PAPER'}"
    )

    log(
        f"Target premium: ₹{TARGET_OPTION_PREMIUM}"
    )

    log(
        f"Pair target: ₹{TARGET_PROFIT}"
    )

    log(
        f"Stop loss: ₹{STOP_LOSS}"
    )

    log(
        f"Total target: ₹{TOTAL_PROFIT_TARGET}"
    )

    # ========================================================
    # LOGIN
    # ========================================================

    if not login():

        with state_lock:

            bot_running = False

            bot_state["running"] = False

            bot_state["status"] = (
                "Login failed"
            )

        return

    update_state(

        status="Running",

        message="Bot is running.",

        running=True
    )

    log(
        "Bot is now RUNNING."
    )

    # ========================================================
    # MAIN LOOP
    # ========================================================

    while bot_running:

        try:

            now = datetime.now().time()

            # =================================================
            # FORCE EXIT
            # =================================================

            if now >= FORCE_EXIT_TIME:

                if active_trade is not None:

                    force_exit()

                log(
                    "Force exit time reached."
                )

                break

            # =================================================
            # CUMULATIVE TARGET
            # =================================================

            with state_lock:

                total_profit = (
                    bot_state[
                        "total_profit"
                    ]
                )

            if (
                total_profit
                >= TOTAL_PROFIT_TARGET
            ):

                if active_trade is not None:

                    exit_pair(
                        "Cumulative profit target"
                    )

                log(
                    "₹1000 cumulative profit "
                    "target reached."
                )

                update_state(

                    status="Target reached",

                    message=(
                        "Cumulative profit "
                        "target reached."
                    )
                )

                break

            # =================================================
            # ACTIVE TRADE
            # =================================================

            if active_trade is not None:

                combined_pnl = (
                    calculate_combined_pnl()
                )

                if combined_pnl is not None:

                    log(
                        f"Trade #"
                        f"{active_trade['trade_number']} "
                        f"P&L = "
                        f"₹{combined_pnl:.2f}"
                    )

                    # -----------------------------------------
                    # TARGET
                    # -----------------------------------------

                    if (
                        combined_pnl
                        >= TARGET_PROFIT
                    ):

                        exit_pair(
                            "Combined profit target"
                        )

                        time.sleep(
                            REST_DELAY
                        )

                        continue

                    # -----------------------------------------
                    # STOP LOSS
                    # -----------------------------------------

                    if (
                        combined_pnl
                        <= STOP_LOSS
                    ):

                        exit_pair(
                            "Combined stop loss"
                        )

                        time.sleep(
                            REST_DELAY
                        )

                        continue

                time.sleep(
                    CHAIN_REFRESH_SECONDS
                )

                continue

            # =================================================
            # NO ACTIVE TRADE
            # =================================================

            # -------------------------------------------------
            # NO ENTRY AFTER 15:00
            # -------------------------------------------------

            if now >= LAST_ENTRY_TIME:

                log(
                    "Last entry time reached. "
                    "No new trades."
                )

                time.sleep(
                    CHAIN_REFRESH_SECONDS
                )

                continue

            # =================================================
            # GET EXPIRY
            # =================================================

            expiry = (
                get_nearest_expiry()
            )

            if not expiry:

                log(
                    "Unable to get expiry."
                )

                time.sleep(
                    CHAIN_REFRESH_SECONDS
                )

                continue

            update_state(
                expiry=expiry
            )

            # =================================================
            # GET OPTION CHAIN
            # =================================================

            chain_data = (
                get_option_chain(
                    expiry
                )
            )

            if not chain_data:

                log(
                    "Unable to get option chain."
                )

                time.sleep(
                    CHAIN_REFRESH_SECONDS
                )

                continue

            # =================================================
            # GET SPOT
            # =================================================

            spot = (
                get_sensex_spot(
                    chain_data
                )
            )

            if spot <= 0:

                log(
                    "Invalid SENSEX spot."
                )

                time.sleep(
                    CHAIN_REFRESH_SECONDS
                )

                continue

            update_state(
                spot=spot
            )

            # =================================================
            # PARSE OPTIONS
            # =================================================

            calls, puts = (
                parse_option_chain(
                    chain_data
                )
            )

            if not calls or not puts:

                log(
                    "CALL or PUT data unavailable."
                )

                time.sleep(
                    CHAIN_REFRESH_SECONDS
                )

                continue

            # =================================================
            # FIND ₹10 OTM CALL
            # =================================================

            call = (
                find_otm_closest_to_premium(
                    calls,
                    spot,
                    "CE",
                    TARGET_OPTION_PREMIUM
                )
            )

            # =================================================
            # FIND ₹10 OTM PUT
            # =================================================

            put = (
                find_otm_closest_to_premium(
                    puts,
                    spot,
                    "PE",
                    TARGET_OPTION_PREMIUM
                )
            )

            if not call or not put:

                log(
                    "Unable to find suitable "
                    "OTM CALL and PUT."
                )

                time.sleep(
                    CHAIN_REFRESH_SECONDS
                )

                continue

            # =================================================
            # UPDATE DASHBOARD
            # =================================================

            update_state(

                call_target_strike=call[
                    "strike"
                ],

                call_strike=call[
                    "strike"
                ],

                call_symbol=call[
                    "symbol"
                ],

                call_ltp=call[
                    "ltp"
                ],

                put_target_strike=put[
                    "strike"
                ],

                put_strike=put[
                    "strike"
                ],

                put_symbol=put[
                    "symbol"
                ],

                put_ltp=put[
                    "ltp"
                ]
            )

            log(
                f"₹10 candidates | "
                f"CALL {call['symbol']} "
                f"₹{call['ltp']:.2f} | "
                f"PUT {put['symbol']} "
                f"₹{put['ltp']:.2f} | "
                f"SPOT {spot:.2f}"
            )

            # =================================================
            # ENTER PAIR
            # =================================================

            enter_pair(
                call,
                put
            )

            time.sleep(
                CHAIN_REFRESH_SECONDS
            )

        except Exception as e:

            error_message = (
                f"{type(e).__name__}: {e}"
            )

            log(
                f"BOT LOOP ERROR: "
                f"{error_message}"
            )

            print(
                traceback.format_exc()
            )

            update_state(
                last_error=error_message
            )

            time.sleep(
                CHAIN_REFRESH_SECONDS
            )

    # ========================================================
    # FINAL CLEANUP
    # ========================================================

    with state_lock:

        bot_running = False

        bot_state["running"] = False

        if (
            bot_state["status"]
            != "Target reached"
        ):

            bot_state["status"] = (
                "Stopped"
            )

    log(
        "Bot worker stopped."
    )


# ============================================================
# START BOT
# ============================================================

def start_bot():

    global bot_running
    global bot_thread

    with state_lock:

        if bot_running:

            return {
                "success": False,
                "message":
                    "Bot is already running."
            }

        bot_running = True

        bot_state["running"] = True

        bot_state["status"] = (
            "Starting"
        )

        bot_state["message"] = (
            "Bot is starting..."
        )

        bot_state["last_error"] = ""

        bot_state["last_update"] = (
            datetime.now().strftime(
                "%H:%M:%S"
            )
        )

    bot_thread = threading.Thread(
        target=bot_worker,
        daemon=True
    )

    bot_thread.start()

    log(
        "Bot start command accepted."
    )

    return {
        "success": True,
        "message":
            "Bot started successfully."
    }


# ============================================================
# STOP BOT
# ============================================================

def stop_bot():

    global bot_running

    with state_lock:

        if not bot_running:

            return {
                "success": False,
                "message":
                    "Bot is already stopped."
            }

        bot_running = False

        bot_state["running"] = False

        bot_state["status"] = (
            "Stopping"
        )

        bot_state["message"] = (
            "Bot is stopping..."
        )

    # ========================================================
    # CLOSE ACTIVE TRADE
    # ========================================================

    try:

        if active_trade is not None:

            exit_pair(
                "Manual bot stop"
            )

    except Exception as e:

        log(
            f"Error closing active trade: {e}"
        )

    log(
        "Bot stop command accepted."
    )

    return {
        "success": True,
        "message":
            "Bot stopped successfully."
    }


# ============================================================
# DIRECT RUN
# ============================================================

if __name__ == "__main__":

    print()
    print("=" * 60)
    print("              SENSEX OPTION BOT")
    print("=" * 60)
    print()

    print(
        f"Mode: "
        f"{'LIVE' if LIVE_TRADING else 'PAPER'}"
    )

    print(
        f"Target premium: "
        f"₹{TARGET_OPTION_PREMIUM}"
    )

    print(
        f"Pair profit target: "
        f"₹{TARGET_PROFIT}"
    )

    print(
        f"Stop loss: "
        f"₹{STOP_LOSS}"
    )

    print(
        f"Total profit target: "
        f"₹{TOTAL_PROFIT_TARGET}"
    )

    print(
        f"Lot size: "
        f"{LOT_SIZE}"
    )

    print()

    start_bot()

    try:

        while bot_running:

            time.sleep(1)

    except KeyboardInterrupt:

        print()
        print(
            "Stopping bot..."
        )

        stop_bot()
