import os
import csv
import time
import threading
from datetime import datetime, time as dt_time
from concurrent.futures import ThreadPoolExecutor

from neo_api_client import NeoAPI
import config


# ============================================================
# CONFIGURATION
# ============================================================

LIVE_TRADING = False

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

TRADE_LOG_FILE = "trade_log.csv"


# ============================================================
# GLOBAL STATE
# ============================================================

bot_running = False
bot_thread = None

client = None
login_done = False

state_lock = threading.Lock()

total_profit = 0.0
completed_trades = 0

current_trade = None

last_error = None


# ============================================================
# STATE
# ============================================================

def get_state():

    with state_lock:

        return {
            "running": bot_running,
            "total_profit": round(total_profit, 2),
            "completed_trades": completed_trades,
            "current_trade": current_trade,
            "login_done": login_done,
            "error": last_error
        }


# ============================================================
# LOGGING
# ============================================================

def log_trade(
    trade_number,
    ce_symbol,
    pe_symbol,
    ce_entry,
    pe_entry,
    ce_exit,
    pe_exit,
    profit,
    cumulative_profit
):

    file_exists = os.path.exists(TRADE_LOG_FILE)

    with open(
        TRADE_LOG_FILE,
        "a",
        newline="",
        encoding="utf-8"
    ) as f:

        writer = csv.writer(f)

        if not file_exists:

            writer.writerow([
                "time",
                "trade_number",
                "ce_symbol",
                "pe_symbol",
                "ce_entry",
                "pe_entry",
                "ce_exit",
                "pe_exit",
                "profit",
                "cumulative_profit"
            ])

        writer.writerow([
            datetime.now().strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
            trade_number,
            ce_symbol,
            pe_symbol,
            ce_entry,
            pe_entry,
            ce_exit,
            pe_exit,
            round(profit, 2),
            round(cumulative_profit, 2)
        ])


# ============================================================
# LOGIN
# ============================================================

def login():

    global client
    global login_done
    global last_error

    try:

        client = NeoAPI(
            consumer_key=config.CONSUMER_KEY,
            environment="prod"
        )

        print("Logging in...")

        login_response = client.totp_login(
            mobile_number=config.MOBILE_NUMBER,
            ucc=config.UCC,
            totp=config.TOTP
        )

        print("TOTP login response:")
        print(login_response)

        validate_response = client.totp_validate(
            mpin=config.MPIN
        )

        print("MPIN validation response:")
        print(validate_response)

        login_done = True
        last_error = None

        print("Login successful.")

        return True

    except Exception as e:

        login_done = False
        last_error = str(e)

        print("LOGIN ERROR:", e)

        return False


# ============================================================
# OPTION CHAIN
# ============================================================

def get_option_chain():

    global last_error

    if client is None:

        last_error = (
            "NeoAPI client is None."
        )

        print(
            "OPTION CHAIN ERROR: client is None"
        )

        return None

    try:

        print()
        print("=" * 60)
        print("REQUESTING SENSEX OPTION CHAIN")
        print("=" * 60)

        response = client.option_chain(
            exchange=EXCHANGE,
            underlying=UNDERLYING,
            instrument_type="option",
            count=100
        )

        print(
            "Option-chain response type:",
            type(response).__name__
        )

        print(
            "Option-chain response:"
        )

        print(response)

        if response is None:

            last_error = (
                "NeoAPI returned None for option chain."
            )

            return None

        if isinstance(response, dict):

            print(
                "Response keys:",
                list(response.keys())
            )

            if response.get("stat") == "Not_Ok":

                last_error = (
                    "NeoAPI option-chain error: "
                    + str(response)
                )

                print(
                    last_error
                )

                return None

        return response

    except Exception as e:

        last_error = (
            f"Option chain error: "
            f"{type(e).__name__}: {e}"
        )

        print(
            last_error
        )

        print(
            traceback.format_exc()
        )

        return None
# ============================================================
# NORMALIZE OPTION CHAIN
# ============================================================

def extract_chain_rows(response):

    if not response:
        return []

    data = response.get("data")

    if isinstance(data, dict):

        data = data.get("data")

    if not isinstance(data, list):

        return []

    return data


# ============================================================
# NUMBER CONVERSION
# ============================================================

def to_float(value):

    try:

        return float(value)

    except:

        return None


# ============================================================
# FIND OTM OPTIONS NEAREST TO ₹10 PREMIUM
# ============================================================

def find_otm_options(response):

    rows = extract_chain_rows(response)

    if not rows:
        return None, None

    spot = None

    # --------------------------------------------------------
    # Try to identify spot
    # --------------------------------------------------------

    for row in rows:

        if not isinstance(row, dict):
            continue

        for key in (
            "spot",
            "spotPrice",
            "underlyingValue",
            "ltp"
        ):

            value = to_float(row.get(key))

            if value is not None and value > 1000:

                spot = value
                break

        if spot is not None:
            break

    # --------------------------------------------------------
    # If spot isn't available, use strike center
    # --------------------------------------------------------

    strikes = []

    for row in rows:

        if not isinstance(row, dict):
            continue

        strike = None

        for key in (
            "strikePrice",
            "strike",
            "stkPrc"
        ):

            strike = to_float(row.get(key))

            if strike is not None:
                break

        if strike is not None:
            strikes.append(strike)

    if spot is None and strikes:

        spot = sum(strikes) / len(strikes)

    if spot is None:

        print("Could not determine SENSEX spot.")

        return None, None

    calls = []
    puts = []

    # --------------------------------------------------------
    # Extract CE / PE
    # --------------------------------------------------------

    for row in rows:

        if not isinstance(row, dict):
            continue

        strike = None

        for key in (
            "strikePrice",
            "strike",
            "stkPrc"
        ):

            strike = to_float(row.get(key))

            if strike is not None:
                break

        if strike is None:
            continue

        # Possible formats from API responses
        ce = (
            row.get("CE")
            or row.get("ce")
            or row.get("call")
            or row.get("Call")
        )

        pe = (
            row.get("PE")
            or row.get("pe")
            or row.get("put")
            or row.get("Put")
        )

        if isinstance(ce, dict):

            ltp = (
                ce.get("ltp")
                or ce.get("LTP")
                or ce.get("lastPrice")
                or ce.get("last_traded_price")
            )

            symbol = (
                ce.get("tradingSymbol")
                or ce.get("trading_symbol")
                or ce.get("trdSym")
                or ce.get("symbol")
            )

            token = (
                ce.get("instrumentToken")
                or ce.get("instrument_token")
                or ce.get("tok")
            )

            ltp = to_float(ltp)

            if (
                ltp is not None
                and symbol
                and strike > spot
            ):

                calls.append({
                    "symbol": symbol,
                    "strike": strike,
                    "ltp": ltp,
                    "token": token
                })

        if isinstance(pe, dict):

            ltp = (
                pe.get("ltp")
                or pe.get("LTP")
                or pe.get("lastPrice")
                or pe.get("last_traded_price")
            )

            symbol = (
                pe.get("tradingSymbol")
                or pe.get("trading_symbol")
                or pe.get("trdSym")
                or pe.get("symbol")
            )

            token = (
                pe.get("instrumentToken")
                or pe.get("instrument_token")
                or pe.get("tok")
            )

            ltp = to_float(ltp)

            if (
                ltp is not None
                and symbol
                and strike < spot
            ):

                puts.append({
                    "symbol": symbol,
                    "strike": strike,
                    "ltp": ltp,
                    "token": token
                })

    if not calls or not puts:

        print("Could not find both OTM CE and PE.")

        return None, None

    # --------------------------------------------------------
    # IMPORTANT:
    #
    # Select the OTM CE and PE whose PREMIUM/LTP is closest
    # to ₹10.
    #
    # Secondary condition:
    # closest strike to spot.
    # --------------------------------------------------------

    calls.sort(
        key=lambda x: (
            abs(x["ltp"] - TARGET_OPTION_PREMIUM),
            abs(x["strike"] - spot)
        )
    )

    puts.sort(
        key=lambda x: (
            abs(x["ltp"] - TARGET_OPTION_PREMIUM),
            abs(x["strike"] - spot)
        )
    )

    return calls[0], puts[0]


# ============================================================
# QUOTE
# ============================================================

def get_ltp(option):

    if client is None:
        return None

    token = option.get("token")

    if not token:
        return None

    try:

        response = client.quotes(
            instrument_tokens=[
                {
                    "instrument_token": str(token),
                    "exchange_segment": EXCHANGE
                }
            ],
            quote_type="all"
        )

        return extract_ltp_from_quote(response)

    except Exception as e:

        print(
            "Quote error for",
            option.get("symbol"),
            e
        )

        return None


def extract_ltp_from_quote(response):

    if not response:
        return None

    data = response.get("data")

    if isinstance(data, dict):

        data = data.get("data") or data

    if isinstance(data, list) and data:

        data = data[0]

    if not isinstance(data, dict):

        return None

    for key in (
        "ltp",
        "LTP",
        "lastPrice",
        "last_traded_price",
        "lastTradedPrice",
        "lp"
    ):

        value = to_float(data.get(key))

        if value is not None:
            return value

    return None


# ============================================================
# PLACE BUY ORDER
# ============================================================

def buy_option(option):

    if not LIVE_TRADING:

        print(
            "PAPER BUY:",
            option["symbol"]
        )

        return {
            "success": True,
            "price": option["ltp"]
        }

    try:

        response = client.place_order(

            exchange_segment=EXCHANGE,

            product="NRML",

            price="0",

            order_type="MKT",

            quantity=str(LOT_SIZE),

            validity="DAY",

            trading_symbol=option["symbol"],

            transaction_type="B",

            tag="SENSEX_BOT"
        )

        print(
            "BUY RESPONSE:",
            response
        )

        price = get_ltp(option)

        if price is None:

            price = option["ltp"]

        return {
            "success": True,
            "price": price,
            "response": response
        }

    except Exception as e:

        print(
            "BUY ERROR:",
            option["symbol"],
            e
        )

        return {
            "success": False,
            "error": str(e)
        }


# ============================================================
# PLACE SELL ORDER
# ============================================================

def sell_option(option):

    if not LIVE_TRADING:

        print(
            "PAPER SELL:",
            option["symbol"]
        )

        return {
            "success": True,
            "price": option["ltp"]
        }

    try:

        response = client.place_order(

            exchange_segment=EXCHANGE,

            product="NRML",

            price="0",

            order_type="MKT",

            quantity=str(LOT_SIZE),

            validity="DAY",

            trading_symbol=option["symbol"],

            transaction_type="S",

            tag="SENSEX_BOT"
        )

        print(
            "SELL RESPONSE:",
            response
        )

        price = get_ltp(option)

        if price is None:

            price = option["ltp"]

        return {
            "success": True,
            "price": price,
            "response": response
        }

    except Exception as e:

        print(
            "SELL ERROR:",
            option["symbol"],
            e
        )

        return {
            "success": False,
            "error": str(e)
        }


# ============================================================
# TIME CHECK
# ============================================================

def current_time():

    return datetime.now().time()


def before_entry_cutoff():

    return current_time() < LAST_ENTRY_TIME


def force_exit_time_reached():

    return current_time() >= FORCE_EXIT_TIME


# ============================================================
# ENTER BOTH OPTIONS SIMULTANEOUSLY
# ============================================================

def enter_both(ce, pe):

    print()
    print("==============================================")
    print("BUYING CE + PE")
    print("==============================================")

    with ThreadPoolExecutor(
        max_workers=2
    ) as executor:

        ce_future = executor.submit(
            buy_option,
            ce
        )

        pe_future = executor.submit(
            buy_option,
            pe
        )

        ce_result = ce_future.result()
        pe_result = pe_future.result()

    if not ce_result.get("success"):
        return None

    if not pe_result.get("success"):

        # CE was bought but PE failed.
        # Attempt to close CE immediately.

        print(
            "PE BUY FAILED. Closing CE."
        )

        sell_option(ce)

        return None

    return {
        "ce_entry": float(
            ce_result["price"]
        ),

        "pe_entry": float(
            pe_result["price"]
        )
    }


# ============================================================
# EXIT BOTH OPTIONS SIMULTANEOUSLY
# ============================================================

def exit_both(ce, pe):

    print()
    print("==============================================")
    print("EXITING CE + PE")
    print("==============================================")

    with ThreadPoolExecutor(
        max_workers=2
    ) as executor:

        ce_future = executor.submit(
            sell_option,
            ce
        )

        pe_future = executor.submit(
            sell_option,
            pe
        )

        ce_result = ce_future.result()
        pe_result = pe_future.result()

    return ce_result, pe_result


# ============================================================
# TRADE LOOP
# ============================================================

def run_trade():

    global total_profit
    global completed_trades
    global current_trade
    global last_error

    # --------------------------------------------------------
    # Get option chain
    # --------------------------------------------------------

    chain = get_option_chain()

    if chain is None:

        return False

    ce, pe = find_otm_options(chain)

    if ce is None or pe is None:

        return False

    print()
    print("SELECTED CE:")
    print(ce)

    print()
    print("SELECTED PE:")
    print(pe)

    # --------------------------------------------------------
    # Enter both
    # --------------------------------------------------------

    entry = enter_both(
        ce,
        pe
    )

    if entry is None:

        return False

    ce_entry = entry["ce_entry"]
    pe_entry = entry["pe_entry"]

    with state_lock:

        current_trade = {
            "ce_symbol": ce["symbol"],
            "pe_symbol": pe["symbol"],
            "ce_entry": ce_entry,
            "pe_entry": pe_entry,
            "combined_profit": 0.0
        }

    print()
    print(
        "CE ENTRY:",
        ce_entry
    )

    print(
        "PE ENTRY:",
        pe_entry
    )

    # --------------------------------------------------------
    # Monitor combined P&L
    # --------------------------------------------------------

    while bot_running:

        if force_exit_time_reached():

            print(
                "FORCE EXIT TIME REACHED."
            )

            break

        ce_ltp = get_ltp(ce)
        pe_ltp = get_ltp(pe)

        if ce_ltp is None or pe_ltp is None:

            time.sleep(
                CHAIN_REFRESH_SECONDS
            )

            continue

        # ----------------------------------------------------
        # Since both legs were BUY positions:
        #
        # profit = (current - entry) * quantity
        # ----------------------------------------------------

        ce_profit = (
            ce_ltp - ce_entry
        ) * LOT_SIZE

        pe_profit = (
            pe_ltp - pe_entry
        ) * LOT_SIZE

        combined_profit = (
            ce_profit + pe_profit
        )

        with state_lock:

            if current_trade is not None:

                current_trade[
                    "ce_ltp"
                ] = ce_ltp

                current_trade[
                    "pe_ltp"
                ] = pe_ltp

                current_trade[
                    "combined_profit"
                ] = round(
                    combined_profit,
                    2
                )

        print(
            "CE:",
            round(ce_ltp, 2),
            "| PE:",
            round(pe_ltp, 2),
            "| Combined P&L:",
            round(combined_profit, 2)
        )

        # ----------------------------------------------------
        # TARGET
        # ----------------------------------------------------

        if combined_profit >= TARGET_PROFIT:

            print(
                "TARGET PROFIT REACHED:",
                combined_profit
            )

            break

        # ----------------------------------------------------
        # STOP LOSS
        # ----------------------------------------------------

        if combined_profit <= STOP_LOSS:

            print(
                "STOP LOSS REACHED:",
                combined_profit
            )

            break

        time.sleep(
            CHAIN_REFRESH_SECONDS
        )

    # --------------------------------------------------------
    # EXIT BOTH
    # --------------------------------------------------------

    ce_result, pe_result = exit_both(
        ce,
        pe
    )

    ce_exit = ce_result.get(
        "price",
        ce_ltp
    )

    pe_exit = pe_result.get(
        "price",
        pe_ltp
    )

    ce_exit = float(ce_exit)
    pe_exit = float(pe_exit)

    # --------------------------------------------------------
    # Final realized estimate
    # --------------------------------------------------------

    ce_final_profit = (
        ce_exit - ce_entry
    ) * LOT_SIZE

    pe_final_profit = (
        pe_exit - pe_entry
    ) * LOT_SIZE

    trade_profit = (
        ce_final_profit +
        pe_final_profit
    )

    total_profit += trade_profit

    completed_trades += 1

    trade_number = completed_trades

    log_trade(
        trade_number,
        ce["symbol"],
        pe["symbol"],
        ce_entry,
        pe_entry,
        ce_exit,
        pe_exit,
        trade_profit,
        total_profit
    )

    print()
    print("==============================================")
    print("TRADE COMPLETED")
    print("Trade:", trade_number)
    print("Profit:", round(trade_profit, 2))
    print("Total:", round(total_profit, 2))
    print("==============================================")

    with state_lock:

        current_trade = None

    # --------------------------------------------------------
    # Check total target
    # --------------------------------------------------------

    if total_profit >= TOTAL_PROFIT_TARGET:

        print()
        print(
            "TOTAL PROFIT TARGET REACHED."
        )

        return True

    return True


# ============================================================
# MAIN BOT WORKER
# ============================================================

def bot_worker():

    global bot_running
    global last_error

    print()
    print("==============================================")
    print("SENSEX OPTION BOT STARTED")
    print("==============================================")

    try:

        if not login():

            bot_running = False

            return

        while bot_running:

            # ------------------------------------------------
            # Stop after total target
            # ------------------------------------------------

            if total_profit >= TOTAL_PROFIT_TARGET:

                print(
                    "₹1000 TOTAL PROFIT TARGET REACHED."
                )

                break

            # ------------------------------------------------
            # Do not enter after 15:00
            # ------------------------------------------------

            if not before_entry_cutoff():

                print(
                    "Entry time finished. Waiting for exit time."
                )

                if force_exit_time_reached():

                    break

                time.sleep(30)

                continue

            # ------------------------------------------------
            # One trade at a time
            # ------------------------------------------------

            if current_trade is not None:

                time.sleep(1)

                continue

            try:

                run_trade()

            except Exception as e:

                last_error = str(e)

                print(
                    "TRADE ERROR:",
                    e
                )

                time.sleep(5)

    except Exception as e:

        last_error = str(e)

        print(
            "BOT WORKER ERROR:",
            e
        )

    finally:

        bot_running = False

        with state_lock:

            current_trade = None

        print()
        print(
            "BOT STOPPED."
        )


# ============================================================
# START BOT
# ============================================================

def start_bot():

    global bot_running
    global bot_thread
    global total_profit
    global completed_trades
    global last_error

    with state_lock:

        if bot_running:

            return {
                "status": "already_running",
                "message": "Bot is already running."
            }

        total_profit = 0.0
        completed_trades = 0

        last_error = None

        bot_running = True

    bot_thread = threading.Thread(
        target=bot_worker,
        daemon=True
    )

    bot_thread.start()

    return {
        "status": "started",
        "message": "Bot started."
    }


# ============================================================
# STOP BOT
# ============================================================

def stop_bot():

    global bot_running

    with state_lock:

        if not bot_running:

            return {
                "status": "already_stopped",
                "message": "Bot is already stopped."
            }

        bot_running = False

    return {
        "status": "stopping",
        "message": "Stop signal sent to bot."
    }
