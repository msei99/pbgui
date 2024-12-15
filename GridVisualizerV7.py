import streamlit as st
from enum import Enum
from math import floor, ceil
from dataclasses import dataclass, field, replace
from enum import Enum
import math

# ----------------------------
# Enums and Data Classes
# ----------------------------

class OrderType(Enum):
    EntryInitialNormalLong = 1
    EntryInitialPartialLong = 2
    EntryGridNormalLong = 3
    EntryGridCroppedLong = 4
    EntryGridInflatedLong = 5
    EntryTrailingNormalLong = 6
    EntryTrailingCroppedLong = 7

    EntryInitialNormalShort = 8
    EntryInitialPartialShort = 9
    EntryGridNormalShort = 10
    EntryGridCroppedShort = 11
    EntryGridInflatedShort = 12
    EntryTrailingNormalShort = 13
    EntryTrailingCroppedShort = 14

    CloseGridLong = 15
    CloseGridShort = 16
    CloseTrailingLong = 17
    CloseTrailingShort = 18

    Default = 0

class Side(Enum):
    Long = 1
    Short = 2

class GridTrailingMode(Enum):
    Unknown = 0
    GridOnly = 1
    TrailingOnly = 2
    GridFirst = 3
    TrailingFirst = 4

@dataclass
class Order:
    qty: float = 0.0
    price: float = 0.0
    order_type: OrderType = OrderType.Default

    @staticmethod
    def default():
        return Order()

@dataclass
class Position:
    size: float = 0.0
    price: float = 0.0

@dataclass
class EmaBands:
    lower: float = 0.0
    upper: float = 0.0

@dataclass
class OrderBook:
    bid: float = 0.0
    ask: float = 0.0

@dataclass
class StateParams:
    balance: float = 0.0
    order_book: OrderBook = field(default_factory=OrderBook)
    ema_bands: EmaBands = field(default_factory=EmaBands)

    def clone(self):
        # Returns a new StateParams with copied fields
        return replace(
            self,
            order_book=OrderBook(self.order_book.bid, self.order_book.ask),
            ema_bands=EmaBands(self.ema_bands.lower, self.ema_bands.upper)
        )

@dataclass
class BotParams:
    wallet_exposure_limit: float = 0.0
    entry_initial_qty_pct: float = 0.0
    entry_initial_ema_dist: float = 0.0
    entry_grid_spacing_pct: float = 0.0
    entry_grid_spacing_weight: float = 0.0
    entry_grid_double_down_factor: float = 0.0
    entry_trailing_threshold_pct: float = 0.0
    entry_trailing_retracement_pct: float = 0.0
    entry_trailing_grid_ratio: float = 0.0

    close_grid_min_markup: float = 0.0
    close_grid_markup_range: float = 0.0
    close_grid_qty_pct: float = 0.0
    close_trailing_threshold_pct: float = 0.0
    close_trailing_retracement_pct: float = 0.0
    close_trailing_qty_pct: float = 0.0
    close_trailing_grid_ratio: float = 0.0

    def clone(self):
        return BotParams(
            wallet_exposure_limit=self.wallet_exposure_limit,
            entry_initial_qty_pct=self.entry_initial_qty_pct,
            entry_initial_ema_dist=self.entry_initial_ema_dist,
            entry_grid_spacing_pct=self.entry_grid_spacing_pct,
            entry_grid_spacing_weight=self.entry_grid_spacing_weight,
            entry_grid_double_down_factor=self.entry_grid_double_down_factor,
            entry_trailing_threshold_pct=self.entry_trailing_threshold_pct,
            entry_trailing_retracement_pct=self.entry_trailing_retracement_pct,
            entry_trailing_grid_ratio=self.entry_trailing_grid_ratio,

            close_grid_min_markup=self.close_grid_min_markup,
            close_grid_markup_range=self.close_grid_markup_range,
            close_grid_qty_pct=self.close_grid_qty_pct,
            close_trailing_threshold_pct=self.close_trailing_threshold_pct,
            close_trailing_retracement_pct=self.close_trailing_retracement_pct,
            close_trailing_qty_pct=self.close_trailing_qty_pct,
            close_trailing_grid_ratio=self.close_trailing_grid_ratio,
        )

@dataclass
class ExchangeParams:
    min_qty: float = 0.0
    min_cost: float = 0.0
    qty_step: float = 0.0
    price_step: float = 0.0
    c_mult: float = 1.0

@dataclass
class TrailingPriceBundle:
    max_since_open: float = 0.0
    min_since_open: float = 0.0
    max_since_min: float = 0.0
    min_since_max: float = 0.0

# ----------------------------
# Utility Functions
# ----------------------------

def round_to_decimal_places(value: float, decimal_places: int = 10) -> float:
    return round(value, decimal_places)


def round_up(n: float, step: float) -> float:
    if step == 0.0:
        return round_to_decimal_places(n)
    result = math.ceil(n / step) * step
    return round_to_decimal_places(result, 10)


def round_dn(n: float, step: float) -> float:
    if step == 0.0:
        return round_to_decimal_places(n)
    result = math.floor(n / step) * step
    return round_to_decimal_places(result, 10)


def round_(n: float, step: float) -> float:
    if step == 0.0:
        return round_to_decimal_places(n)
    result = round(n / step) * step
    return round_to_decimal_places(result, 10)


def round_dynamic(n: float, d: int) -> float:
    if n == 0.0:
        return 0.0
    shift = d - math.floor(math.log10(abs(n))) - 1
    multiplier = 10 ** shift
    result = round(n * multiplier) / multiplier
    return round_to_decimal_places(result, 10)


def round_dynamic_up(n: float, d: int) -> float:
    if n == 0.0:
        return 0.0
    shift = d - math.floor(math.log10(abs(n))) - 1
    multiplier = 10 ** shift
    result = math.ceil(n * multiplier) / multiplier
    return round_to_decimal_places(result, 10)


def round_dynamic_dn(n: float, d: int) -> float:
    if n == 0.0:
        return 0.0
    shift = d - math.floor(math.log10(abs(n))) - 1
    multiplier = 10 ** shift
    result = math.floor(n * multiplier) / multiplier
    return round_to_decimal_places(result, 10)


def calc_diff(x: float, y: float) -> float:
    if y == 0.0:
        return 0.0 if x == 0.0 else float('inf')
    return (abs(x - y) / abs(y))


def cost_to_qty(cost: float, price: float, c_mult: float) -> float:
    # Matches rust: cost / (price * c_mult), if price > 0 else 0.0
    if price <= 0.0:
        return 0.0
    return round_to_decimal_places((cost / (price * c_mult)), 10)


def qty_to_cost(qty: float, price: float, c_mult: float) -> float:
    # Matches rust: (qty.abs() * price) * c_mult
    return round_to_decimal_places(abs(qty) * price * c_mult, 10)


def calc_wallet_exposure(c_mult: float, balance: float, position_size: float, position_price: float) -> float:
    # Rust logic: if balance <=0 or position_size==0 return 0.0 else qty_to_cost(...) / balance
    if balance <= 0.0 or position_size == 0.0:
        return 0.0
    cost = qty_to_cost(position_size, position_price, c_mult)
    if balance == 0.0:
        return 0.0
    return round_to_decimal_places(cost / balance, 10)


def calc_new_psize_pprice(psize: float, pprice: float, qty: float, price: float, qty_step: float) -> tuple[float, float]:
    # If qty == 0.0, return as is
    if qty == 0.0:
        return (psize, pprice)

    if psize == 0.0:
        return (round_(qty, qty_step), price)

    new_psize = round_(psize + qty, qty_step)
    if new_psize == 0.0:
        return (0.0, 0.0)

    # Avoid NaN in Python:
    pprice = 0.0 if math.isnan(pprice) else pprice

    # Weighted average:
    new_pprice = (pprice * (psize / new_psize)) + (price * (qty / new_psize))
    return (round_to_decimal_places(new_psize, 10), round_to_decimal_places(new_pprice, 10))


def calc_wallet_exposure_if_filled(balance: float, psize: float, pprice: float, qty: float, price: float, exchange_params) -> float:
    # In rust: first round psize & qty abs with qty_step, then calc_new_psize_pprice, then calc_wallet_exposure
    psize_abs = round_(abs(psize), exchange_params.qty_step)
    qty_abs = round_(abs(qty), exchange_params.qty_step)
    (new_psize, new_pprice) = calc_new_psize_pprice(psize_abs, pprice, qty_abs, price, exchange_params.qty_step)
    return calc_wallet_exposure(exchange_params.c_mult, balance, new_psize, new_pprice)

def interpolate(x: float, xs: list[float], ys: list[float]) -> float:
    """
    Interpolates a value at x using Lagrange polynomial interpolation
    given arrays of x-coordinates (xs) and corresponding y-coordinates (ys).

    This function computes the Lagrange interpolation polynomial that passes
    through all the points defined by (xs[i], ys[i]) and returns the value at x.

    Parameters
    ----------
    x : float
        The x-coordinate at which to interpolate.
    xs : list[float]
        The x-coordinates of the data points. Must be distinct.
    ys : list[float]
        The y-coordinates of the data points.

    Returns
    -------
    float
        The interpolated value at x.

    Raises
    ------
    ValueError
        If xs and ys do not have the same length, or if they are empty,
        or if xs contains duplicate values.
    ZeroDivisionError
        If any two xs values are identical (which should be caught by the distinctness check).
    """

    if len(xs) != len(ys):
        raise ValueError("xs and ys must have the same length.")
    if len(xs) == 0:
        raise ValueError("xs and ys cannot be empty.")
    if len(set(xs)) != len(xs):
        raise ValueError("All xs must be distinct for Lagrange interpolation.")

    n = len(xs)
    result = 0.0

    for i in range(n):
        # Start with the corresponding y value
        term = ys[i]
        # Multiply by each fraction (x - xs[j]) / (xs[i] - xs[j]) for j != i
        for j in range(n):
            if i != j:
                denominator = xs[i] - xs[j]
                if denominator == 0:
                    raise ZeroDivisionError("Duplicate x-values found.")
                term *= (x - xs[j]) / denominator
        result += term

    return result


# def interpolate(x: float, xs: list[float], ys: list[float]) -> float:
#     if len(xs) != len(ys):
#         raise ValueError("xs and ys must have the same length")
#     n = len(xs)
#     result = 0.0
#     for i in range(n):
#         term = ys[i]
#         for j in range(n):
#             if i != j:
#                 term *= (x - xs[j]) / (xs[i] - xs[j])
#         result += term
#     return round_to_decimal_places(result, 10)

# def interpolate(x, xs, ys):
#     return np.sum(
#         np.array(
#             [
#                 np.prod(np.array([(x - xs[m]) / (xs[j] - xs[m]) for m in range(len(xs)) if m != j]))
#                 * ys[j]
#                 for j in range(len(xs))
#             ]
#         )
#     )
    
def calc_pnl_long(entry_price: float, close_price: float, qty: float, c_mult: float) -> float:
    # Matches rust: qty.abs()*c_mult*(close_price - entry_price)
    return round_to_decimal_places(abs(qty) * c_mult * (close_price - entry_price), 10)


def calc_pnl_short(entry_price: float, close_price: float, qty: float, c_mult: float) -> float:
    # Matches rust: qty.abs()*c_mult*(entry_price - close_price)
    return round_to_decimal_places(abs(qty) * c_mult * (entry_price - close_price), 10)


def calc_auto_unstuck_allowance(balance: float, loss_allowance_pct: float, pnl_cumsum_max: float, pnl_cumsum_last: float) -> float:
    # Rust: 
    # balance_peak = balance + (pnl_cumsum_max - pnl_cumsum_last)
    # drop_since_peak_pct = balance/balance_peak - 1.0
    # (balance_peak * (loss_allowance_pct + drop_since_peak_pct)).max(0.0)
    balance_peak = balance + (pnl_cumsum_max - pnl_cumsum_last)
    if balance_peak == 0.0:
        return 0.0
    drop_since_peak_pct = balance / balance_peak - 1.0
    val = balance_peak * (loss_allowance_pct + drop_since_peak_pct)
    return round_to_decimal_places(max(val, 0.0), 10)


def calc_ema_price_bid(price_step: float, order_book_bid: float, ema_bands_lower: float, ema_dist: float) -> float:
    # Matches Rust:
    # min(order_book_bid, round_dn(ema_bands_lower*(1-ema_dist), price_step))
    price = ema_bands_lower * (1.0 - ema_dist)
    price = round_dn(price, price_step)
    return min(order_book_bid, price)


def calc_ema_price_ask(price_step: float, order_book_ask: float, ema_bands_upper: float, ema_dist: float) -> float:
    # Matches Rust:
    # max(order_book_ask, round_up(ema_bands_upper*(1+ema_dist), price_step))
    price = ema_bands_upper * (1.0 + ema_dist)
    price = round_up(price, price_step)
    return max(order_book_ask, price)
# ----------------------------
# Shared Entry/Close Helpers
# ----------------------------

def calc_min_entry_qty(entry_price, exchange_params):
    return max(
        exchange_params.min_qty,
        round_up(
            cost_to_qty(
                exchange_params.min_cost,
                entry_price,
                exchange_params.c_mult,
            ),
            exchange_params.qty_step,
        ),
    )

# ----------------------------
# Entry Calculation Functions (Long)
# ----------------------------

def calc_initial_entry_qty(exchange_params, bot_params, balance, entry_price):
    return max(
        calc_min_entry_qty(entry_price, exchange_params),
        round_(
            cost_to_qty(
                balance * bot_params.wallet_exposure_limit * bot_params.entry_initial_qty_pct,
                entry_price,
                exchange_params.c_mult,
            ),
            exchange_params.qty_step,
        ),
    )

def calc_cropped_reentry_qty(
    exchange_params, bot_params, position, wallet_exposure, balance, entry_qty, entry_price
):
    position_size_abs = abs(position.size)
    entry_qty_abs = abs(entry_qty)
    wallet_exposure_if_filled_val = calc_wallet_exposure_if_filled(
        balance,
        position_size_abs,
        position.price,
        entry_qty_abs,
        entry_price,
        exchange_params,
    )
    min_entry_qty_val = calc_min_entry_qty(entry_price, exchange_params)
    if wallet_exposure_if_filled_val > bot_params.wallet_exposure_limit * 1.01:
        entry_qty_abs_new = interpolate(
            bot_params.wallet_exposure_limit,
            [wallet_exposure, wallet_exposure_if_filled_val],
            [position_size_abs, position_size_abs + entry_qty_abs],
        ) - position_size_abs
        return (
            wallet_exposure_if_filled_val,
            max(round_(entry_qty_abs_new, exchange_params.qty_step), min_entry_qty_val),
        )
    else:
        return (
            wallet_exposure_if_filled_val,
            max(entry_qty_abs, min_entry_qty_val),
        )

def calc_reentry_qty(entry_price, balance, position_size, exchange_params, bot_params):
    return max(
        calc_min_entry_qty(entry_price, exchange_params),
        round_(
            max(
                abs(position_size) * bot_params.entry_grid_double_down_factor,
                cost_to_qty(balance, entry_price, exchange_params.c_mult)
                * bot_params.wallet_exposure_limit
                * bot_params.entry_initial_qty_pct,
            ),
            exchange_params.qty_step,
        ),
    )

def calc_reentry_price_bid(position_price, wallet_exposure, order_book_bid, exchange_params, bot_params):
    #st.warning(f"position_price: {position_price}")
    multiplier = (wallet_exposure / bot_params.wallet_exposure_limit) * bot_params.entry_grid_spacing_weight
    #st.warning(f"multiplier: {multiplier}")
    #st.warning(f"bot_params.wallet_exposure_limit: {bot_params.wallet_exposure_limit}")
    #st.warning(f"bot_params.entry_grid_spacing_weight: {bot_params.entry_grid_spacing_weight}")
    reentry_price = min(
        round_dn(
            position_price * (1.0 - bot_params.entry_grid_spacing_pct * (1.0 + multiplier)),
            exchange_params.price_step,
        ),
        order_book_bid,
    )
    #st.error(f"reentry_price: {reentry_price}")
    if reentry_price <= exchange_params.price_step:
        return 0.0
    else:
        return reentry_price

def calc_reentry_price_ask(position_price, wallet_exposure, order_book_ask, exchange_params, bot_params):
    multiplier = (wallet_exposure / bot_params.wallet_exposure_limit) * bot_params.entry_grid_spacing_weight
    reentry_price = max(
        round_up(
            position_price * (1.0 + bot_params.entry_grid_spacing_pct * (1.0 + multiplier)),
            exchange_params.price_step,
        ),
        order_book_ask,
    )
    if reentry_price <= exchange_params.price_step:
        return 0.0
    else:
        return reentry_price

def calc_grid_entry_long(exchange_params, state_params, bot_params, position):
    if bot_params.wallet_exposure_limit == 0.0 or state_params.balance <= 0.0:
        return Order.default()
    initial_entry_price = calc_ema_price_bid(
        exchange_params.price_step,
        state_params.order_book.bid,
        state_params.ema_bands.lower,
        bot_params.entry_initial_ema_dist,
    )
    if initial_entry_price <= exchange_params.price_step:
        return Order.default()
    initial_entry_qty = calc_initial_entry_qty(
        exchange_params,
        bot_params,
        state_params.balance,
        initial_entry_price,
    )
    if position.size == 0.0:
        return Order(
            qty=initial_entry_qty,
            price=initial_entry_price,
            order_type=OrderType.EntryInitialNormalLong,
        )
    elif position.size < initial_entry_qty * 0.8:
        return Order(
            qty=max(
                calc_min_entry_qty(initial_entry_price, exchange_params),
                round_dn(initial_entry_qty - position.size, exchange_params.qty_step),
            ),
            price=initial_entry_price,
            order_type=OrderType.EntryInitialPartialLong,
        )
    wallet_exposure = calc_wallet_exposure(
        exchange_params.c_mult,
        state_params.balance,
        position.size,
        position.price,
    )
    if wallet_exposure >= bot_params.wallet_exposure_limit * 0.999:
        return Order.default()

    # normal re-entry
    reentry_price = calc_reentry_price_bid(
        position.price,
        wallet_exposure,
        state_params.order_book.bid,
        exchange_params,
        bot_params,
    )
    if reentry_price <= 0.0:
        return Order.default()
    reentry_qty = max(
        calc_reentry_qty(
            reentry_price,
            state_params.balance,
            position.size,
            exchange_params,
            bot_params,
        ),
        initial_entry_qty,
    )
    (wallet_exposure_if_filled, reentry_qty_cropped) = calc_cropped_reentry_qty(
        exchange_params,
        bot_params,
        position,
        wallet_exposure,
        state_params.balance,
        reentry_qty,
        reentry_price,
    )
    if reentry_qty_cropped < reentry_qty:
        return Order(
            qty=reentry_qty_cropped,
            price=reentry_price,
            order_type=OrderType.EntryGridCroppedLong,
        )

    # preview next order
    (psize_if_filled, pprice_if_filled) = calc_new_psize_pprice(
        position.size,
        position.price,
        reentry_qty,
        reentry_price,
        exchange_params.qty_step,
    )
    next_reentry_price = calc_reentry_price_bid(
        pprice_if_filled,
        wallet_exposure_if_filled,
        state_params.order_book.bid,
        exchange_params,
        bot_params,
    )
    next_reentry_qty = max(
        calc_reentry_qty(
            next_reentry_price,
            state_params.balance,
            psize_if_filled,
            exchange_params,
            bot_params,
        ),
        initial_entry_qty,
    )
    (_, next_reentry_qty_cropped) = calc_cropped_reentry_qty(
        exchange_params,
        bot_params,
        Position(psize_if_filled, pprice_if_filled),
        wallet_exposure_if_filled,
        state_params.balance,
        next_reentry_qty,
        next_reentry_price,
    )
    effective_double_down_factor = next_reentry_qty_cropped / psize_if_filled if psize_if_filled != 0 else 0.0
    if effective_double_down_factor < bot_params.entry_grid_double_down_factor * 0.25:
        # next reentry too small. Inflate current reentry.
        #st.warning("Inflating current reentry")
        #st.warning(f"reentry_price: {reentry_price}")
        new_entry_qty = interpolate(
            bot_params.wallet_exposure_limit,
            [wallet_exposure, wallet_exposure_if_filled],
            [position.size, position.size + reentry_qty],
        ) - position.size
        return Order(
            qty=round_(new_entry_qty, exchange_params.qty_step),
            price=reentry_price,
            order_type=OrderType.EntryGridInflatedLong,
        )
    else:
        return Order(
            qty=reentry_qty,
            price=reentry_price,
            order_type=OrderType.EntryGridNormalLong,
        )

def calc_trailing_entry_long(exchange_params, state_params, bot_params, position, trailing_price_bundle):
    initial_entry_price = calc_ema_price_bid(
        exchange_params.price_step,
        state_params.order_book.bid,
        state_params.ema_bands.lower,
        bot_params.entry_initial_ema_dist,
    )
    if initial_entry_price <= exchange_params.price_step:
        return Order.default()
    initial_entry_qty = calc_initial_entry_qty(
        exchange_params,
        bot_params,
        state_params.balance,
        initial_entry_price,
    )
    if position.size == 0.0:
        return Order(
            qty=initial_entry_qty,
            price=initial_entry_price,
            order_type=OrderType.EntryInitialNormalLong,
        )
    elif position.size < initial_entry_qty * 0.8:
        return Order(
            qty=max(
                calc_min_entry_qty(initial_entry_price, exchange_params),
                round_dn(initial_entry_qty - position.size, exchange_params.qty_step),
            ),
            price=initial_entry_price,
            order_type=OrderType.EntryInitialPartialLong,
        )
    wallet_exposure = calc_wallet_exposure(
        exchange_params.c_mult,
        state_params.balance,
        position.size,
        position.price,
    )
    if wallet_exposure > bot_params.wallet_exposure_limit * 0.999:
        return Order.default()

    entry_triggered = False
    reentry_price = 0.0
    if bot_params.entry_trailing_threshold_pct <= 0.0:
        # immediate trailing entry
        if (bot_params.entry_trailing_retracement_pct > 0.0 and
            trailing_price_bundle.max_since_min > trailing_price_bundle.min_since_open * (1.0 + bot_params.entry_trailing_retracement_pct)):
            entry_triggered = True
            reentry_price = state_params.order_book.bid
    else:
        if bot_params.entry_trailing_retracement_pct <= 0.0:
            entry_triggered = True
            reentry_price = min(
                state_params.order_book.bid,
                round_dn(
                    position.price * (1.0 - bot_params.entry_trailing_threshold_pct),
                    exchange_params.price_step,
                ),
            )
        else:
            if (trailing_price_bundle.min_since_open < position.price * (1.0 - bot_params.entry_trailing_threshold_pct) and
                trailing_price_bundle.max_since_min > trailing_price_bundle.min_since_open * (1.0 + bot_params.entry_trailing_retracement_pct)):
                entry_triggered = True
                reentry_price = min(
                    state_params.order_book.bid,
                    round_dn(
                        position.price * (1.0 - bot_params.entry_trailing_threshold_pct + bot_params.entry_trailing_retracement_pct),
                        exchange_params.price_step,
                    ),
                )

    if not entry_triggered:
        return Order(
            qty=0.0,
            price=0.0,
            order_type=OrderType.EntryTrailingNormalLong,
        )

    reentry_qty = max(
        calc_reentry_qty(
            reentry_price,
            state_params.balance,
            position.size,
            exchange_params,
            bot_params,
        ),
        initial_entry_qty,
    )
    (_, reentry_qty_cropped) = calc_cropped_reentry_qty(
        exchange_params,
        bot_params,
        position,
        wallet_exposure,
        state_params.balance,
        reentry_qty,
        reentry_price,
    )
    if reentry_qty_cropped < reentry_qty:
        return Order(
            qty=reentry_qty_cropped,
            price=reentry_price,
            order_type=OrderType.EntryTrailingCroppedLong,
        )
    else:
        return Order(
            qty=reentry_qty,
            price=reentry_price,
            order_type=OrderType.EntryTrailingNormalLong,
        )

def calc_next_entry_long(exchange_params, state_params, bot_params, position, trailing_price_bundle):
    if bot_params.wallet_exposure_limit == 0.0 or state_params.balance <= 0.0:
        return Order.default()
    if bot_params.entry_trailing_grid_ratio >= 1.0 or bot_params.entry_trailing_grid_ratio <= -1.0:
        # trailing only
        return calc_trailing_entry_long(
            exchange_params,
            state_params,
            bot_params,
            position,
            trailing_price_bundle,
        )
    elif bot_params.entry_trailing_grid_ratio == 0.0:
        # grid only
        return calc_grid_entry_long(exchange_params, state_params, bot_params, position)

    wallet_exposure = calc_wallet_exposure(
        exchange_params.c_mult,
        state_params.balance,
        position.size,
        position.price,
    )
    wallet_exposure_ratio = wallet_exposure / bot_params.wallet_exposure_limit
    if bot_params.entry_trailing_grid_ratio > 0.0:
        # trailing first
        if wallet_exposure_ratio < bot_params.entry_trailing_grid_ratio:
            if wallet_exposure == 0.0:
                return calc_trailing_entry_long(
                    exchange_params,
                    state_params,
                    bot_params,
                    position,
                    trailing_price_bundle,
                )
            else:
                bot_params_modified = bot_params.clone()
                bot_params_modified.wallet_exposure_limit = (
                    bot_params.wallet_exposure_limit
                    * bot_params.entry_trailing_grid_ratio
                    * 1.01
                )
                return calc_trailing_entry_long(
                    exchange_params,
                    state_params,
                    bot_params_modified,
                    position,
                    trailing_price_bundle,
                )
        else:
            return calc_grid_entry_long(exchange_params, state_params, bot_params, position)
    else:
        # grid first
        if wallet_exposure_ratio < 1.0 + bot_params.entry_trailing_grid_ratio:
            if wallet_exposure == 0.0:
                return calc_grid_entry_long(exchange_params, state_params, bot_params, position)
            else:
                bot_params_modified = bot_params.clone()
                if wallet_exposure != 0.0:
                    bot_params_modified.wallet_exposure_limit = (
                        bot_params.wallet_exposure_limit
                        * (1.0 + bot_params.entry_trailing_grid_ratio)
                        * 1.01
                    )
                return calc_grid_entry_long(
                    exchange_params,
                    state_params,
                    bot_params_modified,
                    position,
                )
        else:
            return calc_trailing_entry_long(
                exchange_params,
                state_params,
                bot_params,
                position,
                trailing_price_bundle,
            )

def calc_entries_long(exchange_params, state_params, bot_params, position, trailing_price_bundle):
    entries = []
    psize = position.size
    pprice = position.price
    bid = state_params.order_book.bid
    for _ in range(500):
        position_mod = Position(psize, pprice)
        state_params_mod = state_params.clone()
        state_params_mod.order_book.bid = bid
        entry = calc_next_entry_long(
            exchange_params,
            state_params_mod,
            bot_params,
            position_mod,
            trailing_price_bundle,
        )
        if entry.qty == 0.0:
            break
        if entries:
            if entry.order_type in [OrderType.EntryTrailingNormalLong, OrderType.EntryTrailingCroppedLong]:
                break
            if entries[-1].price == entry.price:
                break
        (psize, pprice) = calc_new_psize_pprice(
            psize,
            pprice,
            entry.qty,
            entry.price,
            exchange_params.qty_step,
        )
        bid = min(bid, entry.price)
        entries.append(entry)
        
    
    return entries

# ----------------------------
# Entry Calculation Functions (Short)
# ----------------------------

def calc_grid_entry_short(exchange_params, state_params, bot_params, position):
    if bot_params.wallet_exposure_limit == 0.0 or state_params.balance <= 0.0:
        return Order.default()
    initial_entry_price = calc_ema_price_ask(
        exchange_params.price_step,
        state_params.order_book.ask,
        state_params.ema_bands.upper,
        bot_params.entry_initial_ema_dist,
    )
    if initial_entry_price <= exchange_params.price_step:
        return Order.default()
    initial_entry_qty = calc_initial_entry_qty(
        exchange_params,
        bot_params,
        state_params.balance,
        initial_entry_price,
    )
    position_size_abs = abs(position.size)
    if position_size_abs == 0.0:
        return Order(
            qty=-initial_entry_qty,
            price=initial_entry_price,
            order_type=OrderType.EntryInitialNormalShort,
        )
    elif position_size_abs < initial_entry_qty * 0.8:
        return Order(
            qty=-max(
                calc_min_entry_qty(initial_entry_price, exchange_params),
                round_dn(initial_entry_qty - position_size_abs, exchange_params.qty_step),
            ),
            price=initial_entry_price,
            order_type=OrderType.EntryInitialPartialShort,
        )
    wallet_exposure = calc_wallet_exposure(
        exchange_params.c_mult,
        state_params.balance,
        position_size_abs,
        position.price,
    )
    if wallet_exposure >= bot_params.wallet_exposure_limit * 0.999:
        return Order.default()

    reentry_price = calc_reentry_price_ask(
        position.price,
        wallet_exposure,
        state_params.order_book.ask,
        exchange_params,
        bot_params,
    )
    if reentry_price <= 0.0:
        return Order.default()
    reentry_qty = max(
        calc_reentry_qty(
            reentry_price,
            state_params.balance,
            position_size_abs,
            exchange_params,
            bot_params,
        ),
        initial_entry_qty,
    )
    (wallet_exposure_if_filled, reentry_qty_cropped) = calc_cropped_reentry_qty(
        exchange_params,
        bot_params,
        position,
        wallet_exposure,
        state_params.balance,
        reentry_qty,
        reentry_price,
    )
    if reentry_qty_cropped < reentry_qty:
        return Order(
            qty=-reentry_qty_cropped,
            price=reentry_price,
            order_type=OrderType.EntryGridCroppedShort,
        )
    (psize_if_filled, pprice_if_filled) = calc_new_psize_pprice(
        position_size_abs,
        position.price,
        reentry_qty,
        reentry_price,
        exchange_params.qty_step,
    )
    next_reentry_price = calc_reentry_price_ask(
        pprice_if_filled,
        wallet_exposure_if_filled,
        state_params.order_book.ask,
        exchange_params,
        bot_params,
    )
    next_reentry_qty = max(
        calc_reentry_qty(
            next_reentry_price,
            state_params.balance,
            psize_if_filled,
            exchange_params,
            bot_params,
        ),
        initial_entry_qty,
    )
    (_, next_reentry_qty_cropped) = calc_cropped_reentry_qty(
        exchange_params,
        bot_params,
        Position(psize_if_filled, pprice_if_filled),
        wallet_exposure_if_filled,
        state_params.balance,
        next_reentry_qty,
        next_reentry_price,
    )
    effective_double_down_factor = next_reentry_qty_cropped / psize_if_filled if psize_if_filled != 0 else 0.0
    if effective_double_down_factor < bot_params.entry_grid_double_down_factor * 0.25:
        new_entry_qty = interpolate(
            bot_params.wallet_exposure_limit,
            [wallet_exposure, wallet_exposure_if_filled],
            [position_size_abs, position_size_abs + reentry_qty],
        ) - position_size_abs
        return Order(
            qty=-round_(new_entry_qty, exchange_params.qty_step),
            price=reentry_price,
            order_type=OrderType.EntryGridInflatedShort,
        )
    else:
        return Order(
            qty=-reentry_qty,
            price=reentry_price,
            order_type=OrderType.EntryGridNormalShort,
        )

def calc_trailing_entry_short(exchange_params, state_params, bot_params, position, trailing_price_bundle):
    initial_entry_price = calc_ema_price_ask(
        exchange_params.price_step,
        state_params.order_book.ask,
        state_params.ema_bands.upper,
        bot_params.entry_initial_ema_dist,
    )
    if initial_entry_price <= exchange_params.price_step:
        return Order.default()
    initial_entry_qty = calc_initial_entry_qty(
        exchange_params,
        bot_params,
        state_params.balance,
        initial_entry_price,
    )
    position_size_abs = abs(position.size)
    if position_size_abs == 0.0:
        return Order(
            qty=-initial_entry_qty,
            price=initial_entry_price,
            order_type=OrderType.EntryInitialNormalShort,
        )
    elif position_size_abs < initial_entry_qty * 0.8:
        return Order(
            qty=-max(
                calc_min_entry_qty(initial_entry_price, exchange_params),
                round_dn(initial_entry_qty - position_size_abs, exchange_params.qty_step),
            ),
            price=initial_entry_price,
            order_type=OrderType.EntryInitialPartialShort,
        )
    wallet_exposure = calc_wallet_exposure(
        exchange_params.c_mult,
        state_params.balance,
        position_size_abs,
        position.price,
    )
    if wallet_exposure > bot_params.wallet_exposure_limit * 0.999:
        return Order.default()
    entry_triggered = False
    reentry_price = 0.0
    if bot_params.entry_trailing_threshold_pct <= 0.0:
        # immediate trailing entry for short
        if bot_params.entry_trailing_retracement_pct > 0.0 \
           and trailing_price_bundle.min_since_max < trailing_price_bundle.max_since_open * (1.0 - bot_params.entry_trailing_retracement_pct):
            entry_triggered = True
            reentry_price = state_params.order_book.ask
    else:
        if bot_params.entry_trailing_retracement_pct <= 0.0:
            entry_triggered = True
            reentry_price = max(
                state_params.order_book.ask,
                round_up(
                    position.price * (1.0 + bot_params.entry_trailing_threshold_pct),
                    exchange_params.price_step,
                ),
            )
        else:
            if (trailing_price_bundle.max_since_open > position.price * (1.0 + bot_params.entry_trailing_threshold_pct)
                and trailing_price_bundle.min_since_max < trailing_price_bundle.max_since_open * (1.0 - bot_params.entry_trailing_retracement_pct)):
                entry_triggered = True
                reentry_price = max(
                    state_params.order_book.ask,
                    round_up(
                        position.price * (1.0 + bot_params.entry_trailing_threshold_pct - bot_params.entry_trailing_retracement_pct),
                        exchange_params.price_step,
                    ),
                )
    if not entry_triggered:
        return Order(
            qty=0.0,
            price=0.0,
            order_type=OrderType.EntryTrailingNormalShort,
        )

    reentry_qty = max(
        calc_reentry_qty(
            reentry_price,
            state_params.balance,
            position_size_abs,
            exchange_params,
            bot_params,
        ),
        initial_entry_qty,
    )
    (_, reentry_qty_cropped) = calc_cropped_reentry_qty(
        exchange_params,
        bot_params,
        position,
        wallet_exposure,
        state_params.balance,
        reentry_qty,
        reentry_price,
    )
    if reentry_qty_cropped < reentry_qty:
        return Order(
            qty=-reentry_qty_cropped,
            price=reentry_price,
            order_type=OrderType.EntryTrailingCroppedShort,
        )
    else:
        return Order(
            qty=-reentry_qty,
            price=reentry_price,
            order_type=OrderType.EntryTrailingNormalShort,
        )

def calc_next_entry_short(
    exchange_params,
    state_params,
    bot_params,
    position,
    trailing_price_bundle
):
    position_size_abs = abs(position.size)
    if bot_params.wallet_exposure_limit == 0.0 or state_params.balance <= 0.0:
        return Order.default()
    if bot_params.entry_trailing_grid_ratio >= 1.0 or bot_params.entry_trailing_grid_ratio <= -1.0:
        return calc_trailing_entry_short(
            exchange_params,
            state_params,
            bot_params,
            position,
            trailing_price_bundle,
        )
    elif bot_params.entry_trailing_grid_ratio == 0.0:
        return calc_grid_entry_short(exchange_params, state_params, bot_params, position)

    wallet_exposure = calc_wallet_exposure(
        exchange_params.c_mult,
        state_params.balance,
        position_size_abs,
        position.price,
    )
    wallet_exposure_ratio = wallet_exposure / bot_params.wallet_exposure_limit
    if bot_params.entry_trailing_grid_ratio > 0.0:
        # trailing first
        if wallet_exposure_ratio < bot_params.entry_trailing_grid_ratio:
            if wallet_exposure == 0.0:
                return calc_trailing_entry_short(
                    exchange_params,
                    state_params,
                    bot_params,
                    position,
                    trailing_price_bundle,
                )
            else:
                bot_params_modified = bot_params.clone()
                bot_params_modified.wallet_exposure_limit = (
                    bot_params.wallet_exposure_limit
                    * bot_params.entry_trailing_grid_ratio
                    * 1.01
                )
                return calc_trailing_entry_short(
                    exchange_params,
                    state_params,
                    bot_params_modified,
                    position,
                    trailing_price_bundle,
                )
        else:
            return calc_grid_entry_short(exchange_params, state_params, bot_params, position)
    else:
        # grid first
        if wallet_exposure_ratio < 1.0 + bot_params.entry_trailing_grid_ratio:
            if wallet_exposure == 0.0:
                return calc_grid_entry_short(exchange_params, state_params, bot_params, position)
            else:
                bot_params_modified = bot_params.clone()
                if wallet_exposure != 0.0:
                    bot_params_modified.wallet_exposure_limit = (
                        bot_params.wallet_exposure_limit
                        * (1.0 + bot_params.entry_trailing_grid_ratio)
                        * 1.01
                    )
                return calc_grid_entry_short(
                    exchange_params,
                    state_params,
                    bot_params_modified,
                    position,
                )
        else:
            return calc_trailing_entry_short(
                exchange_params,
                state_params,
                bot_params,
                position,
                trailing_price_bundle,
            )

def calc_entries_short(
    exchange_params,
    state_params,
    bot_params,
    position,
    trailing_price_bundle
):
    entries = []
    psize = position.size
    pprice = position.price
    ask = state_params.order_book.ask
    for _ in range(500):
        position_mod = Position(psize, pprice)
        state_params_mod = state_params.clone()
        state_params_mod.order_book.ask = ask
        entry = calc_next_entry_short(
            exchange_params,
            state_params_mod,
            bot_params,
            position_mod,
            trailing_price_bundle,
        )
        if entry.qty == 0.0:
            break
        if entries:
            if entry.order_type in [OrderType.EntryTrailingNormalShort, OrderType.EntryTrailingCroppedShort]:
                break
            if entries[-1].price == entry.price:
                break
        (psize, pprice) = calc_new_psize_pprice(
            psize,
            pprice,
            entry.qty,
            entry.price,
            exchange_params.qty_step,
        )
        ask = max(ask, entry.price)
        entries.append(entry)
    return entries

# ----------------------------
# Close Calculation Functions (Long)
# ----------------------------

def calc_close_qty(
    exchange_params,
    bot_params,
    position,
    close_qty_pct,
    balance,
    close_price,
):
    full_psize = cost_to_qty(
        balance * bot_params.wallet_exposure_limit,
        position.price,
        exchange_params.c_mult,
    )
    position_size_abs = abs(position.size)
    leftover = max(0.0, position_size_abs - full_psize)
    min_entry_qty_val = calc_min_entry_qty(close_price, exchange_params)
    close_qty = min(
        round_(position_size_abs, exchange_params.qty_step),
        max(
            min_entry_qty_val,
            round_up(
                full_psize * close_qty_pct + leftover,
                exchange_params.qty_step,
            ),
        ),
    )
    if close_qty > 0.0 and close_qty < position_size_abs and position_size_abs - close_qty < min_entry_qty_val:
        return position_size_abs
    else:
        return close_qty

def calc_grid_close_long(
    exchange_params,
    state_params,
    bot_params,
    position
):
    if position.size <= 0.0:
        return Order.default()
    if bot_params.close_grid_markup_range <= 0.0 or bot_params.close_grid_qty_pct < 0.0 or bot_params.close_grid_qty_pct >= 1.0:
        return Order(
            qty=-round_(position.size, exchange_params.qty_step),
            price=max(
                state_params.order_book.ask,
                round_up(
                    position.price * (1.0 + bot_params.close_grid_min_markup),
                    exchange_params.price_step,
                ),
            ),
            order_type=OrderType.CloseGridLong,
        )
    close_prices_start = round_up(
        position.price * (1.0 + bot_params.close_grid_min_markup),
        exchange_params.price_step,
    )
    close_prices_end = round_up(
        position.price * (1.0 + bot_params.close_grid_min_markup + bot_params.close_grid_markup_range),
        exchange_params.price_step,
    )
    if close_prices_start == close_prices_end:
        return Order(
            qty=-round_(position.size, exchange_params.qty_step),
            price=max(state_params.order_book.ask, close_prices_start),
            order_type=OrderType.CloseGridLong,
        )
    n_steps = ceil((close_prices_end - close_prices_start) / exchange_params.price_step)
    close_grid_qty_pct_modified = max(bot_params.close_grid_qty_pct, 1.0 / n_steps)
    wallet_exposure = calc_wallet_exposure(
        exchange_params.c_mult,
        state_params.balance,
        position.size,
        position.price,
    )
    wallet_exposure_ratio = min(1.0, wallet_exposure / bot_params.wallet_exposure_limit)

    close_price = max(
        round_up(
            position.price
            * (1.0 + bot_params.close_grid_min_markup + bot_params.close_grid_markup_range * (1.0 - wallet_exposure_ratio)),
            exchange_params.price_step,
        ),
        state_params.order_book.ask,
    )
    close_qty = -calc_close_qty(
        exchange_params,
        bot_params,
        position,
        close_grid_qty_pct_modified,
        state_params.balance,
        close_price,
    )
    #st.error(f"close_qty: {close_qty}, close_price: {close_price}")
    return Order(
        qty=close_qty,
        price=close_price,
        order_type=OrderType.CloseGridLong,
    )

def calc_trailing_close_long(
    exchange_params,
    state_params,
    bot_params,
    position,
    trailing_price_bundle
):
    if position.size == 0.0:
        return Order.default()
    if bot_params.close_trailing_threshold_pct <= 0.0:
        if bot_params.close_trailing_retracement_pct > 0.0 \
           and trailing_price_bundle.min_since_max < trailing_price_bundle.max_since_open * (1.0 - bot_params.close_trailing_retracement_pct):
            return Order(
                qty=-calc_close_qty(
                    exchange_params,
                    bot_params,
                    position,
                    bot_params.close_trailing_qty_pct,
                    state_params.balance,
                    state_params.order_book.ask,
                ),
                price=state_params.order_book.ask,
                order_type=OrderType.CloseTrailingLong,
            )
        else:
            return Order(
                qty=0.0,
                price=0.0,
                order_type=OrderType.CloseTrailingLong,
            )
    else:
        if bot_params.close_trailing_retracement_pct <= 0.0:
            close_price = max(
                state_params.order_book.ask,
                round_up(
                    position.price * (1.0 + bot_params.close_trailing_threshold_pct),
                    exchange_params.price_step,
                ),
            )
            return Order(
                qty=-calc_close_qty(
                    exchange_params,
                    bot_params,
                    position,
                    bot_params.close_trailing_qty_pct,
                    state_params.balance,
                    close_price,
                ),
                price=close_price,
                order_type=OrderType.CloseTrailingLong,
            )
        else:
            if (trailing_price_bundle.max_since_open > position.price * (1.0 + bot_params.close_trailing_threshold_pct)
                and trailing_price_bundle.min_since_max < trailing_price_bundle.max_since_open * (1.0 - bot_params.close_trailing_retracement_pct)):
                close_price = max(
                    state_params.order_book.ask,
                    round_up(
                        position.price * (1.0 + bot_params.close_trailing_threshold_pct - bot_params.close_trailing_retracement_pct),
                        exchange_params.price_step,
                    ),
                )
                return Order(
                    qty=-calc_close_qty(
                        exchange_params,
                        bot_params,
                        position,
                        bot_params.close_trailing_qty_pct,
                        state_params.balance,
                        close_price,
                    ),
                    price=close_price,
                    order_type=OrderType.CloseTrailingLong,
                )
            else:
                return Order(
                    qty=0.0,
                    price=0.0,
                    order_type=OrderType.CloseTrailingLong,
                )

def calc_next_close_long(
    exchange_params,
    state_params,
    bot_params,
    position,
    trailing_price_bundle
):
    if position.size == 0.0:
        return Order.default()
    if bot_params.close_trailing_grid_ratio >= 1.0 or bot_params.close_trailing_grid_ratio <= -1.0:
        return calc_trailing_close_long(
            exchange_params,
            state_params,
            bot_params,
            position,
            trailing_price_bundle
        )
    if bot_params.close_trailing_grid_ratio == 0.0:
        return calc_grid_close_long(exchange_params, state_params, bot_params, position)

    wallet_exposure_ratio = calc_wallet_exposure(
        exchange_params.c_mult,
        state_params.balance,
        position.size,
        position.price,
    ) / bot_params.wallet_exposure_limit
    if bot_params.close_trailing_grid_ratio > 0.0:
        if wallet_exposure_ratio < bot_params.close_trailing_grid_ratio:
            st.error("Trailing first")
            return calc_trailing_close_long(
                exchange_params,
                state_params,
                bot_params,
                position,
                trailing_price_bundle,
            )
        else:
            st.error("Grid first")
            trailing_allocation = cost_to_qty(
                state_params.balance * bot_params.wallet_exposure_limit * bot_params.close_trailing_grid_ratio,
                position.price,
                exchange_params.c_mult,
            )
            min_entry_qty_val = calc_min_entry_qty(position.price, exchange_params)
            if trailing_allocation < min_entry_qty_val:
                trailing_allocation = 0.0
            grid_allocation = round_(position.size - trailing_allocation, exchange_params.qty_step)
            position_mod = Position(
                size=min(position.size, max(grid_allocation, min_entry_qty_val)),
                price=position.price
            )
            return calc_grid_close_long(exchange_params, state_params, bot_params, position_mod)
    else:
        if wallet_exposure_ratio < 1.0 + bot_params.close_trailing_grid_ratio:
            return calc_grid_close_long(exchange_params, state_params, bot_params, position)
        else:
            grid_allocation = cost_to_qty(
                state_params.balance * bot_params.wallet_exposure_limit * (1.0 + bot_params.close_trailing_grid_ratio),
                position.price,
                exchange_params.c_mult,
            )
            min_entry_qty_val = calc_min_entry_qty(position.price, exchange_params)
            if grid_allocation < min_entry_qty_val:
                grid_allocation = 0.0
            trailing_allocation = round_(position.size - grid_allocation, exchange_params.qty_step)
            position_mod = Position(
                size=min(position.size, max(trailing_allocation, min_entry_qty_val)),
                price=position.price
            )
            return calc_trailing_close_long(
                exchange_params,
                state_params,
                bot_params,
                position_mod,
                trailing_price_bundle,
            )

def calc_closes_long(
    exchange_params,
    state_params,
    bot_params,
    position,
    trailing_price_bundle
):
    closes = []
    psize = position.size
    ask = state_params.order_book.ask
    for _ in range(500):
        position_mod = Position(psize, position.price)
        state_params_mod = state_params.clone()
        state_params_mod.order_book.ask = ask
        close = calc_next_close_long(
            exchange_params,
            state_params_mod,
            bot_params,
            position_mod,
            trailing_price_bundle
        )
        if close.qty == 0.0:
            break
        psize = round_(psize + close.qty, exchange_params.qty_step)
        ask = max(ask, close.price)
        if closes:
            if close.order_type == OrderType.CloseTrailingLong:
                closes.append(close)
                break
            if closes[-1].price == close.price:
                previous_close = closes.pop()
                merged_close = Order(
                    qty=round_(previous_close.qty + close.qty, exchange_params.qty_step),
                    price=close.price,
                    order_type=close.order_type,
                )
                closes.append(merged_close)
                continue
        closes.append(close)
    return closes

# ----------------------------
# Close Calculation Functions (Short)
# ----------------------------

def calc_grid_close_short(
    exchange_params,
    state_params,
    bot_params,
    position,
):
    position_size_abs = abs(position.size)
    if position_size_abs == 0.0:
        return Order.default()
    if bot_params.close_grid_markup_range <= 0.0 \
       or bot_params.close_grid_qty_pct < 0.0  \
       or bot_params.close_grid_qty_pct >= 1.0:
        return Order(
            qty=round_(position_size_abs, exchange_params.qty_step),
            price=min(
                state_params.order_book.bid,
                round_dn(
                    position.price * (1.0 - bot_params.close_grid_min_markup),
                    exchange_params.price_step,
                ),
            ),
            order_type=OrderType.CloseGridShort,
        )
    close_prices_start = round_dn(
        position.price * (1.0 - bot_params.close_grid_min_markup),
        exchange_params.price_step,
    )
    close_prices_end = round_dn(
        position.price * (1.0 - bot_params.close_grid_min_markup - bot_params.close_grid_markup_range),
        exchange_params.price_step,
    )
    if close_prices_start == close_prices_end:
        return Order(
            qty=round_(position_size_abs, exchange_params.qty_step),
            price=min(state_params.order_book.bid, close_prices_start),
            order_type=OrderType.CloseGridShort,
        )
    n_steps = ceil((close_prices_start - close_prices_end) / exchange_params.price_step)
    close_grid_qty_pct_modified = max(bot_params.close_grid_qty_pct, 1.0 / n_steps)
    wallet_exposure = calc_wallet_exposure(
        exchange_params.c_mult,
        state_params.balance,
        position_size_abs,
        position.price,
    )
    wallet_exposure_ratio = min(1.0, wallet_exposure / bot_params.wallet_exposure_limit)
    close_price = min(
        round_dn(
            position.price
            * (1.0 - bot_params.close_grid_min_markup - bot_params.close_grid_markup_range * (1.0 - wallet_exposure_ratio)),
            exchange_params.price_step,
        ),
        state_params.order_book.bid,
    )
    close_qty = calc_close_qty(
        exchange_params,
        bot_params,
        position,
        close_grid_qty_pct_modified,
        state_params.balance,
        close_price,
    )
    return Order(
        qty=close_qty,
        price=close_price,
        order_type=OrderType.CloseGridShort,
    )

def calc_trailing_close_short(
    exchange_params,
    state_params,
    bot_params,
    position,
    trailing_price_bundle,
):
    position_size_abs = abs(position.size)
    if position_size_abs == 0.0:
        return Order.default()
    if bot_params.close_trailing_threshold_pct <= 0.0:
        # immediate trailing stop for short
        if bot_params.close_trailing_retracement_pct > 0.0 \
           and trailing_price_bundle.max_since_min > trailing_price_bundle.min_since_open * (1.0 + bot_params.close_trailing_retracement_pct):
            return Order(
                qty=calc_close_qty(
                    exchange_params,
                    bot_params,
                    position,
                    bot_params.close_trailing_qty_pct,
                    state_params.balance,
                    state_params.order_book.bid,
                ),
                price=state_params.order_book.bid,
                order_type=OrderType.CloseTrailingShort,
            )
        else:
            return Order(
                qty=0.0,
                price=0.0,
                order_type=OrderType.CloseTrailingShort,
            )
    else:
        if bot_params.close_trailing_retracement_pct <= 0.0:
            close_price = min(
                state_params.order_book.bid,
                round_dn(
                    position.price * (1.0 - bot_params.close_trailing_threshold_pct),
                    exchange_params.price_step,
                ),
            )
            return Order(
                qty=calc_close_qty(
                    exchange_params,
                    bot_params,
                    position,
                    bot_params.close_trailing_qty_pct,
                    state_params.balance,
                    close_price,
                ),
                price=close_price,
                order_type=OrderType.CloseTrailingShort,
            )
        else:
            if (trailing_price_bundle.min_since_open < position.price * (1.0 - bot_params.close_trailing_threshold_pct)
                and trailing_price_bundle.max_since_min > trailing_price_bundle.min_since_open * (1.0 + bot_params.close_trailing_retracement_pct)):
                close_price = min(
                    state_params.order_book.bid,
                    round_dn(
                        position.price * (1.0 - bot_params.close_trailing_threshold_pct + bot_params.close_trailing_retracement_pct),
                        exchange_params.price_step,
                    ),
                )
                return Order(
                    qty=calc_close_qty(
                        exchange_params,
                        bot_params,
                        position,
                        bot_params.close_trailing_qty_pct,
                        state_params.balance,
                        close_price,
                    ),
                    price=close_price,
                    order_type=OrderType.CloseTrailingShort,
                )
            else:
                return Order(
                    qty=0.0,
                    price=0.0,
                    order_type=OrderType.CloseTrailingShort,
                )

def calc_next_close_short(
    exchange_params,
    state_params,
    bot_params,
    position,
    trailing_price_bundle,
):
    position_size_abs = abs(position.size)
    if position_size_abs == 0.0:
        return Order.default()
    if bot_params.close_trailing_grid_ratio >= 1.0 or bot_params.close_trailing_grid_ratio <= -1.0:
        return calc_trailing_close_short(
            exchange_params,
            state_params,
            bot_params,
            position,
            trailing_price_bundle,
        )
    if bot_params.close_trailing_grid_ratio == 0.0:
        return calc_grid_close_short(exchange_params, state_params, bot_params, position)

    wallet_exposure_ratio = calc_wallet_exposure(
        exchange_params.c_mult,
        state_params.balance,
        position_size_abs,
        position.price,
    ) / bot_params.wallet_exposure_limit
    if bot_params.close_trailing_grid_ratio > 0.0:
        if wallet_exposure_ratio < bot_params.close_trailing_grid_ratio:
            return calc_trailing_close_short(
                exchange_params,
                state_params,
                bot_params,
                position,
                trailing_price_bundle,
            )
        else:
            trailing_allocation = cost_to_qty(
                state_params.balance
                * bot_params.wallet_exposure_limit
                * bot_params.close_trailing_grid_ratio,
                position.price,
                exchange_params.c_mult,
            )
            min_entry_qty_val = calc_min_entry_qty(position.price, exchange_params)
            if trailing_allocation < min_entry_qty_val:
                trailing_allocation = 0.0
            grid_allocation = round_(
                position_size_abs - trailing_allocation,
                exchange_params.qty_step,
            )
            position_mod = Position(
                size=-min(position_size_abs, max(grid_allocation, min_entry_qty_val)),
                price=position.price,
            )
            return calc_grid_close_short(exchange_params, state_params, bot_params, position_mod)
    else:
        if wallet_exposure_ratio < 1.0 + bot_params.close_trailing_grid_ratio:
            return calc_grid_close_short(exchange_params, state_params, bot_params, position)
        else:
            grid_allocation = cost_to_qty(
                state_params.balance
                * bot_params.wallet_exposure_limit
                * (1.0 + bot_params.close_trailing_grid_ratio),
                position.price,
                exchange_params.c_mult,
            )
            min_entry_qty_val = calc_min_entry_qty(position.price, exchange_params)
            if grid_allocation < min_entry_qty_val:
                grid_allocation = 0.0
            trailing_allocation = round_(
                position_size_abs - grid_allocation,
                exchange_params.qty_step,
            )
            position_mod = Position(
                size=-min(
                    position_size_abs,
                    max(trailing_allocation, min_entry_qty_val),
                ),
                price=position.price,
            )
            return calc_trailing_close_short(
                exchange_params,
                state_params,
                bot_params,
                position_mod,
                trailing_price_bundle,
            )

def calc_closes_short(
    exchange_params,
    state_params,
    bot_params,
    position,
    trailing_price_bundle,
):
    closes = []
    psize = position.size
    bid = state_params.order_book.bid
    for _ in range(500):
        position_mod = Position(psize, position.price)
        state_params_mod = state_params.clone()
        state_params_mod.order_book.bid = bid
        close = calc_next_close_short(
            exchange_params,
            state_params_mod,
            bot_params,
            position_mod,
            trailing_price_bundle,
        )
        if close.qty == 0.0:
            break
        psize = round_(psize + close.qty, exchange_params.qty_step)
        bid = min(bid, close.price)
        if closes:
            if close.order_type == OrderType.CloseTrailingShort:
                closes.append(close)
                break
            if closes[-1].price == close.price:
                previous_close = closes.pop()
                merged_close = Order(
                    qty=round_(previous_close.qty + close.qty, exchange_params.qty_step),
                    price=close.price,
                    order_type=close.order_type,
                )
                closes.append(merged_close)
                continue
        closes.append(close)
    return closes

