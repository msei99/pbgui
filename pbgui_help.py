assigned_balance = """
    ```
    assigned_balance overriding balance fetched from exchange
    On spot market it is recommended to fix wallet balance to a given value.
    This because in spot trading there are no positions, no wallet balance
    and no unrealized PnL. There is only plain buy and sell.
    So wallet balance and position must be simulated by looking in past user
    trade history.
    ```"""
price_distance_threshold = """
    ```
    only create limit orders closer to price than threshold. default=0.5 (50%)
    The grid is exactly the same it just does not place the buy/sell orders
    which are far away from the current price. When the price moves in that
    direction and gets into the threshold's % range it will place those orders too.
    ```"""
mode = """
    ```
    n (normal); normal operation
    gs (graceful stop): let the bot continue as normal until
        all positions are fully closed, then not open any more positions.
    p (panic): bot will close positions asap using limit orders
    t (TP-only): bot only manages TP grid and will not cancel
       or create any entries.
    ```"""
exposure = """
    ```
    specify wallet_exposure_limit, overriding value from live config
    0 = reset to value from live config
    ```"""
min_markup = """
    ```
    specify min_markup, overriding value from live config
    0 = reset to value from live config
    ```"""
markup_range = """
    ```
    specify markup_range, overriding value from live config
    0 = reset to value from live config
    ```"""
lev = """
    ```
    On futures markets with leverage, passivbot may expose more than 100%
    of the wallet's funds. Passivbot uses only (unleveraged) wallet balance
    in its calculations, so adjusting leverage on exchange will make
    no difference on risk, profit or bot behavior, as long as leverage is set
    high enough for the bot to make its grid according to the configuration.
    ```"""
co = """
    ```
    When in OHLCV mode, offset the execution cycle by a certain number of
    seconds from the start of each minute. This can help avoid exceeding
    the API rate limit when running multiple instances.
    Default is random (-1)
    ```"""
ohlcv = """
    ```
    use 1m ohlcv instead of 1s ticks
    ```"""
price_precision = """
    ```
    Override price step with round_dynamic(market_price * price_precision, 1).
    Default: None (0.0000) Suggested val 0.0001
    ```"""
price_step = """
    ```
    Override price step with custom price step. Takes precedence over -pp
    Default: None (0.000) Not every exchange has the same minimal step
    ```"""
api_error = """
    Check your API-Key and enable spot and/or future trading if you need it
    """
upload_pbguidb = """
    ```
    Share your configuration with pbconfigdb
    You can enter a name that will be displayed in pbconfigdb as source
    ```"""
instance_save = """
    ```
    Save config
    ```"""
instance_restart = """
    ```
    Save config and restart Instance
    ```"""
instance_enable = """
    ```
    Save config and start/stop Instance
    ```"""

opt_iters = """
    ```
    n optimize iters
    ```"""

opt_reruns = """
    ```
    n optimizer reruns
    An optimizer reruns can yield better results with the following settings:
    Iters=25000 Reruns=20. This approach is more effective compared to running
    the optimizer only once with 500000 iterations. By rerunning the optimizer,
    you have a higher chance of finding good configurations that are not overfitted.
    ```"""

pbrun = """
    ```
    This is the Instance Manager from PBGUI.
    Enable, to start all enabled Instances.
    To start the Instance Manager after reboot your server, you have to
    start PBRun.py when your Server starts.
    This can be done in your crontab with @reboot

    Example crontab
    @reboot ~/software/pbgui/start.sh

    Example start.sh
    #!/usr/bin/bash
    venv=~/software/venv_pb39       #Path to python venv
    pbgui=~/software/pbgui          #path to pbgui installation
    source ${venv}/bin/activate
    cd ${pbgui}
    python PBRun.py
    # python PBRemote.py
    # python PBStat.py

    Run "chmod 755 start.sh" and change the path to your needs
    Run "crontab -e" and add the @reboot with your path
    ```"""

pbremote = """
    ```
    This is the Remote Server Manager from PBGUI.
    Enable, to start sync Bots running on a Remote Server.
    To start the Remote Server Manager after reboot your server, you have to
    start PBRemote.py when your Server starts.
    This can be done in your crontab with @reboot

    Example crontab
    @reboot ~/software/pbgui/start.sh

    Example start.sh
    #!/usr/bin/bash
    venv=~/software/venv_pb39       #Path to python venv
    pbgui=~/software/pbgui          #path to pbgui installation
    source ${venv}/bin/activate
    cd ${pbgui}
    # python PBRun.py
    python PBRemote.py
    # python PBStat.py

    Run "chmod 755 start.sh" and change the path to your needs
    Run "crontab -e" and add the @reboot with your path
    ```"""

pbstat = """
    ```
    This is the Data Scrapper from PBGUI.
    If you disable PBStat, you will not be able to see live exchange data.
    Enable, to start scrapping data from Exchanges.
    To start the Data Scrapper after reboot your server, you have to
    start PBStat.py when your Server starts.
    This can be done in your crontab with @reboot

    Example crontab
    @reboot ~/software/pbgui/start.sh

    Example start.sh
    #!/usr/bin/bash
    venv=~/software/venv_pb39       #Path to python venv
    pbgui=~/software/pbgui          #path to pbgui installation
    source ${venv}/bin/activate
    cd ${pbgui}
    # python PBRun.py
    # python PBRemote.py
    python PBStat.py

    Run "chmod 755 start.sh" and change the path to your needs
    Run "crontab -e" and add the @reboot with your path
    ```"""

score_maximum = """
    ```
    score = adg per exposure weighted according to adg subdivisions
    score metric thresholds
    any improvement beyond threshold is ignored
    maximum_x: don't penalize scores with values lower than maximum_x
    set any to -1 (less than zero) to disable
    ```"""

clip_threshold = """
    ```
    clip results: compute score on top performers only
    clip_threshold=0.1 means drop 10% worst performers;
    clip_threshold=0.0 means include all
    clip_threshold>=1 means include exactly x symbols,
    e.g. clip_threshold=4: include exactly 4 symbols
    ```"""

backtest_slices = """
    ```
    to reduce overfitting, perform backtest with multiple start dates,
    taking mean of metrics as final analysis
    ```"""

grid_span = """
    ```
    per uno (0.32 == 32%) distance from initial entry price to last node's price
    ```"""

max_n_entry_orders = """
    ```
    Max number of nodes in entry grid.
    ```"""

eqty_exp_base = """
    ```
    if 1.0, spacing between all nodes' prices is equal
    higher than 1.0 and spacing will increase deeper in the grid
    ```"""

eprice_exp_base = """
    ```
    if 1.0, qtys will increase linearly deeper in the grid
    if > 1.0, qtys will increase exponentially deeper in the grid
    ```"""

ema_span = """
    ```
    ema_span_0: float
    ema_span_1: float
    spans are given in minutes
    next_EMA = prev_EMA * (1 - alpha) + new_val * alpha
    where alpha = 2 / (span + 1)
    one more EMA span is added in between span_0 and span_1:
    EMA_spans = [ema_span_0, (ema_span_0 * ema_span_1)**0.5, ema_span_1]
    these three EMAs are used to make an upper and a lower EMA band:
    ema_band_lower = min(emas)
    ema_band_upper = max(emas)
    ```"""

initial_qty_pct = """
    ```
    initial_qty_pct: float
    initial_entry_cost = balance * wallet_exposure_limit * initial_qty_pct
    ```"""

initial_eprice_ema_dist = """
    ```
    initial_eprice_ema_dist: float
    if no pos, initial entry price is:
    ema_band_lower * (1 - initial_eprice_ema_dist) for long
    ema_band_upper * (1 + initial_eprice_ema_dist) for short
    ```"""

wallet_exposure_limit = """
    ```
    wallet_exposure_limit: float
    bot limits pos size to wallet_balance_in_contracts * wallet_exposure_limit
    ```"""

ddown_factor = """
    ```
    next_reentry_qty = pos_size * ddown_factor
    in recursive grid mode ddown factor is static;
    in neat grid mode ddown factor becomes dynamic
    ```"""

rentry_pprice_dist = """
    ```
    rentry_pprice_dist: float
    ```"""

rentry_pprice_dist_wallet_exposure_weighting = """
    ```
    if set to zero, spacing between nodes will be approximately the same
    if > zero, spacing between nodes will increase in some proportion to wallet_exposure
    given long,
    next_reentry_price = pos_price * (1 - rentry_pprice_diff * modifier)
    where modifier = (1 + ratio * rentry_pprice_dist_wallet_exposure_weighting)
    and where ratio = wallet_exposure / wallet_exposure_limit
    ```"""

min_markup = """
    ```
    min_markup: float
    ```"""

markup_range = """
    ```
    markup_range: float
    ```"""

n_close_orders = """
    ```
    n_close_orders: int (if float: int(round(x)))
    Take Profit (TP) prices are spread out from
    pos_price * (1 + min_markup) to pos_price * (1 + min_markup + markup_range) for long
    pos_price * (1 - min_markup) to pos_price * (1 - min_markup - markup_range) for short
    e.g. if pos_price==100, min_markup=0.02, markup_range=0.03 and
    n_close_orders=7, TP prices are [102, 102.5, 103, 103.5, 104, 104.5, 105]
    qty per order is pos size divided by n_close_orders
    say long, if one TP ask is filled and afterwards price dips below that price level,
    bot recreates TP grid with reduced qty on each price level
    ```"""

auto_unstuck_wallet_exposure_threshold = """
    ```
    Ratio of exposure to exposure_limit at which auto unstuck (AU) kicks in.
    if wallet_exposure / wallet_exposure_limit > (1 - auto_unstuck_wallet_exposure_threshold): enable AU
    E.g.
    auto_unstuck_wallet_exposure_threshold == 0.0: auto unstuck is disabled.
    auto_unstuck_wallet_exposure_threshold == 0.1: auto unstuck kicks in when exposure is 10% away from exposure_limit.
    auto_unstuck_wallet_exposure_threshold == 0.9: auto unstuck kicks in when exposure is 90% away from exposure_limit.
    auto_unstuck_wallet_exposure_threshold == 1.0: auto unstuck is always enabled.
    ```"""

auto_unstuck_qty_pct = """
    ```
    How much of max pos size to close.
    close_cost = balance * wallet_exposure_limit * auto_unstuck_qty_pct
    For example, if balance is $1000, wallet_exposure_limit=0.3 and auto_unstuck_qty_pct=0.02:
    close_cost == $1000 * 0.3 * 0.02 == $6.
    ```"""

auto_unstuck_ema_dist = """
    ```
    ema_span_0, ema_span_1
    Bot uses three emas of spans: [span0, (span0 * span1)**0.5, span1], given in minutes.
    Close price distance from EMA band.
    Lower auto unstuck EMA band is min(ema0, ema1, ema2) * (1 - auto_unstuck_ema_dist).
    Upper auto unstuck EMA band is max(ema0, ema1, ema2) * (1 + auto_unstuck_ema_dist).    How much of max pos size to close.
    ```"""

auto_unstuck_delay_minutes = """
    ```
    Timer for unstuck closes, given in minutes.
    if now - prev_AU_close_ts > auto_unstuck_delay: enable AU
    ```"""

harmony_search = """
    ```
    Parameters for Harmony Search. Don't change them as long as you not fully
    unterstand how hardmony search work.
    Chaning them will not get you better configs. But it can speed up or slow down
    the algorithm.
    ```"""

particle_swarm = """
    ```
    Parameters for Particle Swarm. Don't change them as long as you not fully
    unterstand how particle swarm work.
    Chaning them will not get you better configs. But it can speed up or slow down
    the algorithm.
    ```"""