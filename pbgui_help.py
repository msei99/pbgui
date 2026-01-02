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
instance_min_markup = """
    ```
    specify min_markup, overriding value from live config
    0 = reset to value from live config
    ```"""
instance_markup_range = """
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
    Number of backtests per optimize session.
    ```"""

opt_today = """
    ```
    If selected, the optimizer will always take the current date as the
    end date. This means that when the date changes, the next rerun is
    executed up to the current day.
    ```"""

opt_reruns = """
    ```
    n optimizer reruns
    An optimizer reruns can yield better results with the following settings:
    Iters=25000 Reruns=20. This approach is more effective compared to running
    the optimizer only once with 500000 iterations. By rerunning the optimizer,
    you have a higher chance of finding good configurations that are not overfitted.
    ```"""

backtest_best = """
    ```
    automatic backtest n best results
    ```"""

backtest_sharp = """
    ```
    automatic backtest n sharpest results
    ```"""

backtest_adg = """
    ```
    automatic backtest n highest average daily gains results
    ```"""

backtest_drawdown = """
    ```
    automatic backtest n lowest drawdown results
    ```"""

backtest_stuck = """
    ```
    automatic backtest n lowest hours stuck results
    ```"""

backtest_twe_resolution = """
    ```
    Resolution of the total wallet exposure limit in minutes.
    Best is 1 minute, but it can be very slow.
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
    # python PBMon.py
    # python PBStat.py
    # python PBShare.py
    # python PBData.py
    # python PBCoinData.py

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
    # python PBMOn.py
    # python PBStat.py
    # python PBShare.py
    # python PBData.py
    # python PBCoinData.py

    Run "chmod 755 start.sh" and change the path to your needs
    Run "crontab -e" and add the @reboot with your path
    ```"""

pbmon = """
    ```
    This is the Monitoring Manager from PBGUI.
    Enable, to start monitoring your Bots.
    To start the Monitoring Manager after reboot your server, you have to
    start PBMon.py when your Server starts.
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
    python PBMon.py
    # python PBStat.py
    # python PBShare.py
    # python PBData.py
    # python PBCoinData.py

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
    # python PBMon.py
    python PBStat.py
    # python PBShare.py
    # python PBData.py
    # python PBCoinData.py

    Run "chmod 755 start.sh" and change the path to your needs
    Run "crontab -e" and add the @reboot with your path
    ```"""

pbdata = """
    ```
    This is the Data Manager from PBGUI.
    It stores history, positions and orders in a local sqlite database.
    Enable, to start fetching data from the exchanges.
    To start the Data Manager after reboot your server, you have to
    start PBData.py when your Server starts.
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
    # python PBMon.py
    # python PBStat.py
    # python PBShare.py
    python PBData.py
    # python PBCoinData.py

    Run "chmod 755 start.sh" and change the path to your needs
    Run "crontab -e" and add the @reboot with your path
    ```"""
pbdata_ws_max = """
    ```
    Global cap for private websocket clients across all exchanges
    ```"""

pbcoindata = """
    ```
    This is the CoinData Manager from PBGUI.
    It fetches coin data like marketcap from MarketCap and available coins from Exchanges.
    Enable, to start fetching data.
    To start the CoinData Manager after reboot your server, you have to
    start PBCoinData.py when your Server starts.
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
    # python PBMon.py
    # python PBStat.py
    # python PBShare.py
    # python PBData.py
    python PBCoinData.py

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
    which are used for initial entries and auto unstuck closes
    ```"""

ema_dist = """
    ```
    offset from lower/upper ema band.
    long_entry/short_close price is lower ema band minus offset
    short_entry/long_close price is upper ema band plus offset
    clock_bid_price = min(emas) * (1 - ema_dist_lower)
    clock_ask_price = max(emas) * (1 + ema_dist_upper)
    See ema_span_0/ema_span_1
    ```"""

qty_pct = """
    ```
    basic formula is entry_cost = balance * wallet_exposure_limit * qty_pct
    ```"""

delay_between_fills_minutes = """
    ```
    delay between entries/closes given in minutes
    entry delay resets after full pos close
    ```"""

delay_weight = """
    ```
    delay between clock orders may be reduced, but not increased.
    if pos size is zero, the timer is reset for entries, but not for closes.
    the formula is:
    modified_delay = delay_between_fills * min(1, (1 - pprice_diff * delay_weight))
    where for bids (long entries and short closes):
    pprice_diff = (pos_price / market_price - 1)
    and for asks (short entries and long closes):
    pprice_diff = (market_price / pos_price - 1)
    this means (given delay_weights > 0):
    if market_price > pprice_long (upnl is green):
        entry_delay is unchanged and close_delay reduced
    if market_price < pprice_long (upnl is red):
        entry_delay is reduced and close_delay is unchanged
    if market_price < pprice_short (upnl is green):
        entry_delay is unchanged and close_delay is reduced
    if market_price > pprice_short (upnl is red):
        entry_delay is reduced and close_delay is unchanged
    ```"""

we_multiplier = """
    ```
    similar in function to Recursive Grid mode's ddown_factor
    entry cost is modified according to:
    entry_cost = balance * wallet_exposure_limit * qty_pct * (1 + ratio * we_multiplier)
    where ratio = wallet_exposure / wallet_exposure_limit
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
    Upper auto unstuck EMA band is max(ema0, ema1, ema2) * (1 + auto_unstuck_ema_dist).
    How much of max pos size to close.
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

leverage = """
    ```
    leverage set on exchange
    ```"""

logging_level = """
    ```
    Controls global verbosity for Passivbot and tooling.
    Accepted values: 0 (warnings), 1 (info), 2 (debug), 3 (trace).
    ```"""

backtest_balance_sample_divider = """
    ```
    Minutes per bucket when sampling balances/equity for balance_and_equity.csv
    and related plots.
    1 keeps full per-minute resolution; higher values thin out the series
    (e.g., 15 stores one point every 15 minutes) to reduce file sizes.
    Default is 60, meaning that results are recorded every 60 minutes.
    Warning: Lower values increase the size of the backtest result files.
    A setting of 1 will create very large files and should only be used for small timeframes.
    Viewing large backtest result files may be slow or crash the browser.
    ```"""

loss_allowance_pct = """
    ```
    multisym auto unstuck: will use profits from other positions to offset
    losses realized on stuck positions
    how much below past peak balance to allow losses (default 1% == 0.01).
    Set to 0.0 to disable multisym auto unstuck.
    ```"""

pnls_max_lookback_days = """
    ```
    how far into the past to fetch pnl history
    ```"""

stuck_threshold = """
    ```
    if wallet_exposure / wallet_exposure_limit > stuck_threshold
    consider position as stuck
    ```"""

unstuck_close_pct = """
    ```
    Percentage of full pos size * wallet_exposure_limit to close for each unstucking order.
    ```"""

execution_delay_seconds = """
    ```
    wait x seconds after executing to exchange
    delay between executions to exchange. Set to 60 to simulate 1m ohlcv backtest.
    ```"""

auto_gs = """
    ```
    automatically enable graceful stop for positions on disapproved coins
    graceful stop means the bot will continue trading as normal, but not
    open a new position after current position is fully closed.
    ```"""

TWE_long_short = """
    ```
    total wallet exposure limits long and short.
    Exposure limit for each bot will be TWE_pos_side / len(active_symbols_pos_side)
    The WE from single/local config takes precedence.
    Example:
    Configured TWE 2.0
    2 symbols with local config WE 0.5
    3 symbols with default/universal config
    Result: 
    2 x 0.5 WE
    3 x 0.4 WE (2.0/5)
    Real TWE will be 2.2
    ```"""

multi_long_short_enabled = """
    ```
    if true, mode defaults to 'normal'.
    If false, mode defaults to 'manual'.
    ```"""

n_longs_shorts = """
    ```
    Max number of positions to have open.
    If n_longs and n_shorts are both zero, forager mode is disabled.
    n_longs: 0 // if > 0, overrides longs_enabled
    n_shorts: 0 // if > 0, overrides shorts_enabled
    ```"""

minimum_market_age_days = """
    ```
    minimum market age. Don't trade markets younger than x days. Set to zero to allow all markets.
    ```"""

ohlcv_interval = """
    ```
    interval of ohlcvs used for noisiness, volumes and EMAs
    ```"""

n_ohlcvs = """
    ```
    number of ohlcvs used for noisiness, volumes and EMAs
    ```"""

relative_volume_filter_clip_pct = """
    ```
    Volume filter: disapprove the lowest relative volume symbols. Default 0.1 == 10%. Set to zero to allow all.
    ```"""

max_n_per_batch = """
    ```
    how many executions in parallel per batch
    ```"""

max_n_restarts_per_day = """
    ```
    If the bot crashes for any reason, restart the bot up to n times per day before stopping completely.
    ```"""

ohlcvs_1m_rolling_window_days = """
    ```
    How many days worth of OHLCVs for the bot to keep in memory.
    Reduce this number if RAM consumption becomes an issue.
    ```"""

ohlcvs_1m_update_after_minutes = """
    ```
    How many minutes old OHLCVs for a coin may be before the bot will fetch fresh ones from the exchange.
    Increase this number if rate limiting becomes an issue.
    ```"""

inactive_coin_candle_ttl_minutes = """
    ```
    to control how long 1m candles for inactive symbols are kept in RAM before being refreshed
    ```"""

max_disk_candles_per_symbol_per_tf = """
    ```
    Maximum number of candles to store on disk per symbol per timeframe.
    Older candles will be deleted when this limit is exceeded.
    ```"""

max_memory_candles_per_symbol = """
    ```
    Maximum number of candles to keep in memory per symbol.
    Older candles will be deleted from memory when this limit is exceeded.
    ```"""

filter_by_min_effective_cost = """
    ```
    if true, will disallow symbols where balance * WE_limit * initial_qty_pct < min_effective_cost
    e.g. if exchange's effective min cost for a coin is $5, but bot wants to make an order of $2, disallow that coin.
    ```"""

forced_mode_long_short = """
    ```
    Force all positions to the same mode. Individually flagged modes take precedence.
    Choices: [n (normal), m (manual), gs (graceful_stop), p (panic), t (take_profit_only)]
    ```"""

multi_approved_symbols = """
    ```
    Approved symbols that are enabled and can be selected in forager mode
    Forager mode = Dynamically enable bots on markets of higher noisiness.
    Only select among approved_symbols defined.
    If approved_symbols == [], all symbols are eligible.
    ```"""

multi_ignored_symbols = """
    ```
    put on graceful_stop if auto_gs, else manual
    ```"""

multi_config_type = """
    ```
    Choose between default or universal config.
    ```"""

multi_universal_config = """
    ```
    Example format for universal config:
    {
      long:
      {
        ddown_factor: 0.8697
        ema_span_0:  776.7
        ema_span_1:  774.3
        initial_eprice_ema_dist:  -0.008465
        initial_qty_pct:  0.01167
        markup_range:  0.002187
        min_markup:  0.008534
        n_close_orders:  4.0
        rentry_pprice_dist:  0.04938
        rentry_pprice_dist_wallet_exposure_weighting:  2.143
      }
      short:
      {
        ddown_factor: 1.114
        ema_span_0: 1074.0
        ema_span_1: 786.2
        initial_eprice_ema_dist: -0.07048
        initial_qty_pct: 0.01296
        markup_range: 0.006174
        min_markup: 0.003647
        n_close_orders: 1.675
        rentry_pprice_dist: 0.05371
        rentry_pprice_dist_wallet_exposure_weighting: 2.492
      }
    }
    ```"""

default_config = """
    ```
    If symbol has no config, default to this config
    ```"""

config_version = """
    ```
    The Version number of the configuration. This number is required for
    synchronisation to your VPS. If the bot that runs this configuration
    see a new higher version number, it will switch to the new config.
    No need to manual change this number. It will automatical increased
    if you hit save.
    ```"""

pbshare_grid = """
    ```
    enable for generate grid picture and share them on gphoto
    ```"""
pbshare_bucket = """
    ```
    Select the rclone remote server where the grid pictures should be uploaded.
    ```"""
pbshare_interval = """
    ```
    Interval in seconds to generate grid pictures.
    ```"""
pbshare_upload_images = """
    ``` 
    Enable to upload grid pictures.
    ```"""
pbshare_download_index = """
    ```
    Download the index.html for preview.
    You can open and view it in your browser.
    You can upload it to your webserver to share your grid pictures.
    A simple free way to share it, is using github pages.
    ```"""
pbremote_bucket = """
    ```
    Select the rclone bucket to use for sync.
    ```"""

worst_drawdown_lower_bound = """
    ```
    will penalize worst_drawdowns greater than %
    ```"""

limits_lower_bound_drawdown_worst = """
    ```
    The optimizer will penalize backtests whose metrics exceed the given values
    lowest drawdown during backtest
    ```"""

limits_lower_bound_drawdown_worst_mean_1pct = """
    ```
    The optimizer will penalize backtests whose metrics exceed the given values
    mean of the worst 1% of drawdowns
    ```"""

limits_lower_bound_equity_balance_diff_mean = """
    ```
    The optimizer will penalize backtests whose metrics exceed the given values
    mean of the difference between equity and balance
    ```"""

limits_lower_bound_loss_profit_ratio = """

    ```
    The optimizer will penalize backtests whose metrics exceed the given values
    abs(sum(losses)) / sum(profit)
    ```"""

limits_lower_bound_position_held_hours_max = """
    ```
    The optimizer will penalize backtests whose metrics exceed the given values
    max hours a position is held
    ```"""

limits = """
    ```
    Optimizer Limits - Penalize backtests whose metrics exceed thresholds
    
    Penalties are added to every objective as a positive modifier; they do not
    disqualify a config but will push it far from the Pareto front when violated.
    
    Select a metric and configure when to penalize:
    - Metric: The performance metric to evaluate
    - Currency: For currency metrics, choose USD or BTC (default: USD)
    - Penalize If: Comparison operator (>, <, outside_range, inside_range)
    - Stat: Override aggregation statistic (min, max, mean, std)
    - Value/Range: The threshold value or [low, high] range
    
    ═══════════════════════════════════════════════════════════════════════
    RETURNS & GROWTH (currency metrics - append _usd or _btc):
    ═══════════════════════════════════════════════════════════════════════
    adg, adg_w                    Average Daily Gain (smoothed geometric) 
                                  and its recency-biased counterpart
    mdg, mdg_w                    Median Daily Gain and recency-biased variant
    gain                          Final balance gain (end/start ratio)
    *_per_exposure_{long,short}   Above metrics divided by exposure limit per side
    
    ═══════════════════════════════════════════════════════════════════════
    RISK METRICS (currency metrics - append _usd or _btc):
    ═══════════════════════════════════════════════════════════════════════
    drawdown_worst                Maximum peak-to-trough drawdown
    drawdown_worst_mean_1pct      Mean of worst 1% drawdowns (daily)
    expected_shortfall_1pct       Mean of worst 1% daily losses (CVaR)
    equity_balance_diff_neg_max   Largest negative equity-balance divergence
    equity_balance_diff_neg_mean  Average negative equity-balance divergence
    equity_balance_diff_pos_max   Largest positive equity-balance divergence
    equity_balance_diff_pos_mean  Average positive equity-balance divergence
    
    ═══════════════════════════════════════════════════════════════════════
    RATIOS & EFFICIENCY (currency metrics - append _usd or _btc):
    ═══════════════════════════════════════════════════════════════════════
    sharpe_ratio, sharpe_ratio_w                  Return-to-volatility ratio
    sortino_ratio, sortino_ratio_w                Return-to-downside-volatility ratio
    calmar_ratio, calmar_ratio_w                  Return divided by maximum drawdown
    sterling_ratio, sterling_ratio_w              Return divided by avg worst 1% drawdowns
    omega_ratio, omega_ratio_w                    Sum(positive returns) / sum(abs negative returns)
    
    ═══════════════════════════════════════════════════════════════════════
    EQUITY CURVE QUALITY (currency metrics - append _usd or _btc):
    ═══════════════════════════════════════════════════════════════════════
    equity_choppiness, equity_choppiness_w        Normalized total variation (lower=smoother)
    equity_jerkiness, equity_jerkiness_w          Normalized mean absolute 2nd derivative
    exponential_fit_error, exponential_fit_error_w MSE from log-linear equity fit
    peak_recovery_hours_equity                    Longest time equity stayed below prior peak
    
    ═══════════════════════════════════════════════════════════════════════
    POSITION & EXECUTION METRICS (shared - no currency suffix):
    ═══════════════════════════════════════════════════════════════════════
    positions_held_per_day        Average positions opened per day
    position_held_hours_max       Maximum holding time (hours)
    position_held_hours_mean      Average holding time (hours)
    position_held_hours_median    Median holding time (hours)
    position_unchanged_hours_max  Longest span without modifying a position
    volume_pct_per_day_avg        Average traded volume as % of account/day
    n_fills_per_day               Average number of fills per day
    loss_profit_ratio             abs(sum(losses)) / sum(profit)
    loss_streak_max               Maximum consecutive losing trades
    hrs_stuck_avg                 Average hours stuck in a position
    hrs_stuck_max                 Maximum hours stuck in a position
    n_positions_max               Maximum concurrent positions
    
    Metrics with *_w suffix use recency-weighted means (biased toward recent behavior).
    ```"""

limits_penalize_if = """
    ```
    Comparison operator determining when to penalize a backtest result:
    
    > or greater_than   Penalize if metric value > threshold
                        Use for metrics to minimize (drawdown, loss_profit_ratio)
                        Default stat: max
    
    < or less_than      Penalize if metric value < threshold
                        Use for metrics to maximize (adg, sharpe_ratio)
                        Default stat: min
    
    outside_range       Penalize if value < low OR value > high
                        Use to keep a metric within [low, high]
                        Default stat: mean
    
    inside_range        Penalize if low <= value <= high
                        Use to exclude a specific range of values
                        Default stat: mean
    
    auto                Automatically determine based on scoring weight:
                        - scoring weight < 0 (maximize): becomes less_than
                        - scoring weight > 0 (minimize): becomes greater_than
                        - metric not in scoring: limit is ignored
    ```"""

limits_stat = """
    ```
    Override the aggregation statistic for limit checking across exchanges/scenarios:
    
    mean    Use the mean value (default for range checks)
    min     Use the minimum value (default for less_than)
    max     Use the maximum value (default for greater_than)
    std     Use the standard deviation
    
    Leave empty to use the default based on penalize_if mode.
    min     Use the minimum value (default for less_than)
    max     Use the maximum value (default for greater_than)
    std     Use the standard deviation
    
    Leave empty to use the default based on penalize_if mode.
    ```"""

population_size = """
    ```
    Size of population for genetic optimization algorithm.
    ```"""

offspring_multiplier = """
    ```
    Multiplier applied to population_size to determine how many offspring (λ) are produced
    each generation in the μ+λ evolution strategy. Values >1.0 increase exploration by sampling
    more children per generation. Default is 1.0.
    ```"""

crossover_probability = """
    ```
    Probability of performing crossover between two individuals in the genetic algorithm.
    Determines how often parents exchange genetic information to create offspring.
    ```"""

crossover_eta = """
    ```
    Crowding factor (η) for simulated-binary crossover. Lower values (<20) allow offspring
    to move farther away from their parents; higher values keep them closer. Default is 20.0
    ```"""

mutation_probability = """
    ```
    Probability of mutating an individual in the genetic algorithm.
    Determines how often random changes are introduced to maintain diversity.
    ```"""

mutation_eta = """
    ```
    Crowding factor (η) for polynomial mutation. Smaller values (<20) produce heavier-tailed
    steps that explore more aggressively, while larger values confine mutations near the
    current value. Default is 20.0.
    ```"""

mutation_indpb = """
    ```
    Probability that each attribute mutates when a mutation is triggered.
    Set to 0 (default) to auto-scale to 1 / number_of_parameters,
    or supply an explicit probability between 0 and 1.
    ```"""

scoring = """
    ```
    The optimizer uses two objectives and finds the Pareto front.
    Chooses the optimal candidate based on the lowest Euclidean distance to the ideal point.
    Default values are median daily gain and Sharpe ratio.
    Uses the NSGA-II algorithm (Non-dominated Sorting Genetic Algorithm II) for multi-objective optimization.
    The fitness function minimizes both objectives (converted to negative values internally).
    ```"""

close_grid_parameters = """
    ```
    close_grid_markup_start, close_grid_markup_end, close_grid_qty_pct:
    Take Profit (TP) prices are linearly spaced between:
        pos_price * (1 + markup_start) to pos_price * (1 + markup_end) for long.
        pos_price * (1 - markup_start) to pos_price * (1 - markup_end) for short.
    The TP direction depends on the relative values of markup_start and markup_end:
        If markup_start > markup_end: TP grid is built backwards (starting at higher price and descending for long / ascending for short).
        If markup_start < markup_end: TP grid is built forwards (starting at lower price and ascending for long / descending for short).
    Example (long, backwards TP): If pos_price = 100, markup_start = 0.01, markup_end = 0.005, and close_grid_qty_pct = 0.2, TP prices are: [101.0, 100.9, 100.8, 100.7, 100.6].
    Example (long, forwards TP): If markup_start = 0.005, markup_end = 0.01, TP prices are: [100.5, 100.6, 100.7, 100.8, 100.9].
    Example (short, forwards TP): If pos_price = 100, markup_start = 0.005, markup_end = 0.01, TP prices are: [99.5, 99.4, 99.3, 99.2, 99.1].
    Example (short, backwards TP): If markup_start = 0.01, markup_end = 0.005, TP prices are: [99.0, 99.1, 99.2, 99.3, 99.4].
    Quantity per order is full pos size * close_grid_qty_pct.
    Note: Full position size refers to the maxed-out size. If the actual position is smaller, fewer than 1 / close_grid_qty_pct orders may be created.
    The TP grid is filled in order from markup_start to markup_end, allocating each slice up to the respective quantity:
        First TP up to close_grid_qty_pct * full_pos_size.
        Second TP from close_grid_qty_pct to 2 * close_grid_qty_pct, etc.
    Example: If full_pos_size = 100 and long_pos_size = 55, and prices are built backwards, then TP orders might be [15@100.8, 20@100.9, 20@101.0].
    If position exceeds full position size, excess size is added to the TP order closest to markup_start.
        Example: If long_pos_size = 130 and grid is forwards, TP orders are [50@100.5, 20@100.6, 20@100.7, 20@100.8, 20@100.9].
    ```"""

trailing_parameters = """
    ```
    The same logic applies to both trailing entries and trailing closes.

    trailing_grid_ratio:
        set trailing and grid allocations.
        if trailing_grid_ratio==0.0, grid orders only.
        if trailing_grid_ratio==1.0 or trailing_grid_ratio==-1.0, trailing orders only.
        if trailing_grid_ratio>0.0, trailing orders first, then grid orders.
        if trailing_grid_ratio<0.0, grid orders first, then trailing orders.
        e.g. trailing_grid_ratio = 0.3: trailing orders until position is 30% full, then grid orders for the rest.
        e.g. trailing_grid_ratio = -0.9: grid orders until position is (1 - 0.9) == 10% full, then trailing orders for the rest.
        e.g. trailing_grid_ratio = -0.12: grid orders until position is (1 - 0.12) == 88% full, then trailing orders for the rest.
    trailing_retracement_pct, trailing_threshold_pct:
        there are two conditions to trigger a trailing order: 1) threshold and 2) retracement.
        if trailing_threshold_pct <= 0.0, threshold condition is always triggered.
        otherwise, the logic is as follows, considering long positions:
        if highest price since position open > position price * (1 + trailing_threshold_pct): 1st condition is met
        and if lowest price since highest price < highest price since position open * (1 - trailing_retracement_pct): 2nd condition is met. Make order.
    close_trailing_qty_pct: close qty is full pos size * close_trailing_qty_pct
    ```"""

entry_grid_double_down_factor = """
    ```
    Quantity of the next grid entry is position size times the double down factor.
    Example: If position size is 1.4 and double_down_factor is 0.9, then the next entry quantity is 1.4 * 0.9 = 1.26.
    Also applies to trailing entries.
    ```"""

entry_grid_spacing_volatility_weight = """
    ```
    entry_grid_spacing_volatility_weight, entry_volatility_ema_span_hours
        The log_component in the multiplier above is derived from the EMA of the per-candle log range ln(high/low).
        entry_grid_spacing_volatility_weight controls how strongly the recent log range widens or narrows spacing. A value of 0 disables the log-based adjustment.
        entry_volatility_ema_span_hours sets the EMA span (in hours) used when smoothing the volatility (log-range) signal before applying the weight. The same volatility EMA also powers the multipliers for entry_trailing_threshold_volatility_weight and entry_trailing_retracement_volatility_weight.
    ```"""

entry_volatility_ema_span_hours = """
    ```
    entry_grid_spacing_volatility_weight, entry_volatility_ema_span_hours
        The log_component in the multiplier above is derived from the EMA of the per-candle log range ln(high/low).
        entry_grid_spacing_volatility_weight controls how strongly the recent log range widens or narrows spacing. A value of 0 disables the log-based adjustment.
        entry_volatility_ema_span_hours sets the EMA span (in hours) used when smoothing the volatility (log-range) signal before applying the weight. The same volatility EMA also powers the multipliers for entry_trailing_threshold_volatility_weight and entry_trailing_retracement_volatility_weight.
    ```"""

entry_trailing_threshold_we_weight = """
    ```
    entry_trailing_threshold_we_weight, entry_trailing_retracement_we_weight:
        Extra scaling based on wallet exposure. As exposure approaches the per-symbol limit, positive weights widen the trailing bands to slow additional entries.
        Set to 0.0 to disable the adjustment.
    ```"""

entry_trailing_threshold_volatility_weight = """
    ```
    entry_trailing_threshold_volatility_weight, entry_trailing_retracement_volatility_weight:
        Adds sensitivity to recent volatility using the shared entry_volatility_ema_span_hours EMA.
        Positive weights increase the thresholds in choppy markets; 0.0 removes the volatility modulation.
    ```"""

entry_trailing_retracement_we_weight = """
    ```
    entry_trailing_threshold_we_weight, entry_trailing_retracement_we_weight:
        Extra scaling based on wallet exposure. As exposure approaches the per-symbol limit, positive weights widen the trailing bands to slow additional entries.
        Set to 0.0 to disable the adjustment.
    ```"""

entry_trailing_retracement_volatility_weight = """
    ```
    entry_trailing_threshold_volatility_weight, entry_trailing_retracement_volatility_weight:
        Adds sensitivity to recent volatility using the shared entry_volatility_ema_span_hours EMA.
        Positive weights increase the thresholds in choppy markets; 0.0 removes the volatility modulation.
    ```"""

entry_grid_spacing = """
    ```
    entry_grid_spacing_pct, entry_grid_spacing_we_weight (formerly entry_grid_spacing_weight):
        Grid re-entry prices are determined as follows:
        next_reentry_price_long = pos_price * (1 - entry_grid_spacing_pct * multiplier)
        next_reentry_price_short = pos_price * (1 + entry_grid_spacing_pct * multiplier)
        
        where multiplier = 1 + (wallet_exposure / wallet_exposure_limit) * entry_grid_spacing_we_weight + log_component
        
        Setting entry_grid_spacing_we_weight > 0 widens spacing as the position approaches
        the wallet exposure limit; negative values tighten spacing when exposure is small.
    ```"""

entry_initial_ema_dist = """
    ```
    offset from lower/upper ema band.
    long_initial_entry/short_unstuck_close prices are lower ema band minus offset
    short_initial_entry/long_unstuck_close prices are upper ema band plus offset
    See ema_span_0/ema_span_1
    ```"""

entry_initial_qty_pct = """
    ```
    initial_entry_cost = balance * wallet_exposure_limit * initial_qty_pct
    ```"""

filter_ema_span = """
    ```
    filter_volatility_ema_span / filter_volume_ema_span:
    Number of minutes to look into the past to compute the volatility (log-range)
    and volume EMAs used for dynamic coin selection in forager mode.
    ```"""

filter_volume_drop_pct = """
    ```
    Coins selected for trading are filtered by volume and noisiness.
    First, filter coins by volume, dropping a percentage of the lowest volume coins.
    Then, sort the eligible coins by noisiness and select the top noisiest coins for trading.

    Volume filter. Disapproves the lowest relative volume coins.
    Example: filter_volume_drop_pct = 0.1 drops the 10% lowest volume coins. Set to 0 to allow all.    
    ```"""

filter_volatility_drop_pct = """
    ```
    Volatility clip. Drops the highest-volatility fraction after volume filtering.
    Example: 0.2 drops the top 20% most volatile coins, forcing the selector to choose among the calmer 80%.
    Log range is computed from 1m OHLCVs as mean(ln(high / low)).
    In forager mode, the bot selects coins with the highest log-range values for opening positions.
    ```"""

# filter_relative_volume_clip_pct = """
#     ```
#     Coins selected for trading are filtered by volume and noisiness.
#     First, filter coins by volume, dropping a percentage of the lowest volume coins.
#     Then, sort the eligible coins by noisiness and select the top noisiest coins for trading.

#     Volume filter; disapprove the lowest relative volume coins.
#     For example, filter_relative_volume_clip_pct = 0.1 drops the 10% lowest volume coins. Set to zero to allow all.
#     ```"""

# filter_rolling_window = """
#     ```
#     Coins selected for trading are filtered by volume and noisiness.
#     First, filter coins by volume, dropping a percentage of the lowest volume coins.
#     Then, sort the eligible coins by noisiness and select the top noisiest coins for trading.

#     Number of minutes to look into the past to compute volume and noisiness,
#     used for dynamic coin selection in forager mode.
#     Noisiness is normalized relative range of 1m OHLCVs: mean((high - low) / close).
#     In forager mode, the bot will select coins with highest noisiness for opening positions.
#     ```"""

n_positions = """
    ```
    Maximum number of positions to open. Set to 0 to disable long/short.
    ```"""

total_wallet_exposure_limit = """
    ```
    Maximum exposure allowed.
    Example: total_wallet_exposure_limit = 0.75 means 75% of (unleveraged) wallet balance is used.
    Example: total_wallet_exposure_limit = 1.6 means 160% of (unleveraged) wallet balance is used.
    Each position is given an equal share: wallet_exposure_limit = total_wallet_exposure_limit / n_positions.
    See more: docs/risk_management.md
    ```"""

unstuck_ema_dist = """
    ```
    distance from EMA band to place unstucking order:
    long_unstuck_close_price = upper_EMA_band * (1 + unstuck_ema_dist)
    short_unstuck_close_price = lower_EMA_band * (1 - unstuck_ema_dist)
    ```"""

unstuck_loss_allowance_pct = """
    ```
    Weighted percentage below past peak balance to allow losses.
    loss_allowance = past_peak_balance * (1 - unstuck_loss_allowance_pct * total_wallet_exposure_limit)
    
    Example: If past peak balance was $10,000, unstuck_loss_allowance_pct = 0.02,
    and total_wallet_exposure_limit = 1.5, the bot stops taking losses when balance
    reaches $10,000 * (1 - 0.02 * 1.5) = $9,700.
    ```"""

unstuck_threshold = """
    ```
    If a position is larger than the threshold, consider it stuck and activate unstucking.
    if wallet_exposure / wallet_exposure_limit > unstuck_threshold: unstucking enabled
    
    Example: If a position size is $500 and max allowed position size is $1000, the position is 50% full.
    If unstuck_threshold = 0.45, unstuck the position until its size is $450.
    ```"""

minimum_coin_age_days = """
    ```
    Disallows coins younger than a given number of days.
    ```"""

recv_window_ms = """
    ```
    Millisecond tolerance for authenticated REST calls (default 5000).
    Increase if your exchange intermittently rejects requests with
    "invalid request ... recv_window" errors due to clock drift.
    ```"""

order_match_tolerance_pct = """
    ```
    Tolerance percentage for matching existing orders with new orders (default 0.0002 = 0.02%).
    When the bot compares orders to create vs orders to cancel, it uses this tolerance
    for price and quantity matching. If an existing order matches a new order within
    this tolerance, both the cancel and create are skipped to avoid unnecessary
    order churn. This reduces exchange API calls and avoids recreating nearly
    identical orders.
    ```"""

balance_override = """
    ```
    Manually override the exchange balance with a fixed value.
    When set to a value > 0, the bot uses this value instead of fetching 
    the actual balance from the exchange.
    
    Use cases:
    - Risk management: Limit the balance the bot uses for position sizing
    - Multiple bots: Run multiple bots on the same account with allocated portions
    
    Set to 0 to disable and use the actual exchange balance.
    ```"""

balance_hysteresis_snap_pct = """
    ```
    Percentage threshold for balance change before updating position calculations (default: 0.02 = 2%).
    
    The bot only updates its "snapped" balance when the actual balance deviates more than
    this percentage from the current snapped value. This prevents small balance fluctuations
    (from fees, small PnL changes) from constantly recalculating position sizes.
    
    Example: With 2% threshold, if snapped balance is $10,000, the balance must move
    above $10,200 or below $9,800 before triggering a recalculation.
    ```"""

max_warmup_minutes = """
    ```
    Hard ceiling applied to the historical warm-up window for both backtests and live warm-ups.
    Use 0 to disable the cap; otherwise values above 0 clamp the per-symbol warmup
    calculated from EMA spans.
    Default: 0 (disabled)
    ```"""

memory_snapshot_interval_minutes = """
    ```
    Interval between memory snapshot telemetry entries (RSS, cache footprint, 
    asyncio task counts) in minutes (default: 30).
    Lower values surface memory leaks sooner, higher values reduce log noise.
    ```"""

volume_refresh_info_threshold_seconds = """
    ```
    Minimum duration a bulk volume-EMA refresh must take before it is promoted 
    to an INFO log (default: 30).
    Runs that finish faster emit only DEBUG output (when debug logging is enabled).
    Set to 0 to keep the previous always-INFO behaviour.
    ```"""

pareto_max_size = """
    ```
    Maximum number of Pareto-optimal configs kept on disk under optimize_results/.../pareto/.
    Members are pruned by crowding (least diverse removed first, while per-objective
    extremes are preserved), not by age.
    ```"""

ohlcv_rolling_window = """
    ```
    number of minutes to look into the past to compute volume and noisiness,
    used for dynamic coin selection in forager mode.
        noisiness is normalized relative range of 1m ohlcvs: mean((high - low) / close)
        in forager mode, bot will select coins with highest noisiness for opening positions
    ```"""

time_in_force = """
    ```
    Time in force indicates how long your order will remain active before it is executed or expired.
    GTC (good_till_cancelled: The order will last until it is completed or you cancel it.
    PostOnly (post_only): If the order would be filled immediately when submitted, it will be cancelled.
    ```"""

apply_filters = """
    ```
    If true, will apply filters to the coins.    
    ```"""

only_cpt = """
    ```
    If true, will only trade coins that are allowed for CopyTrading.
    ```"""

market_cap = """
    ```
    minimum market capitalization in millions of USD (1 million = 1'000'000)
    ```"""

vol_mcap = """
    ```
    minimum volume to market cap ratio
    ```"""

coindata_tags = """
    ```
    list of tags to filter coins by
    ```"""

coindata_api_key = """
    ```
    CoinMarketCap API key
    https://coinmarketcap.com/api/pricing/
    Basic Free is good enough.
    ```"""

coindata_fetch_limit = """
    ```
    limit of coins to fetch from CoinMarketCap
    ```"""

coindata_fetch_interval = """
    ```
    interval in hours to fetch coins from CoinMarketCap
    Make sure to not exceed the Basic Plan limit of 10'000 api calls per month.
    You need 1 credit for 200 coins. So count your coins and interval to not exceed the limit.
    ```"""

coindata_metadata_interval = """
    ```
    interval in days to fetch metadata from CoinMarketCap
    Make sure to not exceed the Basic Plan limit of 10'000 api calls per month.
    You need 1 credit for 100 coins. So count your coins and interval to not exceed the limit.
    ```"""

market_orders_allowed = """
    ```
    If true, allow Passivbot to place market orders when order price is very close to current
    market price. If false, will only place limit orders. Default is true.
    ```"""

mimic_backtest_1m_delay = """
    ```
    If true, the bot will only update and evaluate open orders once per full minute, synchronized to the clock (e.g., 12:01:00, 12:02:00, etc.).
    This mimics the backtester's timestep logic and avoids intraminute updates. Useful for achieving higher fidelity between backtest and live performance.
    ```"""

approved_coins = """
    ```
    list of approved coins to trade
    You can not add coins here if they are in ignored_coins.
    ```"""

ignored_coins = """
    ```
    list of coins to ignore
    If you add coins here, they will be removed from approved_coins.
    ```"""

dynamic_ignore = """
    ```
    If enabled, PBRun will dynamically maintain the ignored_coins list.
    The list will created using the market_cap, vol_mcap and tags filters.
    If only_cpt is enabled, coins not allowed for CopyTrading will be
    If notices_ignore is enabled, coins with notices will be added to the ignored_coins list.
    added to the ignored_coins list. Coins in ignored_symbols_long or
    ignored_symbols_short will also be added to the ignored_coins list.
    Update interval is configured in PBCoinData.
    On passivbot6 PBRun will restart the bot if needed.
    On passivbot7 PBRun creat the ignored_coins.json file and pb7 will use this list as filter.
    ```"""

notices_ignore = """
    ```
    If true, will only trade coins that has no notice warning on CoinMarketCap.
    ```"""

empty_means_all_approved = """
    ```
    To combine the ohlcv data from multiple exchanges into a single array.
    Otherwise, backtest for each exchange individually
    ```"""

gap_tolerance_ohlcvs_minutes = """
    ```
    If the gap between two consecutive ohlcvs is greater than this value, the bot will not backtest.
    ```"""

warmup_ratio = """
    ```
    Multiplier applied to the longest EMA or log-range span (in minutes) across long/short settings
    to decide how much 1m history to prefetch before trading. A value of 0.2,
    for example, warmups ~20% of the deepest lookback, capped by backtest.max_warmup_minutes
    ```"""

combine_ohlcvs = """
    ```
    When true, build a single "combined" dataset by taking the best-quality feed
    for each coin across all configured exchanges. When false, the
    backtester/optimizer runs each exchange independently.
    ```"""

compress_cache = """
    ```
    Set to true to save disk space. Set to false for faster loading.
    ```"""

bt_filter_by_min_effective_cost = """
    ```
    If true, skip coins whose projected initial entry
    (balance * wallet_exposure_limit * entry_initial_qty_pct)
    would fall below the exchange's effective minimum cost.
    ```"""

btc_collateral_cap = """
    ```
    Target (and ceiling) share of account equity to hold in BTC collateral.
    0 keeps the account fully in USD;
    1.0 mirrors the legacy 100% BTC mode;
    values >1 allow leveraged BTC collateral, accepting negative USD balances.
    ```"""

btc_collateral_ltv_cap = """
    ```
    Optional loan-to-value ceiling (USD debt ÷ equity) enforced when topping up BTC.
    Leave null (default) to allow unlimited debt, or set to a float (e.g., 0.6) to stop
    buying BTC once leverage exceeds that threshold.
    ```"""

compress_results_file = """
    ```
    If true, compresses optimize output results file to save space.
    ```"""

write_all_results = """
    ```
    If true, will write all optimize results to output file.
    ```"""

starting_config = """
    ```
    Start the optimizer with config.
    ```"""

vps_swap = """
    ```
    recommended swap size for VPS
    2GB for VPS with 1GB RAM and 10GB SSD
    4GB for VPS with 2GB RAM and 25GB SSD
    8GB for VPS with 4GB RAM and 50GB SSD
    ```"""

vps_ip = """
    ```
    IP of your VPS (Example: 193.123.150.99)
    ```"""

vps_hostname = """
    ```
    New Hostname of your VPS (Example: mypassivbot01)
    ```"""
vps_initial_root_pw = """
    ```
    The initial root password of your VPS
    ```"""

vps_root_pw = """
    ```
    The new root password of your VPS
    This password will be set after the installation
    ```"""

vps_init_methode = """
    ```
    Method to access your VPS for initial setup
    root: Access your vps with root user and password
    password: Access your vps with user and password (needs a user with sudo rights)
    private_key: Access your vps with user and private key (needs a user with sudo rights)
    ```"""

vps_remove_user = """
    ```
    Enable to remove the user that was used for initial setup
    If the init user is not needed anymore, you can remove it for security reasons
    ```"""

vps_user_sudo = """
    ```
    Your user on vps that has sudo rights
    ```"""

vps_user_sudo_pw = """
    ```
    The password of your vps user that has sudo rights
    ```"""

vps_private_key_user = """
    ```
    Your user with private_key on vps that has sudo rights
    ```"""

vps_private_key_file = """
    ```
    The path/file to your private_key that has access to your vps user
    Example: /home/username/.ssh/username.pem
    Recommended:
    For security reasons, set 400 permissions on the private key file
    chmod 400 /home/username/.ssh/username.pem
    ```"""

vps_user = """
    ```
    Your linux username
    Use the same user on local and all vps, for easy ssh to your vps
    The installer will add your public ssh key to the vps user
    ```"""

vps_user_pw = """
    ```
    Your user password on your vps.
    This will be set when run init.
    Will be used for sudo when run setup.
    ```"""

vps_install_pb6 = """
    ```
    Enable to install passivbot6 on your vps
    If disabled, only passivbot7 will be installed.
    ```"""

vps_firewall = """
    ```
    Enable to install and configure ufw firewall on your vps
    ```"""

vps_firewall_ssh_port = """
    ```
    The ssh port on your vps
    ```"""

vps_firewall_ssh_ips = """
    ```
    List of allowed IPs for ssh, separated by comma, empty for all
    Example: 10.20.10.11, 10.21.22.33
    ```"""

master_user_pw = """
    ```
    The password of the local user on your master machine
    ```"""

role = """
    ```
    If master:
    - PBRemote will download alive data from the VPS
    If slave:
    - PBRemote will upload alive data to the VPS
    ```"""

sudo_pw = """
    ```
    The sudo password of your local user
    This is needed for:
    - install rclone
    - install rustup
    - update PB7 venv
    - install PBGui venv
    - add IP-Address to hosts file
    ```"""

install_pbgui_venv = """
    ```
    Create or update a parallel PBGui virtual environment using Python 3.12.

    What it does:
    - Installs python3.12-venv (requires sudo)
    - Creates/updates: ~/software/venv_pbgui312
    - Installs dependencies from: <pbgui>/requirements.txt

    What it does NOT do:
    - It does not switch the running PBGui/Streamlit instance to Python 3.12.

    Next step (manual):
    - Run: pbgui/setup/mig_py312.sh

    Rollback (manual):
    - Run: pbgui/setup/mig_py310.sh

    Notes:
    - requirements.txt is the default (Python 3.12)
    - requirements310.txt keeps the pinned Python 3.10 baseline
    ```"""

update_pb7_venv = """
    ```
    Recreate your local PB7 virtual environment using Python 3.12.
    Requires sudo password (to install python3.12-venv).

        Recommendation:
        - It is possible to run the update while bots are active; they will be stopped automatically during the update and started again afterwards (via PBRun restart).
        - Better: Move the bots to another VPS first, then update the venv.

    Steps:
    - Stop PBRun
    - Kill running PB7 processes (bots + workers)
    - Install python3.12-venv
    - Delete and recreate venv_pb7 with python3.12
    - Install PB7 requirements and rebuild passivbot-rust
    - Start PBRun
    ```"""

update_pb7_venv_vps = """Recreate PB7 venv on the selected VPS using Python 3.12.

Prerequisite:
- Enter the VPS user password under "VPS Setup Settings" (used for SSH + sudo).

Recommendation:
- It is possible to run the update while bots are active; they will be stopped automatically during the update and started again afterwards (via PBRun restart).
- Better: Move the bots to another VPS first, then update the venv.

Steps:
- Stop PBRun on the VPS
- Kill running PB7 processes (bots + workers)
- Install python3.12-venv (sudo)
- Delete and recreate venv_pb7 with python3.12
- Update PB7 repository to the latest commit of the current branch
- Install PB7 requirements and rebuild passivbot-rust
- Start PBRun

Note: sudo uses the VPS user password configured in the VPS settings.
"""

update_pbgui_venv_vps = """Create/update a parallel PBGui venv on the selected VPS using Python 3.12.

Prerequisite:
- Enter the VPS user password under "VPS Setup Settings" (used for SSH + sudo).

What it does:
- Stops PBRun + PBRemote + PBCoinData
- Installs python3.12-venv (sudo)
- Deletes and recreates: ~/software/venv_pbgui
- Installs dependencies from: <pbgui>/requirements_vps.txt
- Starts PBRun + PBRemote + PBCoinData

Warning:
- This replaces your existing venv_pbgui on the VPS.

Note: sudo uses the VPS user password configured in the VPS settings.
"""

cleanup_vps = """Free disk space on the selected VPS.

Prerequisite:
- Enter the VPS user password under "VPS Setup Settings" (used for SSH + sudo).

What it does:
- Removes Snap (snapd)
- Runs apt cleanup (autoremove/autoclean/clean)
- Shrinks systemd journal logs (keeps ~1 day)

Recommendation:
- After big updates (PBGui/PB7) on small VPS, run this once to free disk space.

Warning:
- This removes Snap; only use it if you don't rely on snap-installed apps.
"""

pb7_version_venv_python = """
PB7 column

Shows the PB7 version from README.md.

If available, the suffix "/3.12" indicates the Python major.minor of the configured PB7 venv interpreter (pb7venv).
"""

pbgui_version_venv_python = """
PBGui column

Shows the PBGui version.

If available, the suffix "/3.12" indicates the Python major.minor of the Python environment running PBGui/PBRun/PBRemote (typically venv_pbgui).
"""
   
smart_filter = """
    ```
    This filter is used for smart filter options.
    "*": Matches all strings.
    "abc*": Matches any string starting with "abc".
    "*xyz": Matches any string ending with "xyz".
    "abc*xyz": Matches any string starting with "abc" and ending with "xyz".
    ```"""
    
change_password = """
    ```
    You can change your password here.
    If you leave the new password empty, then authentication will be disabled.
    If authentication is disabled, you can use this dailog to set a new password.
    ```"""

coin_overrides_config = """
    ```
    Whole config for this coin.
    ```"""
coin_overrides_mode = """
    ```
    Normal mode: Passivbot manages the position as normal.
    Manual mode: Passivbot ignores the position.
    Graceful stop: If there is a position, Passivbot manages it; otherwise, no new positions are opened.
    Take profit only mode: Passivbot only manages closing orders.
    Panic mode: Passivbot closes the position immediately.
    If not set, the mode from the config will be used.
    ```"""

coin_overrides_leverage = """
    ```
    Leverage for this coin.
    If 0.0, the leverage from the config will be used.
    ```"""

coin_overrides_parameters = """
    ```
    Parameters for this coin.
    ```"""

coin_flags_mode = """
    -lm or -sm: Long or short mode. Choices:
     [n (normal), m (manual), gs (graceful_stop), p (panic), t (take_profit_only)].
    Normal mode: passivbot manages the position as normal.
    Manual mode: passivbot ignores the position.
    Graceful stop: if there is a position, passivbot will manage it; otherwise, passivbot will not make new positions.
    Take profit only: passivbot will only manage closing orders.
    ```"""

coin_flags_we = """
    -lw or -sw: Long or short wallet exposure limit.
    ```"""

coin_flags_config = """
    -lc: Path to live config. Load all of another config's bot parameters except
     [n_positions, total_wallet_exposure_limit, unstuck_loss_allowance_pct, unstuck_close_pct].
    ```"""

coin_flags_lev = """
    -lev: Leverage.
    ```"""

pbmon_telegram_token = """
    ```
    Your Telegram Bot Token

    Create a Telegram bot and obtain its API key. You can do this by talking to the BotFather in Telegram. To do this:
    Open the Telegram app on your smartphone or desktop.
    Search for the “BotFather” username in the search bar.
    Click on the “Start” button to start a conversation with the BotFather.
    Type “/newbot” and follow the prompts to create a new bot. The BotFather will give you an API key that you will add as Telgram Bot Token.
    ```"""  

pbmon_telegram_chat_id = """
    ```
    Your Telegram Chat ID
    ```"""

archive_name = """
    ```
    Name of the archive
    ```"""
archive_url = """
    ```
    Github URL of the archive
    Archives:
    https://github.com/msei99/pbconfigs.git
    https://github.com/RustyCZ/pb-configs.git
    ```"""

my_archive = """
    ```
    Select your own archive that you already created on github and added to the archive list.
    ```"""

my_archive_path = """
    ```
    Path where inside the archive the config gte published.
    ```"""

my_archive_username = """
    ```
    Your github username
    ```"""
my_archive_email = """
    ```
    Your github email
    ```"""

my_archive_access_token = """
    ```
    Your github access token that you created for the archive.
    give the token read/write access to content.
    ```"""

dashboard_last = """
    ```
    If last_n is set, the dashboard will show the last N entries as a list.
    ```"""

dashboard_filter = """
    ```
    In list view, you can use the filter to narrow down the displayed entries.
    ```"""

# Help text for showing raw logfile (used in logging_view.py)
show_raw_log = """
    ```
    Show raw logfile (no filters applied). Use this to inspect the full
    unfiltered log content for debugging or timestamp/format validation.
    ```"""

backtest_div_by = """
    ```
    Divisor for backtesting. Determines the granularity of backtesting csv results.
    Default is 60, meaning that results are recorded every 60 minutes.
    Warning: Lower values increase the size of the backtest result files.
    A setting of 1 will create very large files and should only be used for small timeframes.
    Viewing large backtest result files may be slow or crash the browser.
    ```"""

TWE_long_short = """
    ```
    total wallet exposure limits long and short.
    Exposure limit for each bot will be TWE_pos_side / len(active_symbols_pos_side)
    The WE from single/local config takes precedence.
    Example:
    Configured TWE 2.0
    2 symbols with local config WE 0.5
    3 symbols with default/universal config
    Result: 
    2 x 0.5 WE
    3 x 0.4 WE (2.0/5)
    Real TWE will be 2.2
    ```"""

ohlcvs_1m_rolling_window_days = """
    ```
    How many days worth of OHLCVs for the bot to keep in memory.
    Reduce this number if RAM consumption becomes an issue.
    ```"""

ohlcvs_1m_update_after_minutes = """
    ```
    How many minutes old OHLCVs for a coin may be before the bot will fetch fresh ones from the exchange.
    Increase this number if rate limiting becomes an issue.
    ```"""

instance_note = """
    ```
    Your personal note for this instance. It is only intended to help you organise your instances.
    ```"""

task_name = r"""
    ```
    Name of the task
    The following characters are not allowed: / \ : * ? " < > |
    ```"""
    

limits_lower_bound_drawdown_worst_mean_1pct = """
    ```
    The optimizer will penalize backtests whose metrics exceed the given values
    mean of the worst 1% of drawdowns
    ```"""

filter_rolling_window = """
    ```
    Coins selected for trading are filtered by volume and noisiness.
    First, filter coins by volume, dropping a percentage of the lowest volume coins.
    Then, sort eligible coins by noisiness and select the top noisiest coins for trading.

    Number of minutes to look into the past to compute volume and noisiness,
    used for dynamic coin selection in forager mode.
    Noisiness is normalized relative range of 1m OHLCVs: mean((high - low) / close).
    In forager mode, the bot selects coins with the highest noisiness for opening positions.
    ```"""

# filter_relative_volume_clip_pct = """
#     ```
#     Coins selected for trading are filtered by volume and noisiness.
#     First, filter coins by volume, dropping a percentage of the lowest volume coins.
#     Then, sort the eligible coins by noisiness and select the top noisiest coins for trading.

#     Volume filter; disapprove the lowest relative volume coins.
#     For example, filter_relative_volume_clip_pct = 0.1 drops the 10% lowest volume coins. Set to zero to allow all.
#     ```"""

# filter_rolling_window = """
#     ```
#     Coins selected for trading are filtered by volume and noisiness.
#     First, filter coins by volume, dropping a percentage of the lowest volume coins.
#     Then, sort the eligible coins by noisiness and select the top noisiest coins for trading.

#     Number of minutes to look into the past to compute volume and noisiness,
#     used for dynamic coin selection in forager mode.
#     Noisiness is normalized relative range of 1m OHLCVs: mean((high - low) / close).
#     In forager mode, the bot will select coins with highest noisiness for opening positions.
#     ```"""

mimic_backtest_1m_delay = """
    ```
    If true, the bot will only update and evaluate open orders once per full minute, synchronized to the clock (e.g., 12:01:00, 12:02:00, etc.).
    This mimics the backtester's timestep logic and avoids intraminute updates. Useful for achieving higher fidelity between backtest and live performance.
    ```"""

vps_install_pb6 = """
    ````
    Enable to install passivbot6 on your vps
    If disabled, only passivbot7 will be installed.
    ```"""

   

    

  

# Risk management parameters
risk_wel_enforcer_threshold = """
    ```
    Per-symbol multiplier that triggers the WEL enforcer.
    When a position's exposure exceeds wallet_exposure_limit * (1 + risk_we_excess_allowance_pct) * risk_wel_enforcer_threshold,
    the bot emits a reduce-only order to bring it back under control.
    Set <1.0 for continual trimming, 1.0 for a hard cap, or ≤0 to disable.
    ```"""

risk_we_excess_allowance_pct = """
    ```
    Per-symbol allowance above the configured wallet exposure limit that the enforcer tolerates before trimming.
    Useful for smoothing reductions; leave at 0.0 for a hard cap.
    ```"""

risk_twel_enforcer_threshold = """
    ```
    Fraction of the configured total_wallet_exposure_limit that triggers the TWEL enforcer.
    When aggregate exposure exceeds this threshold the bot queues reduction orders instead of new entries.
    Set >1.0 to allow a grace margin, 1.0 for strict enforcement, or ≤0 to disable.
    ```"""

# ═══════════════════════════════════════════════════════════════════════
# SUITE CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════

suite_enabled = """
    ```
    Enable suite mode for multi-scenario backtesting/optimization.
    
    When enabled, the optimizer/backtester evaluates your config across
    all defined scenarios. Each scenario can test different:
    - Coin sets (e.g., "Top 10 MarketCap", "High Volatility Coins")
    - Date ranges (e.g., "Bull Market 2024", "Bear Market 2022")
    - Exchanges (e.g., only Binance, only Bybit)
    - Parameter variations (e.g., n_positions=3 vs n_positions=7)
    
    Results are aggregated across scenarios using the aggregate setting.
    This helps find robust configs that work across different market conditions.
    ```"""

suite_include_base_scenario = """
    ```
    Include base scenario in suite evaluation.
    
    When enabled, the optimizer prepends an auto-generated "base" scenario
    that mirrors your main config settings (coins, dates, exchanges).
    This lets you compare scenario variations against your baseline.
    ```"""

suite_base_label = """
    ```
    Directory name for the auto-generated base scenario.
    Only used when "Include Base Scenario" is enabled.
    Default: "base"
    ```"""

suite_aggregate = """
    ```
    How to aggregate metrics across all scenarios for scoring.
    
    Default aggregation applies to all metrics unless overridden.
    You can specify different aggregation methods per metric.
    
    Available methods:
    - mean:   Average of all scenario results (recommended default)
    - min:    Worst-case result across scenarios  
    - max:    Best-case result across scenarios
    - std:    Standard deviation (variability across scenarios)
    - median: Middle value across scenarios
    
    Example: Use "mean" as default, but "max" for drawdown_worst to ensure
    the optimizer penalizes configs that fail badly in any scenario.
    
    The aggregated values feed into optimize.scoring and optimize.limits.
    ```"""

scenario_label = """
    ```
    Unique identifier for this scenario.
    Used in result directories: backtests/suite_runs/<timestamp>/<label>/
    
    Examples: "bull_market_2024", "top10_mcap", "pure_trailing", "n_positions_3"
    ```"""

scenario_coins = """
    ```
    Override approved_coins for this scenario.
    Comma-separated list of coin symbols.
    Leave empty to use the base config's approved coins.
    
    Examples:
    - "BTC, ETH, SOL, XRP, ADA" (specific coins)
    - "BTC/USDT:USDT, ETH/USDT:USDT" (full symbol format)
    ```"""

scenario_ignored_coins = """
    ```
    Coins to exclude from this scenario.
    Comma-separated list. Applied after the coins filter.
    ```"""

scenario_exchanges = """
    ```
    Restrict which exchanges' data this scenario can see.
    Leave empty to use all exchanges from the base config.
    
    Useful for testing exchange-specific behavior:
    - Select only "binance" to test Binance-specific coin sets
    - Select only "bybit" to test Bybit performance
    ```"""

scenario_overrides = """
    ```
    Override bot parameters for this scenario.
    Format: one override per line as key=value
    
    Supports nested keys with dot notation:
    - bot.long.n_positions=3
    - bot.long.entry_trailing_grid_ratio=1
    - bot.short.total_wallet_exposure_limit=0.5
    
    Useful for testing parameter sensitivity:
    - Compare pure_grid (trailing_grid_ratio=0) vs pure_trailing (ratio=1)
    - Test different n_positions values
    - Compare different exposure limits
    ```"""

suite_add_metric = """
    Select which metric to configure for scenario aggregation.
    Keys fall back to the default aggregation entry if unspecified.
    The aggregated values feed into optimize.scoring and optimize.limits."""

suite_add_aggregation = """
    Choose how to aggregate this metric across all scenarios:
    - mean:   Average of all scenario results (default)
    - min:    Worst-case result across scenarios  
    - max:    Best-case result across scenarios
    - std:    Standard deviation (variability across scenarios)
    - median: Middle value across scenarios"""

suite_add_button = """Add metric-specific aggregation rule"""

coin_sources_delete = """Check to delete this coin source"""

coin_sources_coin = """Coin symbol to override"""

coin_sources_exchange = """Target exchange for this coin"""

coin_sources_select_exchange = """
    Optional mapping of coin → exchange to override automatic selection when combine_ohlcvs is true.
    You can choose any exchange to force this coin to use specific exchange data."""

coin_sources_select_coin = """
    Select coin from the chosen exchange.
    Scenarios may add more overrides; conflicting assignments (same coin to different exchanges) will raise an error."""

scenario_start_date = """
    Override start date for this scenario only.
    Leave empty to use base config start date."""

scenario_end_date = """
    Override end date for this scenario only.
    Leave empty to use base config end date."""

scenario_clear_date = """Clear date (use base config)"""

stats_aggregation_help = """
    Aggregation method used across scenarios.
    Only shown in Aggregated view where scenario results are combined."""

stats_value_help = """Aggregated metric value across all scenarios"""

# Additional OptimizeV7 help texts
exchanges = """
    Exchanges from which to fetch 1m OHLCV data for backtesting and optimizing.
    The template ships with ['binance', 'bybit']; additional exchanges can be wired up
    manually if you maintain your own archives."""

backtest_start_date = """
    Start date of backtest.
    Format: YYYY-MM-DD (e.g., 2024-01-01)"""

backtest_end_date = """
    End date of backtest.
    Format: YYYY-MM-DD (e.g., 2024-06-23)
    Set to 'now' to use today's date as the end date."""

starting_balance = """
    Starting balance in USD at the beginning of the backtest."""

n_cpus = """
    Number of CPU cores utilized in parallel during optimization."""

approved_coins_long = """
    List of coins approved for long positions.
    If empty, see live.empty_means_all_approved.
    Backtester and optimizer use live.approved_coins minus live.ignored_coins."""

approved_coins_short = """
    List of coins approved for short positions.
    If empty, see live.empty_means_all_approved.
    Backtester and optimizer use live.approved_coins minus live.ignored_coins."""

limit_currency = """
    Select currency denomination for this metric.
    Metrics may be suffixed with _usd or _btc to select denomination.
    If config.backtest.btc_collateral_cap is 0, BTC values still represent
    the USD equity translated into BTC terms."""

limit_range_low = """
    Lower bound of the allowed range for this metric.
    Used with 'outside_range' or 'inside_range' penalty conditions."""

limit_range_high = """
    Upper bound of the allowed range for this metric.
    Used with 'outside_range' or 'inside_range' penalty conditions."""

limit_value = """
    Threshold value for this metric limit.
    Used with '<' (less than) or '>' (greater than) penalty conditions."""

scenario_override_side = """
    Select which side (long or short) this parameter override applies to.
    Only a subset of config parameters are eligible for overriding master config."""

scenario_override_param = """
    Select which bot parameter to override for this scenario.
    Eligible parameters from config.bot.long/short include:
    close_grid_markup_end, close_grid_markup_start, close_grid_qty_pct,
    close_trailing_grid_ratio, close_trailing_qty_pct, close_trailing_retracement_pct,
    close_trailing_threshold_pct, ema_span_0, ema_span_1, entry_grid_double_down_factor,
    entry_grid_spacing_pct, entry_grid_spacing_we_weight, entry_grid_spacing_volatility_weight,
    entry_volatility_ema_span_hours, entry_initial_ema_dist, entry_initial_qty_pct,
    entry_trailing_double_down_factor, entry_trailing_grid_ratio, entry_trailing_retracement_pct,
    entry_trailing_threshold_pct, unstuck_close_pct, unstuck_ema_dist, unstuck_threshold,
    wallet_exposure_limit."""

scenario_override_value = """
    Value for the parameter override.
    This value will replace the base config value for this scenario."""

# Button help texts
add_limit_button = """
    Add this limit to the optimization constraints.
    The optimizer will penalize configs that violate this limit."""

add_coin_source_button = """
    Add this coin source override.
    Maps the selected coin to use data from the selected exchange."""

add_scenario_override_button = """
    Add this parameter override to the scenario.
    The override will apply only to this scenario, not to the base config."""

add_scenario_button = """
    Add new scenario to the suite.
    Scenarios allow testing the same config across different market conditions."""

