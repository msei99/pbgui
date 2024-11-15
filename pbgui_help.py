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
    python PBStat.py
    # python PBShare.py
    # python PBData.py
    # python PBCoinData.py

    Run "chmod 755 start.sh" and change the path to your needs
    Run "crontab -e" and add the @reboot with your path
    ```"""

pbshare = """
    ```
    This is the Data Sharing Manager from PBGUI.
    Enable, to start generating live grid and other statistics to share.
    To start the Data Sharing Manager after reboot your server, you have to
    start PBShare.py when your Server starts.
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
    # python PBStat.py
    python PBShare.py
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
    # python PBStat.py
    # python PBShare.py
    python PBData.py
    # python PBCoinData.py

    Run "chmod 755 start.sh" and change the path to your needs
    Run "crontab -e" and add the @reboot with your path
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
    percentage of balance * wallet_exposure_limit to close for each unstucking order
    (default 1% == 0.01)
    ```"""

execution_delay_seconds = """
    ```
    wait x seconds after executing to exchange
    delay between executions to exchange. Set to 60 to simulate 1m ohlcv backtest.
    ```"""

price_distance_threshold = """
    ```
    minimum distance to current price action required for EMA based limit orders
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

instance_note = """
    ```
    Your personal note for this instance. It is only intended to help you organise your instances.
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

population_size = """
    ```
    size of population for genetic optimization algorithm
    ```"""

crossover_probability = """
    ```
    The probability of performing crossover between two individuals in the genetic algorithm.
    It determines how often parents will exchange genetic information to create offspring.
    ```"""

mutation_probability = """
    ```
    The probability of performing crossover between two individuals in the genetic algorithm.
    It determines how often parents will exchange genetic information to create offspring.
    ```"""

scoring = """
    ```
    the optimizer uses two objectives and finds the pareto front,
    finally choosing the optimal candidate based on lowest euclidian distance to ideal point.
    default values are median daily gain and sharpe ratio
    ```"""

close_grid_parameters = """
    ```
    close_grid_markup_range, close_grid_min_markup, close_grid_qty_pct:
    Take Profit (TP) prices are spread out from
        pos_price * (1 + min_markup) to pos_price * (1 + min_markup + markup_range) for long
        pos_price * (1 - min_markup) to pos_price * (1 - min_markup - markup_range) for short
        e.g. if pos_price==100, min_markup=0.01, markup_range=0.02 and close_grid_qty_pct=0.2, TP prices are [101, 101.5, 102, 102.5, 103]
        qty per order is full pos size * close_grid_qty_pct
    the TP grid is built from the top down:
        first TP at 103 up to 20% of full pos size,
        next TP at 102.5 from 20% to 40% of full pos size,
        next TP at 102.0 from 40% to 60% of full pos size,
        etc.    ```"""

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
    quantity of next grid entry is position size times double down factor.
    E.g. if position size is 1.4 and double_down_factor is 0.9, then next entry quantity is 1.4 * 0.9 == 1.26.
    also applies to trailing entries.
    ```"""

entry_grid_spacing = """
    ```
    entry_grid_spacing_pct, entry_grid_spacing_weight:
        grid re-entry prices are determined as follows:
        next_reentry_price_long = pos_price * (1 - entry_grid_spacing_pct * modifier)
        next_reentry_price_short = pos_price * (1 + entry_grid_spacing_pct * modifier)
        where modifier = (1 + ratio * entry_grid_spacing_weight)
        and where ratio = wallet_exposure / wallet_exposure_limithe grid
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

filter_relative_volume_clip_pct = """
    ```
    Coins selected for trading are filtered by volume and noisiness.
    First, filter coins by volume, dropping a percentage of the lowest volume coins.
    Then, sort the eligible coins by noisiness and select the top noisiest coins for trading.

    Volume filter; disapprove the lowest relative volume coins.
    For example, filter_relative_volume_clip_pct = 0.1 drops the 10% lowest volume coins. Set to zero to allow all.
    ```"""

filter_rolling_window = """
    ```
    Coins selected for trading are filtered by volume and noisiness.
    First, filter coins by volume, dropping a percentage of the lowest volume coins.
    Then, sort the eligible coins by noisiness and select the top noisiest coins for trading.

    Number of minutes to look into the past to compute volume and noisiness,
    used for dynamic coin selection in forager mode.
    Noisiness is normalized relative range of 1m OHLCVs: mean((high - low) / close).
    In forager mode, the bot will select coins with highest noisiness for opening positions.
    ```"""

n_positions = """
    ```
    max number of positions to open. Set to zero to disable long/short
    ```"""

total_wallet_exposure_limit = """
    ```
    maximum exposure allowed.
    E.g. total_wallet_exposure_limit = 0.75 means 75% of (unleveraged) wallet balance is used.
    E.g. total_wallet_exposure_limit = 1.6 means 160% of (unleveraged) wallet balance is used.
    Each position is given equal share of total exposure limit, i.e. wallet_exposure_limit = total_wallet_exposure_limit / n_positions.
    See more: docs/risk_management.md
    ```"""

unstuck_close_pct = """
    ```
    percentage of full pos size to close for each unstucking order
    ```"""

unstuck_ema_dist = """
    ```
    distance from EMA band to place unstucking order:
    long_unstuck_close_price = upper_EMA_band * (1 + unstuck_ema_dist)
    short_unstuck_close_price = lower_EMA_band * (1 - unstuck_ema_dist)
    ```"""

unstuck_loss_allowance_pct = """
    ```
    percentage below past peak balance to allow losses.
    e.g. if past peak balance was $10,000 and unstuck_loss_allowance_pct = 0.02,
    the bot will stop taking losses when balance reaches $10,000 * (1 - 0.02) == $9,800
    ```"""

unstuck_threshold = """
    ```
    if a position is bigger than a threshold, consider it stuck and activate unstucking.
    if wallet_exposure / wallet_exposure_limit > unstuck_threshold: unstucking enabled
    e.g. if a position size is $500 and max allowed position size is $1000, then position is 50% full.
    If unstuck_threshold==0.45, then unstuck the position until its size is $450.
    ```"""

minimum_coin_age_days = """
    ```
    disallow coins younger than a given number of days
    ```"""

ohlcv_rolling_window = """
    ```
    number of minutes to look into the past to compute volume and noisiness,
    used for dynamic coin selection in forager mode.
        noisiness is normalized relative range of 1m ohlcvs: mean((high - low) / close)
        in forager mode, bot will select coins with highest noisiness for opening positions
    ```"""

relative_volume_filter_clip_pct = """
    ```
    disapprove the lowest relative volume coins.
    Default 0.1 == 10%. Set to zero to allow all.
    ```"""

time_in_force = """
    ```
    Time in force indicates how long your order will remain active before it is executed or expired.
    GTC (good_till_cancelled: The order will last until it is completed or you cancel it.
    PostOnly (post_only): If the order would be filled immediately when submitted, it will be cancelled.
    ```"""

market_cap = """
    ```
    minimum market capitalization in millions of USD (1 million = 1'000'000)
    ```"""

vol_mcap = """
    ```
    minimum volume to market cap ratio
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

approved_coins = """
    ```
    list of approved coins to trade
    You can not add coins here if they are in ignored_coins.
    Select ALL for add all available coins, but respect filters.
    Select CPT for add only coins that are allowed for CopyTrading, but respect filters.
    ```"""

ignored_coins = """
    ```
    list of coins to ignore
    If you add coins here, they will be removed from approved_coins.
    ```"""

dynamic_ignore = """
    ```
    If enabled, PBRun will dynamically maintain the ignored_coins list.
    The list will created using the market_cap and vol_mcap filters.
    Update interval is configured in PBCoinData.
    On passivbot6 PBRun will restart the bot if needed.
    On passivbot7 PBRun creat the ignored_coins.json file and pb7 will use this list as filter.
    ```"""

empty_means_all_approved = """
    ```
    If true, will interpret approved_coins=[] as all coins approved.
    If false, will interpret approved_coins=[] as no coins approved.
    ```"""

compress_cache = """
    ```
    set to true to save disk space. Set to false to load faster.
    ```"""

vps_swap = """
    ```
    recommended swap size for VPS
    2GB for VPS with 1GB RAM and 10GB SSD
    5GB for VPS with 2GB RAM and 25GB SSD
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

role = """
    ```
    If master:
    - PBRemote will download alive data from the VPS
    If slave:
    - PBRemote will upload alive data to the VPS
    ```"""