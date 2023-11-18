# GUI for Passivbot

v0.7

## Overview
Passivbot GUI (pbgui) is a WEB Interface for Passivbot programed in python with streamlit

![Alt text](docs/images/live01.png)
![Alt text](docs/images/live02.png)
![Alt text](docs/images/live1.png)
![Alt text](docs/images/live2.png)
![Alt text](docs/images/pbconfigdb.png)
![Alt text](docs/images/backtest.png)
![Alt text](docs/images/backtest2.png)
![Alt text](docs/images/optimize.png)
![Alt text](docs/images/api-editor.png)

## Requirements
- Python 3.8.x and higher
- Streamlit 1.26.0 and higher
- Linux and Winodws (Run Module not supported)
- Live Modul only tested on bybit and bitget

## Installation
```
git clone https://github.com/msei99/pbgui.git
cd pbgui
pip install -r requirements.txt
```
## Running
```
streamlit run pbgui.py &
```
Open http://localhost:8501 with Browser\
Password = PBGui$Bot!\
Change Password in file: .streamlit/secrets.toml\
On First Run, you have to select your passivbot directory

## PBRun Instance Manager
To enable the PBGui instance manager in the GUI, you can follow these steps:

1. Open the PBGui interface.
2. Go to Live and enable PBRun

To ensure that the Instance Manager starts after rebooting your server, you can use the following method:

1. Create a script file, such as "start.sh", in your pbgui directory (e.g., ~/software/pbgui).
2. In the script file, include the following lines:

```
#!/usr/bin/bash
venv=~/software/venv_pb39 # Path to your Python virtual environment
pbgui=~/software/pbgui # Path to your PBGui installation

source ${venv}/bin/activate
cd ${pbgui}
python PBRun.py &
```

3. Save the script file and make it executable by running the command: `chmod 755 start.sh`.
4. Open your crontab file by running the command: `crontab -e`.
5. Add the following line to the crontab file to execute the script at reboot:

```
@reboot ~/software/pbgui/start.sh
```

6. Save the crontab file.

Please make sure to adjust the paths in the script file and crontab entry according to your specific setup.

## PBStat Statistics
Actually, the best way to enable PBStat is by adding the following line to your start.sh script:
```
python PBStat.py &
```
This command will run the PBStat.py script in the background, allowing it to collect statistics.

## v0.7 (18-11-2023)
- Live: Add Backtest Button on Edit Page
- Live: Display Balance, uPnl, Position, Price, Entry, DCA, Next DCA, Next TP, Wallet Exposure
- Live: PBStat fetch trades, balance, positions, price and orders in background
- Live: PBStat added
- Live: Bugfix for select instances

## v0.66 (14-11-2023)
- Live: Bugfix for download trades history from bybit

## v0.65 (13-11-2023)
- Live: -co countdown added
- Live: PBRun is now the Instance Manager vom PBGui
- Run: Module removed / run is now included in Live
- Live: Bugfix for change symbol/market

## v0.61 (05-11-2023)
- Backtest: Bugfix for configs with long config_name from pbconfigdb
- Backtest: Added id to Import
- Backtest: Bugfix for backtest queue / remove backtests
- Backtest: Bugfix for change cpu and autostart

## v0.6 (04-11-2023)
- Live: Upload to pbconfigdb
- Backtest: Import from pbconfigdb
- Backtest: Total rewrite for look and feel like Live Module
- Backtest: Wallet_Exposure for long and short
- Backtest: Enable short/long
- Optimizer: Dynamic User / Symbol and Market_Type
- Optimizer: Quick hack for deleting optimizations
- Optimizer: Reverse Logfile
- Live: Fixed kucoin API-Editor / Live View
- Code: Added Base class for User/Symbol/market
- Code: Save ccxt_symbol to instance for speed up binance live module
- Code: Bugfixes for spot market

## v0.5 (21-10-2023)
- Support Windows (Exclude Run Modul)
- Live: Display ohlcv candlesticks with selectable timeframe and auto refresh
- Live: Show position, open/close orders, price, unrealizedPnL
- Live: Show trading history
- Live: Run backtests and compare trading history with backtest
- Live: Add, Edit, Delete instances
- Live: Dynamic edit config file
- Run: Add Instance to Live
- Code cleanup: Config, Instance class added
- Much more small changes

## v0.4 (02-10-2023)
- Run: Display Logfile
- Run: Add Backtest button
- Run: Display and compare backtests
- Backtest: Queue for run multiple backtests
- Backtest: View and Compare backtests
- Backtest and Optimizer: Load available Symbols from Exchange
- Code cleanup: User, Exchange, Backtest class 

## v0.3 (14-09-2023)
- Setup: API-Editor
- Check connection to exchange and get Wallet Balance

## v0.2 (11-09-2023)
- Run: Interface for manager (start/stop/edit live configs)
- Security: Adding Login credentials

## v0.1 (24-08-2023)
First release with basic backtest and optimization functionality

## Roadmap
- Live: Support Kucoin and OKX
- Live: Start / Stop / Status Instances
- Live: Autostart enabled Instances after reboot
- Live: Watchdog for crashed instances and restart them
- Run: Remove the Run modul. Move all to the Live modul
- Backtest: Configure Default Values
- Code cleanup (Optimizer class)
- Optimizer: add/edit configs/optimize/*.hjson
- Optimizer: Queue for run multiple optimizers
- Full support for Windows
- Remote managment for multiple passivebot servers
- ...

## Links:
- Passivbot https://www.passivbot.com/en/latest/
- Streamlit https://streamlit.io/

## Support:
If you like to support pbgui, please join one of my copytradings on bybit\
ADA, DOGE, RNDR, OP WE 0.5 TWE 2.0 https://i.bybit.com/28bMabOR\
RNDR only https://i.bybit.com/1qabmY01