# GUI for Passivbot

v0.4

## Overview
Passivbot GUI (pbgui) is a WEB Interface for Passivbot programed in python with streamlit

![Alt text](docs/images/run.png)
![Alt text](docs/images/run2.png)
![Alt text](docs/images/backtest.png)
![Alt text](docs/images/backtest2.png)
![Alt text](docs/images/optimize.png)
![Alt text](docs/images/api-editor.png)

## Requirements
- Python 3.8.x and higher
- Streamlit 1.26.0 and higher
- Linux (Windows not suppoted at the moment)

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

## v0.4 02-10-2023)
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
- Backtest: Wallet_Exposure for long and short
- Backtest: Enable short/long
- Backtest: Configure Default Values
- Code cleanup (Config, Optimizer class)
- Optimizer: add/edit configs/optimize/*.hjson
- Optimizer: Queue for run multiple optimizers
- Backtest: open configs from filesystem
- Backtest: open configs from github
- Support for Windows
- Run: Display Orders
- Run: Display Positions / Open Orders and Chart
- Run: History PNL, Balance and more
- Run: Compare live results with backtest
- ...

## Links:
- Passivbot https://www.passivbot.com/en/latest/
- Streamlit https://streamlit.io/
