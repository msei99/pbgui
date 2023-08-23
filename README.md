# GUI for Passivbot

v0.1

## Overview
Passivbot GUI (pbgui) is a WEB Interface for Passivbot programed in python with streamlit

![image](https://github.com/msei99/pbgui/assets/70921110/965ce036-2e06-4eaf-9261-ca2d01a076d0)
![image](https://github.com/msei99/pbgui/assets/70921110/29a551b0-480c-4997-b22c-603e380669e3)

## Requirements
- Python 3.8.x and higher
- Streamlit 1.25.0 and higher

## Installation
```
git clone https://github.com/msei99/pbgui.git
cd pbgui
pip install -r requirements.txt
```
## Running
```
streamlit run pbgui.py
```
Open http://localhost:8501 with Browser
On First Run, you have to select your passivbot directory

## v0.1 (24-08-2023)
First release with basic backtest and optimization functionality

## Roadmap
- Optimizer: add/edit configs/optimize/*.hjson
- Run: Interface for manager (start/stop/edit live configs)
- Security: Adding Login credentials
- Run: Add/Edit api-keys
- Backtest: open configs from filesystem
- Backtest: open configs from github
- Backtest: Database for config/results
- Optimizer: Run multiple optimizer sessions
- Optimizer: Database for optimizer results
- Optimizer: Queue for optimizaions
- ...

## Links:
- Passivbot https://www.passivbot.com/en/latest/
- Streamlit https://streamlit.io/
