import json
import hjson
import pprint
import configparser
from pathlib import Path

def save_ini(section : str, parameter : str, value : str):
    pb_config = configparser.ConfigParser()
    pb_config.read('pbgui.ini')
    if not pb_config.has_section(section):
        pb_config.add_section(section)
    pb_config.set(section, parameter, value)
    with open('pbgui.ini', 'w') as pbgui_configfile:
        pb_config.write(pbgui_configfile)

def load_ini(section : str, parameter : str):
    pb_config = configparser.ConfigParser()
    pb_config.read('pbgui.ini')
    if pb_config.has_option(section, parameter):
        return pb_config.get(section, parameter)
    else:
        return ""

def pbdir(): return load_ini("main", "pbdir")

def pbvenv(): return load_ini("main", "pbvenv")

def is_pb_installed():
    if Path(f"{pbdir()}/passivbot.py").exists():
        return True
    return False

def pb7dir(): return load_ini("main", "pb7dir")

def pb7venv(): return load_ini("main", "pb7venv")

def is_pb7_installed():
    if Path(f"{pb7dir()}/src/passivbot.py").exists():
        return True
    return False

PBGDIR = Path.cwd()

def validateJSON(jsonData):
    try:
        json.loads(jsonData)
    except (ValueError,TypeError) as err:
        return False
    return True

def validateHJSON(hjsonData):
    try:
        hjson.loads(hjsonData)
    except (ValueError) as err:
        return False
    return True

def config_pretty_str(config: dict):
    try:
        return json.dumps(config, indent=4)
    except TypeError:
        pretty_str = pprint.pformat(config)
        for r in [("'", '"'), ("True", "true"), ("False", "false"), ("None", "null")]:
            pretty_str = pretty_str.replace(*r)
        return pretty_str

def load_symbols_from_ini(exchange: str, market_type: str):
    pb_config = configparser.ConfigParser()
    pb_config.read('pbgui.ini')
    if pb_config.has_option("exchanges", f'{exchange}.{market_type}'):
        return eval(pb_config.get("exchanges", f'{exchange}.{market_type}'))
    else:
        return []
