import argparse
from PBRun import PBRun
from PBRemote import PBRemote
from PBCoinData import CoinData

def main():
    parser = argparse.ArgumentParser(description='starter')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-s', '--start', action='store_true', help='Start')
    group.add_argument('-k', '--stop', action='store_true', help='Stop')
    group.add_argument('-r', '--restart', action='store_true', help='Restart')
    parser.add_argument('command', choices=['PBRun', 'PBRemote', 'PBCoinData'], nargs='+')

    args = parser.parse_args()

    if args.start and 'PBRun' in args.command:
        print("Start PBRun")
        PBRun().run()
    if args.start and 'PBRemote' in args.command:
        print("Start PBRemote")
        PBRemote().run()
    if args.start and 'PBCoinData' in args.command:
        print("Start PBCoinData")
        CoinData().run()
    if args.stop and 'PBRun' in args.command:
        print("Stop PBRun")
        PBRun().stop()
    if args.stop and 'PBRemote' in args.command:
        print("Stop PBRemote")
        PBRemote().stop()
    if args.stop and 'PBCoinData' in args.command:
        print("Stop PBCoinData")
        CoinData().stop()
    if args.restart and 'PBRun' in args.command:
        print("Restart PBRun")
        PBRun().stop()
        PBRun().run()
    if args.restart and 'PBRemote' in args.command:
        print("Restart PBRemote")
        PBRemote().stop()
        PBRemote().run()
    if args.restart and 'PBCoinData' in args.command:
        print("Restart PBCoinData")
        CoinData().stop()
        CoinData().run()

if __name__ == '__main__':
    main()
