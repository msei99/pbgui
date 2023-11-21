from PBRun import PBRun
from time import sleep

def main():
    run = PBRun()
    while True:
        try:
            run.alive()
            run.has_remote()
#            sleep(5)
        except Exception as e:
            print(f'Something went wrong, but continue {e}')
            sleep(5)

if __name__ == '__main__':
    main()