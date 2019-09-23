LAST_RUN="/home/pi/gyromachine/lastrun"

def load_last_run():
    try:
        with open(LAST_RUN, 'r') as f:
            return int(f.read().strip())
    except:
            return -1

def save_last_run(lr):
    with open(LAST_RUN, 'w') as f:
        f.write(str(lr))
        f.write('\n')

