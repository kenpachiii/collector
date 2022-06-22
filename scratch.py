import glob

def archive_data():

    files = glob.glob('./data/okx/trades/BTC-USD-SWAP/*.csv')
    files.sort()

    if len(files) < 4:
        raise Exception

    file = files.pop(0)
    print(file)

archive_data()