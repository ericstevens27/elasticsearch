import sys


def displaycounter(message: list, count: list):
    display = "\r"
    for m in message:
        # print (message.index(m))
        display = display + m + " {" + str(message.index(m)) + ":,d} "
        # print (display)
    print(display.format(*count), end='')


def printerror(errmsg: str):
    print("[ERROR] {}\n".format(errmsg))
    sys.exit(2)

def listtodict(somelist: list, keys: list) -> dict:
    klen = len(keys)
    if klen != len(somelist):
        print(somelist, keys)
        printerror("Key [{}] and list [{}] length mismatch".format(len(keys), len(somelist)))
    dout = {}
    for i in range(0, klen):
        dout[keys[i]] = somelist[i]
    return dout





