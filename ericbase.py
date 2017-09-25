import sys


def displaycounter(message: list, count: list):
    '''provides a pretty counter style display for things like records processed'''
    display = "\r"
    for m in message:
        # print (message.index(m))
        display = display + m + " {" + str(message.index(m)) + ":,d} "
        # print (display)
    print(display.format(*count), end='')


def printerror(errmsg: str):
    '''prints out an ERROR message and exits'''
    print("[ERROR] {}\n".format(errmsg))
    sys.exit(2)


def listtodict(somelist: list, keys: list) -> dict:
    '''converts a list to a dictionary using the keys list as the keys for the dictionary.'''
    klen = len(keys)
    if klen != len(somelist):
        print(somelist, keys)
        printerror("Key [{}] and list [{}] length mismatch".format(len(keys), len(somelist)))
    dout = {}
    for i in range(0, klen):
        dout[keys[i]] = somelist[i]
    return dout





