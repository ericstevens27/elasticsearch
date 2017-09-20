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


