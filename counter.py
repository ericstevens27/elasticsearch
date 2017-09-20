import time
import ericbase


# def displaycounter(message: list, count: list):
#     display = "\r"
#     for m in message:
#         # print (message.index(m))
#         display = display + m + " {" + str(message.index(m)) + ":,d} "
#         # print (display)
#     print(display.format(*count), end='')


for i in range(3, 0, -1):
    ericbase.displaycounter(["Countdown:", "Second:"], [i, i+3])
    time.sleep(1)

print("\rBOOM!")
