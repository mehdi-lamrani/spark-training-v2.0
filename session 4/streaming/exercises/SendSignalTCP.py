###############################################################################
# This program generates a Random Walk type continuous signal                 #
# randmonly incrementing or decrementing by 1 at each step                    #
# and sends the values one by one to the specified TCP port (default: 9999)   #
# at the specified frequency per second (default: 1)                          #
#                                                                             #
# Usage :                                                                     #
# python3 SendSignalTCP.py -p PORT -s MSG_PER_SEC                             #
#                                                                             #
# Example :                                                                   #
# python3 SendSignalTCP.py -p 9998 -s 10                                      #
#                                                                             #
###############################################################################

import socket
import random,time
from datetime import datetime

import sys, getopt

def main(argv):

    HOST = '127.0.0.1'  # Standard loopback interface address (localhost)
    PORT = 9999  # Default Port to listen on (non-privileged ports are > 1023)
    MSG_PER_SEC=1

    # SIMPLE COMMAND ARGUMENT MANAGEMENT - SKIP THIS PART #
    
    try:
       opts,args = getopt.getopt(sys.argv[1:], 'p:s:')
    except getopt.GetoptError:
        pass
    for opt,arg in opts:
        if opt == "-p":
            PORT = int(arg)
        elif opt == "-s":
           MSG_PER_SEC = int(arg)

    n = 0
    i = 0

    # TCP SERVER SENDING INCREMENTAL TIME VALUE PAIRS

    with socket.socket(socket.AF_INET,socket.SOCK_STREAM) as s:
        s.bind((HOST,PORT))
        s.listen()
        conn,addr = s.accept()
        while True:
            n = n + 1
            i = i + random.choice([-1,1])
            ms = datetime.utcnow().strftime('%H:%M:%S.%f')[:-3]
            print(ms)
            print(i)
            conn.sendall(bytes(str(i)+"\n",'utf-8'))
            time.sleep(1 / MSG_PER_SEC)
            if n == 1000000:
                break


if __name__ == "__main__":
    main(sys.argv)