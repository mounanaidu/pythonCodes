import time
import stomp
from datetime import datetime
from time import sleep
import json
#import schedule
import logging

logging.basicConfig(filename="comsumer.log", format='%(asctime)s %(message)s',
                    filemode='w')
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


class MsgListener(stomp.ConnectionListener):
    def __init__(self):
        self.msg_received = 0

    def on_error(self, headers, message):
        print('received an error "%s"' % message)

    def on_message(self, headers, message):
        logger.info("inside on message ", type(message), message)
        message = json.loads(message)
        self.msg_received += 1
        msg = "{} messages received ".format(self.msg_received)
        print(msg)

        # add the logic to process based on message here

        sleep(0.1)


conn = stomp.StompConnection10([('11.24.32.734', 6339), ])
listener = MsgListener()
conn.set_listener('', listener)
conn.connect('user', 'pass', wait=True)
conn.subscribe('/topic/path', headers={})
start = datetime.now()
msg = "{:%d, %b %Y-%Hhrs:%Mmins:%Ssecs} start consumer".format(start)
print(msg)


# def check_messages():
#     print("inside check_message")
# 
#     time.sleep(1)
#     print("disconnecting")
#     conn.disconnect()
# 
# 
# schedule.every(20).minutes.do(check_messages)
# while 1:
#     if not conn.is_connected():
#         conn = stomp.StompConnection10([('11.24.32.734', 6339), ])
#         listener = MsgListener()
#         conn.set_listener('', listener)
#         conn.connect('user', 'pass', wait=True)
#         conn.subscribe('/topic/path', headers={})
#         start = datetime.now()
#         msg = "{:%d, %b %Y-%Hhrs:%Mmins:%Ssecs} start consumer".format(start)
#         print(msg)
# 
#     else:
#         print("conn is connected")
# 
#     schedule.run_pending()
#     time.sleep(10)