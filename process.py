import pika
import subprocess
import random
import thread
import time
import argparse
"""
to test :
    1. publish and subscribe case : the influence of the routing key                                 -> routing key is ignored
    2. whether the server knows the connection and the disconnection of a client                     -> fuction get_direct_exchanges
    3. do we need to use direct exchange to reply the request message of a process.
     this means we have to declare 2 exchanges, one for multicast another for direct exchange.
     Or we can just use routing key in multicast to solve the problem.                               -> two types of exchange, fanout and direct
"""

class LogicalTime(object):
    def __init__(self):
        self.logical_time = 0

    def local_event(self):
        self.logical_time += 1

    def remote_event(self, logical_time):
        self.logical_time = max(self.logical_time, logical_time) + 1


class Process :

    def __init__(self,id):
        self.id                   = str(id)
        self.logical_time         = LogicalTime()
        self.request_queue        = {}
        self.reply_messages       = {}
        self.multicast_connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.multicast_channel    = self.set_multicast_channel()
        self.direct_connection    = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.direct_channel       = self.set_direct_channel()
        self.is_working           = False
        self.is_requesting        = False

    """
    create multicast channel
    """
    def set_multicast_channel(self):
        channel = self.multicast_connection.channel()
        channel.exchange_declare(exchange='Lamport',exchange_type='fanout')
        result = channel.queue_declare(exclusive=True)
        queue_name = result.method.queue
        channel.queue_bind(exchange='Lamport',queue=queue_name)
        channel.basic_consume(self.reveive_message_callback,queue=queue_name,no_ack=True)

        return channel


    """
    create direct channel to send and reveive reply messages in direct mode
    """
    def set_direct_channel(self):
        channel = self.direct_connection.channel()
        channel.exchange_declare(exchange=self.id, exchange_type='direct')
        result = channel.queue_declare(exclusive=True)
        queue_name = result.method.queue
        channel.queue_bind(exchange=self.id, queue=queue_name,routing_key = self.id)
        channel.basic_consume(self.reveive_message_callback,queue=queue_name,no_ack=True)

        return channel

    def get_direct_exchanges(self):
        cmd  = "sudo rabbitmqctl list_exchanges"
        passwd = "771589"
        proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
        exchanges = proc.communicate(passwd)
        direct_exchanges = []
        for exchange in exchanges[0].split('\n'):
            try :
                name_exchange = exchange.split('\t')[0]
                type_exchange = exchange.split('\t')[1]
                name_exchange_int = int(name_exchange)
                if type_exchange == 'direct' :
                    direct_exchanges.append(name_exchange)
            except:
                continue

        return direct_exchanges

    def add_to_reply_messages(self,id,logical_time):
        self.reply_messages.update({id: logical_time})

    def add_to_queue(self,id,logical_time):
        self.request_queue.update({id: logical_time})

    def remove_from_queue(self,id):
        del self.request_queue[id]

    def clean_reply_messages(self):
        self.reply_messages = {}

    def reply_request(self,id,logical_time):

        message       = 'ok' + '|' + str(logical_time) + '|' + self.id
        connection    = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        reply_channel = connection.channel()
        reply_channel.exchange_declare(exchange=id, exchange_type='direct')
        reply_channel.basic_publish(exchange    = id,
                                    routing_key = id,
                                    body        = message)
        print("send message : " + message)

    def multicast_request(self,id):
        logical_time = self.logical_time.logical_time
        message = 'request' + '|' + str(logical_time) + '|' + id

        self.multicast_connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.multicast_channel    = self.set_multicast_channel()
        self.multicast_channel.basic_publish(exchange    = 'Lamport',
                                             routing_key = '',
                                             body        = message)
        print("send message : " + message)





    def multicast_release(self,id):
        logical_time = self.logical_time.logical_time
        message = 'release' + '|' + str(logical_time) + '|' + id

        self.multicast_connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.multicast_channel    = self.set_multicast_channel()
        self.multicast_channel.basic_publish(exchange    = 'Lamport',
                                             routing_key = '',
                                             body        = message)
        print("send message : " + message)


    def get_first_of_queue(self):
        keys              = self.request_queue.keys()
        if len(keys) == 0:
            return None

        min_logiacal_time = self.request_queue[keys[0]]
        min_key           = keys[0]
        for key in keys:
            if self.request_queue[key]<min_logiacal_time:
                min_key =key
                min_logiacal_time = self.request_queue[key]
        return min_key

    def check_reply_messages(self):
        direct_exchanges = self.get_direct_exchanges()
        direct_exchanges.remove(self.id)
        direct_exchanges.sort()
        keys = self.reply_messages.keys()
        keys.sort()
        acquire_flag = False

        #choose <= not == because a process may disconnect after sending the reply message
        if set(direct_exchanges).issubset(set(keys)) :
            acquire_flag = True
            for key in self.reply_messages.keys():

                #to ensure the time of the reply message is bigger than request
                if int(self.reply_messages[key]) <= int(self.request_queue[self.id]):
                    acquire_flag = False

        return acquire_flag


    def reveive_message_callback(self,ch, method, properties, message_total):

        message      = message_total.split('|')[0]
        logical_time = int(message_total.split('|')[1])
        id           = message_total.split('|')[2]
        if id == self.id:
            return
        if message not in ["ok","request","release"]:
            return

        print("receive message : " + message_total)
        self.logical_time.remote_event(logical_time)
        if message == 'ok':
            self.add_to_reply_messages(id,logical_time)
            id_first = self.get_first_of_queue()
            if id_first == self.id:
                acquire_flag = self.check_reply_messages()
                if acquire_flag == True:
                    self.clean_reply_messages()
                    self.acquire()

        if message == 'request' :
            self.add_to_queue(id,logical_time)
            while self.is_working :
                time.sleep(0.3)
            self.reply_request(id,self.logical_time.logical_time)


        if message == 'release':
            self.remove_from_queue(id)
            id_first = self.get_first_of_queue()
            if id_first == self.id:
                acquire_flag = self.check_reply_messages()
                if acquire_flag == True:
                    self.clean_reply_messages()
                    self.acquire()

    def start(self):
        thread.start_new_thread( self.multicast_channel.start_consuming, ())
        thread.start_new_thread( self.direct_channel.start_consuming, ())
        thread.start_new_thread( self.request_work, ())

    def request_work(self):
        while True :
            if self.is_requesting :
                time.sleep(0.5)
                continue

            time.sleep(random.randint(5,10))
            self.logical_time.local_event()
            logical_time = self.logical_time.logical_time
            self.add_to_queue(self.id,logical_time)

            direct_exchanges = self.get_direct_exchanges()
            if len(direct_exchanges)==1 and direct_exchanges[0] == self.id:
                self.work()
                continue

            self.is_requesting = True
            self.multicast_request(self.id)

    def acquire(self):
        self.is_working = True
        self.work()


    def release(self):
        self.is_working    = False
        self.is_requesting = False
        self.remove_from_queue(self.id)
        self.logical_time.local_event()
        self.multicast_release(self.id)

    def work(self):
        num_iter = random.randint(1,20)
        max_value = 5000000

        total = 0
        start = time.time()
        for j in range(num_iter):
            for i in range(max_value):
                total = total + i
        end = time.time()
        print( "process %s's execution time is : %f, the result get is : %d" %(self.id, end-start, total) )
        self.release()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('id', type=str, help='set id of a process')
    args = parser.parse_args()
    id   = args.id
    pr = Process(id)
    pr.start()
    while 1:
       pass
