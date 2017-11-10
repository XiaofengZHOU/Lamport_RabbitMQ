improt pika 
"""
to test :
    1. publish and subscribe case : the influence of the routing key
    2. whether the server knows the connection and the disconnection of a client 
    3. do we need to use direct exchange to reply the request message of a process. this means we have to declare 
       2 exchanges, one for multicast another for direct exchange. Or we can just use routing key in multicast to 
       solve the problem.
"""

class LogicalTime(object):
    def __init__(self):
        self.time = 0

    def local_event(self):
        self.time += 1

    def remote_event(self, time):
        self.time = max(self.time, time) + 1




class Process :
    
    def __init__(self,id):
        self.id             = id
        self.local_time     = LogicalTime()
        self.request_queue  = {}
        self.reply_message  = {}
        self.channel        = set_connection_channel() 

    def set_connection_channel(self):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        channel = connection.channel()
        channel.exchange_declare(exchange='Lamport',exchange_type='fanout')

        result = channel.queue_declare(exclusive=True)
        queue_name = result.method.queue
        channel.queue_bind(exchange='Lamport',queue=queue_name)
        channel.basic_consume(reveive_message_callback,queue=queue_name,no_ack=True)

        return channel

    def add_to_queue(self,time,id):
        self.request_queue.update({id: time})

    def remove_from_queue(self,id):
        del self.request_queue[id]

    def reply_request(self,id):
        message = 'ok'
        channel.basic_publish(exchange    = 'Lamport',
                              routing_key = id,
                              body        = message)

    def multicast_request(self):
        message = 'request'
        channel.basic_publish(exchange    = 'Lamport',
                              body        = message)
        pass

    def multicast_release(self):
        message = 'release'
        channel.basic_publish(exchange    = 'Lamport',
                              body        = message)
        pass

    def reveive_message_callback(self,ch, method, properties, message):
        if message = 'ok':
            pass
        if message = 'request':
            pass
        if message = 'release':
            pass
        

    def acquire(self):
        pass 

    def release(self):
        pass 

    def work(self):
        pass 
    