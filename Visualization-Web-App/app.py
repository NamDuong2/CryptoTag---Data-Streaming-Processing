import threading
from flask import Flask, render_template, session, copy_current_request_context,request
from flask_socketio import SocketIO, emit, disconnect
from threading import Lock
from kafka import KafkaConsumer
from time import time
import json

async_mode = None
app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret!'
socket_ = SocketIO(app, async_mode=async_mode)
thread = None
thread_lock = Lock()
BOOTSTRAP_SERVERS = '192.168.0.106:9092'
TOPIC_NAME = 'visualize-tweet'
consumer = KafkaConsumer('visualize-tweet',
                         group_id='my-group',
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                         bootstrap_servers=[BOOTSTRAP_SERVERS])
def update():
    for msg in consumer:
        result = msg.value
        list = [time()*1000, result.get('#NFT'), result.get('#BTC'), result.get('#LUNA'), result.get('#ETH'), result.get('#BNB'), result.get('#SOL')]
        socket_.emit('my_response',
            {'data': list},
            namespace='/test')
    """
    while True:
        list = [time.time(), random.randint(0,10), random.randint(10,20), random.randint(20,30)]
        time.sleep(3)
        socket_.emit('my_response',
            {'data': list},
            namespace='/test')
        print("emitted")
    """
t=threading.Thread(target=update)


@socket_.on('connect', namespace='/test')
def handle_connect():
    print('Client connected')
   
    if not t.is_alive():
        t.start()

@app.route('/')
def index():
    
    return render_template('index.html', async_mode=socket_.async_mode)


socket_.run(app,port=8070)