import flask
from flask import Flask, request, render_template, jsonify
import os
import re
import json
import pika
import time

app = Flask(__name__)

data = []

#post preprocessing add it to the queue. this data is sent to a different consumer
#for either preprocessing or response to client
def on_message(ch, method,header, body):
    try:        
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()
        channel.queue_declare(queue='get_proc_data')
        channel.basic_publish(exchange='',
                            routing_key='get_proc_data',
                            body=body,
                            properties=pika.BasicProperties(
                                delivery_mode = 2, # make message persistent
                            ))
        connection.close()
    except:
        return "Error in Processing on_message"

    return "Preprocess Data in queue"

#home page containing a form for user data
@app.route('/home')
def home():
    #here we are sending the post request
    return render_template('home.html')


#cron job to run check the queue and run the process
@app.route('/cron3')
def check_queue():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    queue = channel.queue_declare(queue='get_user_data')
    message_count = queue.method.message_count
    print("Message Count %d" % message_count)
    if message_count > 0:
        preprocessing()
    return("Message Count %d" % message_count)

#data preprocess consumer.
def preprocessing():
    print("entering preprocessing")
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='get_user_data')
    channel.basic_consume(on_message,queue="get_user_data",no_ack=False)
    try:
        channel.start_consuming()
    except:
        channel.stop_consuming()
    connection.close()


#create a producer1s
@app.route('/predict',methods=['POST'])
def predict():
    try:
        if request.method == "POST":
            userdata = request.form # add get_json() when sending it via api call
            connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
            channel = connection.channel()
            channel.queue_declare(queue='get_user_data')
            send_data = json.dumps(userdata)
            channel.basic_publish(exchange='',
                        routing_key='get_user_data',
                        body=send_data,
                        properties=pika.BasicProperties(
                            delivery_mode = 2, # make message persistent
                        ))
            connection.close()
            print("[x] Sent json data to queue %r" % send_data)
            

    except Exception as e:
        return e

    return jsonify(userdata)


if __name__ == "__main__":
    app.run(port=5101,debug=True)