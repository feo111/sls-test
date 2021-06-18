import json
import random
from datetime import datetime
from random import randint
from threading import Thread
from time import sleep
from uuid import uuid4

import boto3

REQUEST_QUEUE = 'https://sqs.eu-central-1.amazonaws.com/597818332231/test-send-queue'
RESPONSE_QUEUE = 'https://sqs.eu-central-1.amazonaws.com/597818332231/test-receive-queuq'


messages_sent = {}
running = True


def receive_messages():
    session = boto3.Session(profile_name='test')
    sqs = session.resource('sqs')
    ts = datetime.now().timestamp()
    queue = sqs.create_queue(
        QueueName=str(uuid4()),
    )
    print(f'Queue creation time: {datetime.now().timestamp() - ts}')
    global RESPONSE_QUEUE
    RESPONSE_QUEUE = queue.url
    while running:
        messages = queue.receive_messages(
            AttributeNames=['All'],
            MessageAttributeNames=['All'],
            MaxNumberOfMessages=10,
            VisibilityTimeout=1,
            WaitTimeSeconds=20,
        )
        for message in messages:
            correlation_id = message.message_attributes['correlation_id']['StringValue']
            if correlation_id not in messages_sent:
                continue
            message.delete()
            data = json.loads(message.body)
            messages_sent[correlation_id]["responded"] = True
            messages_sent[correlation_id]["receive_timestamp"] = datetime.now().timestamp()
            print(f'received {data["result"]} expected {messages_sent[correlation_id]["value"] * 2}, '
                  f'time={messages_sent[correlation_id]["receive_timestamp"] - messages_sent[correlation_id]["sent_timestamp"]}')
    queue.delete()


def main():
    receiver = Thread(target=receive_messages)
    receiver.start()
    sleep(2)

    session = boto3.Session(profile_name='test')
    sqs = session.resource('sqs')
    queue = sqs.Queue(url=REQUEST_QUEUE)

    while True:
        try:
            value = randint(0, 1000)
            correlation_id = str(uuid4())
            body = {'value': value}

            result = queue.send_message(
                MessageBody=json.dumps(body),
                MessageAttributes={
                    'response_to': {'StringValue': RESPONSE_QUEUE,
                                    'DataType': 'String',
                                    },
                    'correlation_id': {'StringValue': correlation_id,
                                       'DataType': 'String',
                                       },
                }
            )
            messages_sent[correlation_id] = {'message_id': result["MessageId"],
                                             'correlation_id': correlation_id,
                                             'value': value,
                                             'responded': False,
                                             'sent_timestamp': datetime.now().timestamp(),
                                             }
            sleep(random.random() * 5)
        except KeyboardInterrupt:
            global running
            running = False
            receiver.join()
            break
    print(f'Total sent: {len(messages_sent)}')
    print(f'Received responses: {len(list(filter(lambda x: x["responded"], messages_sent.values())))}')
    resp_times = list(map(lambda x: x['receive_timestamp'] - x['sent_timestamp'],
                          filter(lambda x: x["responded"], messages_sent.values())))
    print(f'Average response time: {sum(resp_times) / len(resp_times)}')
    queue.purge()


if __name__ == '__main__':
    main()
