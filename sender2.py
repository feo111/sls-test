import multiprocessing as mp
import json
from datetime import datetime
from random import randint
from uuid import uuid4

import boto3

REQUEST_QUEUE = 'https://sqs.eu-central-1.amazonaws.com/597818332231/test-send-queue'


def main(q):
    session = boto3.Session(profile_name='test')
    sqs = session.resource('sqs')

    processing_times = []
    running = True
    deleted = True
    for _ in range(1000):
        try:
            ts = datetime.now().timestamp()
            queue = sqs.Queue(url=REQUEST_QUEUE)
            value = randint(0, 1000)
            correlation_id = str(uuid4())
            body = {'value': value}
            resp_queue = sqs.create_queue(
                QueueName=str(uuid4()),
            )
            deleted = False

            result = queue.send_message(
                MessageBody=json.dumps(body),
                MessageAttributes={
                    'response_to': {'StringValue': resp_queue.url,
                                    'DataType': 'String',
                                    },
                    'correlation_id': {'StringValue': correlation_id,
                                       'DataType': 'String',
                                       },
                }
            )
            received = False
            while not received:
                messages = resp_queue.receive_messages(
                    AttributeNames=['All'],
                    MessageAttributeNames=['All'],
                    MaxNumberOfMessages=1,
                    VisibilityTimeout=10,
                    WaitTimeSeconds=20,
                )
                for message in messages:
                    message.delete()
                    correlation_id_received = message.message_attributes['correlation_id']['StringValue']
                    if correlation_id_received == correlation_id:
                        received = True
            resp_queue.delete()
            deleted = True

            pr_time = datetime.now().timestamp() - ts
            print(f'Processing time: {pr_time}')
            processing_times.append(pr_time)
            q.put(pr_time)
        except KeyboardInterrupt:
            running = False
            if not deleted:
                resp_queue.delete()

def run():
    ctx = mp.get_context('spawn')
    q = ctx.Queue()
    processes = []
    for _ in range(10):
        p = ctx.Process(target=main, args=(q,))
        p.start()
        processes.append(p)
    for p in processes:
        p.join()
    results = []
    while not q.empty():
        results.append(q.get(timeout=0))
    print(f'Total messages: {len(results)}')
    print(f'Avg time: {sum(results) / len(results)}')
    print(f'Min time: {min(results)}')
    print(f'Max time: {max(results)}')



if __name__ == '__main__':
    run()
