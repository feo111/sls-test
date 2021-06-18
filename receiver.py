import json

import boto3

REQUEST_QUEUE = 'https://sqs.eu-central-1.amazonaws.com/597818332231/test-send-queue'


def main():
    session = boto3.Session(profile_name='test')
    sqs = session.resource('sqs')
    queue = sqs.Queue(url=REQUEST_QUEUE)
    while True:
        try:
            messages = queue.receive_messages(
                AttributeNames=['All'],
                MessageAttributeNames=['All'],
                MaxNumberOfMessages=10,
                VisibilityTimeout=5,
                WaitTimeSeconds=20,
            )
            for message in messages:
                message.delete()
                body = json.loads(message.body)
                result = body['value'] * 2
                data = {'result': result}
                correlation_id = message.message_attributes['correlation_id']['StringValue']
                response_to = message.message_attributes['response_to']['StringValue']
                response_queue = sqs.Queue(url=response_to)

                message = response_queue.send_message(
                    MessageBody=json.dumps(data),
                    MessageAttributes={
                        'correlation_id': {'StringValue': correlation_id,
                                           'DataType': 'String',
                                           },
                    }
                )

        except KeyboardInterrupt:
            break


if __name__ == '__main__':
    main()