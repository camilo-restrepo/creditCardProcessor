# -*- coding: utf-8 -*-
import subprocess
import shlex
import sys


def get_email_messages(gmail_client, query):
    print('Gmail query: {}'.format(query))
    message_ids = gmail_client.list_messages_matching_query(query=query)

    all_messages = []
    for message in message_ids:
        complete_message = gmail_client.get_message(message['id'])
        all_messages.append(complete_message)

    return all_messages


def execute_command(command):
    print('Execute command: {}'.format(command))

    args = shlex.split(command)
    p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=False)

    while p.poll() is None:
        output = p.stdout.readline()
        print(output.strip().decode('utf-8'))
        sys.stdout.flush()

    return_code = p.poll()

    if return_code != 0:
        print('Execute command failed. Return Code: {}'.format(return_code))
        raise Exception('Execute command failed. Return Code: {}'.format(return_code))
    return return_code
