# -*- coding: utf-8 -*-
import base64
from datetime import datetime
import os

from src.utils.utils import get_email_messages
from src.utils.utils import execute_command


class CreditCardStatement:
    def __init__(self, gmail_client, config):
        self.config = config
        self.gmail_client = gmail_client

    def get_credit_card_statement(self):
        execute_command('mkdir -p {}'.format(self.config['encrypted_pdfs_folder']))

        file_name = datetime.strftime(datetime.today(), '%Y-%m')
        file_path = os.path.join(self.config['encrypted_pdfs_folder'], file_name)
        self.__get_credit_card_attachment__(file_path)
        print('Open pdfs and print it to remove password.')
        execute_command('open {}'.format(file_path))
        input('Continue? ')

    def __get_credit_card_attachment__(self, file_path):
        email = self.__get_credit_card_email__()

        for part in email['payload']['parts']:
            if part['mimeType'] == 'application/pdf':
                attachment_id = part['body']['attachmentId']
                attachment = self.gmail_client.get_attachment(email['id'], attachment_id)
                file_data = base64.urlsafe_b64decode(attachment['data'].encode('UTF-8'))
                with open(file_path, 'wb') as f:
                    f.write(file_data)

    def __get_credit_card_email__(self):
        query = 'from:{} subject:{} in:{}'.format(self.config['from_citi_email'], self.config['email_subject'],
                                                  self.config['email_inbox_folder'])
        complete_message = get_email_messages(self.gmail_client, query)[0]
        return complete_message











