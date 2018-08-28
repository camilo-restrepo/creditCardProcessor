import os
from apiclient import discovery, errors
from oauth2client import client
from oauth2client.file import Storage
import httplib2


class Gmail(object):

    def __init__(self, config):
        credentials = self.get_credentials(config)
        http = credentials.authorize(httplib2.Http())
        self.service = discovery.build('gmail', 'v1', http=http)

    def get_credentials(self, config):
        if not os.path.exists(config['credentials_dir']):
            os.makedirs(config['credentials_dir'])
        credential_path = os.path.join(config['credentials_dir'], config['app_name'] + '.json')

        flow = client.flow_from_clientsecrets(config['secret_file'], config['scope'],
                                              redirect_uri='urn:ietf:wg:oauth:2.0:oob')
        store = Storage(credential_path)
        credentials = store.get()
        if not credentials or credentials.invalid:
            auth_uri = flow.step1_get_authorize_url()
            print('auth uri: {}'.format(auth_uri))

            auth_code = input('Enter the auth code: ')
            credentials = flow.step2_exchange(auth_code)
            print('Storing credentials to ' + credential_path)
            store.put(credentials)

        return credentials

    def list_messages_matching_query(self, user_id='me', label_ids=[], query=''):
        try:
            response = self.service.users().messages().list(userId=user_id, labelIds=label_ids, q=query).execute()
            messages = []
            if 'messages' in response:
                messages.extend(response['messages'])

            while 'nextPageToken' in response:
                page_token = response['nextPageToken']
                response = self.service.users().messages().list(userId=user_id, labelIds=label_ids, q=query,
                                                                pageToken=page_token).execute()
                if response['nextPageToken'] != page_token:
                    messages.extend(response['messages'])
                else:
                    break

            return messages
        except errors.HttpError as error:
            print('An error occurred: %s' % error)

    def get_labels(self, user_id='me'):
        results = self.service.users().labels().list(userId=user_id).execute()
        labels = results.get('labels', [])

        if not labels:
            return None
        else:
            results = []
            for label in labels:
                results.append((label['name'], label['id']))
            return results

    def get_message(self, email_id, user_id='me', result_format=None):
        if result_format is None:
            message = self.service.users().messages().get(userId=user_id, id=email_id).execute()
        else:
            message = self.service.users().messages().get(userId=user_id, id=email_id, format=result_format).execute()
        return message

    def get_attachment(self, email_id, attachment_id, user_id='me'):
        attachment = self.service.users().messages().attachments().get(userId=user_id, messageId=email_id,
                                                                       id=attachment_id).execute()
        return attachment
