# -*- coding: utf-8 -*-
import base64
import os
import re
import shlex
import shutil
import subprocess
import sys
import time
from datetime import datetime, timedelta

import httplib2
import pandas as pd
from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from apiclient import discovery, errors
from bs4 import BeautifulSoup
from oauth2client import client
from oauth2client.file import Storage
from airflow.hooks.mysql_hook import MySqlHook


class Gmail(object):

    def __init__(self, config):
        self.config = config
        credentials = self.__get_credentials__()
        http = credentials.authorize(httplib2.Http())
        self.service = discovery.build('gmail', 'v1', http=http)

    def __get_credentials__(self):
        if not os.path.exists(self.config['credentials_dir']):
            os.makedirs(self.config['credentials_dir'])
        credential_path = os.path.join(self.config['credentials_dir'], self.config['app_name'] + '.json')

        flow = client.flow_from_clientsecrets(self.config['secret_file'], self.config['scope'],
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

    def __list_messages_matching_query__(self, user_id='me', label_ids=[], query=''):
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

    def __get_message__(self, email_id, user_id='me', result_format=None):
        if result_format is None:
            message = self.service.users().messages().get(userId=user_id, id=email_id).execute()
        else:
            message = self.service.users().messages().get(userId=user_id, id=email_id, format=result_format).execute()
        return message

    def get_attachment(self, email_id, attachment_id, user_id='me'):
        attachment = self.service.users().messages().attachments().get(userId=user_id, messageId=email_id,
                                                                       id=attachment_id).execute()
        return attachment

    def get_email_messages(self, query):
        print('Gmail query: {}'.format(query))
        message_ids = self.__list_messages_matching_query__(query=query)

        all_messages = []
        for message in message_ids:
            complete_message = self.__get_message__(message['id'])
            all_messages.append(complete_message)

        print(len(all_messages))
        return all_messages


config = Variable.get('credit_card_processor_config', deserialize_json=True)
month = datetime.today().strftime('%Y-%m')
gmail_client = Gmail(config['gmail_config'])
data_dir = os.path.join(config['root_dir'], month)
images_dir = os.path.join(data_dir, 'vouchers')
tmp_dir = os.path.join(data_dir, 'tmp')


def create_dirs():
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)

    os.makedirs(data_dir)
    os.makedirs(images_dir)
    os.makedirs(tmp_dir)


def download_statement():
    emails_config = config['statement_email_config']
    query = 'from:{} subject:{} in:{}'.format(emails_config['from'], emails_config['subject'], emails_config['folder'])
    statement_email = gmail_client.get_email_messages(query)[0]
    for part in statement_email['payload']['parts']:
        if part['mimeType'] == 'application/pdf':
            attachment_id = part['body']['attachmentId']
            attachment = gmail_client.get_attachment(statement_email['id'], attachment_id)
            file_data = base64.urlsafe_b64decode(attachment['data'].encode('UTF-8'))

            pdf_file = os.path.join(tmp_dir, emails_config['statement_pdf'])
            f = open(pdf_file, 'wb')
            f.write(file_data)
            f.close()


def wait_for_statement():
    pdf_file = os.path.join(data_dir, config['statement_email_config']['statement_pdf'])
    while not os.path.exists(pdf_file):
        time.sleep(30)


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


def open_tabula():
    execute_command('open {}'.format(config['tabula']['path']))


def wait_for_csv():
    csv_file = os.path.join(tmp_dir, config['csv_filename'])
    while not os.path.exists(csv_file):
        time.sleep(30)


def get_min_max_dates(transactions):
    min_date = min(transactions['date']).strftime('%Y/%m/%d')
    max_date = (max(transactions['date']) + timedelta(days=1)).strftime('%Y/%m/%d')
    return min_date, max_date


def get_transactions(**kwargs):
    csv_file = os.path.join(tmp_dir, config['csv_filename'])
    bank_transactions = pd.read_csv(csv_file, header=None)
    bank_transactions.columns = ['date', 'ref', 'name', 'rate', 'payments', 'value']
    bank_transactions = bank_transactions[bank_transactions.value.str.contains('-') == False]
    bank_transactions.drop(columns=['ref', 'rate', 'payments'], inplace=True)
    bank_transactions['date'] = pd.to_datetime(bank_transactions['date'], format='%d/%m/%y')
    bank_transactions['value'] = bank_transactions['value'].replace('[\$,]', '', regex=True).astype(float)
    min_date, max_date = get_min_max_dates(bank_transactions)

    kwargs['task_instance'].xcom_push(key='min_date', value=min_date)
    kwargs['task_instance'].xcom_push(key='max_date', value=max_date)
    kwargs['task_instance'].xcom_push(key='transactions', value=bank_transactions)


def get_email_messages(email_config, ini_date, end_date):
    query_params = []
    if 'from' in email_config:
        query_params.append('from:{}'.format(email_config['from']))

    if 'subject' in email_config:
        query_params.append('subject:{}'.format(email_config['subject']))

    if 'folder' in email_config:
        query_params.append('in:{}'.format(email_config['folder']))

    query_params.append('after:{} before:{}'.format(ini_date, end_date))
    query = ' '.join(query_params)
    all_messages = gmail_client.get_email_messages(query)

    return all_messages


def get_bank_email_body(email):
    payld = email['payload']
    mssg_parts = payld['parts']
    part_one = mssg_parts[0]
    part_body = part_one['parts'][0]['body']
    part_data = part_body['data']
    clean_one = part_data.replace("-", "+")
    clean_one = clean_one.replace("_", "/")
    clean_two = base64.b64decode(bytes(clean_one, 'UTF-8'))
    soup = BeautifulSoup(clean_two, 'lxml')
    mssg_body = soup.text
    return mssg_body


def find_in_text(regex, text, ignore_case=False):
    flags = 0
    if ignore_case:
        flags = re.IGNORECASE
    m = re.search(regex, text, flags=flags)
    if m:
        found = m.group(0)
        return found
    return None


def get_bank_data(**kwargs):
    ini_date = kwargs['task_instance'].xcom_pull(key='min_date', task_ids='get_transactions')
    end_date = kwargs['task_instance'].xcom_pull(key='max_date', task_ids='get_transactions')
    emails = get_email_messages(config['bank_email_config'], ini_date, end_date)

    data = []
    for email in emails:
        body = get_bank_email_body(email)
        body = body.replace('\r', '')
        sentences = body.split('\n')
        for sentence in sentences:
            if 'Visa Gold Aadvantage' in sentence:
                text = sentence.strip()[:-4]
                date = datetime.strptime(find_in_text(r'\d\d/\d\d/\d\d', text), '%d/%m/%y')
                value = float(find_in_text(r'\d+\.\d+', text))
                name = text.split(' en ')[-1][:-1]
                data.append({
                    'date': date,
                    'value': value,
                    'name': name
                })

    kwargs['task_instance'].xcom_push(key='bank_data', value=data)


def get_uber_messages(min_date, max_date):
    # uber_config = config['uber_email_config']
    # query = 'from:{} in:{} after:{} before:{}'
    # query = query.format(uber_config['from'], uber_config['folder'], min_date, max_date)
    # all_messages = gmail_client.get_email_messages(query)
    all_messages = get_email_messages(config['uber_email_config'], min_date, max_date)

    return all_messages


def get_uber_data(**kwargs):
    min_date = kwargs['task_instance'].xcom_pull(key='min_date', task_ids='get_transactions')
    max_date = kwargs['task_instance'].xcom_pull(key='max_date', task_ids='get_transactions')
    emails = get_uber_messages(min_date, max_date)
    patterns = config['uber_email_config']['patterns']
    data = []
    for email in emails:
        value_str = find_in_text(r'\$\d+', email['snippet'])
        value = float(value_str[1:])
        regex = r'\b(mon|tues|wed|thur|fri|sat|sun)\b,? (jan|feb|mar|apr|may|jun|jul|sept|oct|nov|dec) \d{2}, \d{4}'
        date_str = find_in_text(regex, email['snippet'], ignore_case=True)

        date = None
        for p in patterns:
            try:
                date = datetime.strptime(date_str, p)
            except:
                continue

        print('----------------------------- UBER SNIPPET -----------------------------')
        print(email['snippet'])
        print('Value: {}'.format(value))
        print('Date: {}'.format(date))
        print('---------------------------------------------------------------------------')

        if date:
            data.append({
                'date': date,
                'value': value
            })
        else:
            raise ValueError('Date not parsed correctly.')

    kwargs['task_instance'].xcom_push(key='uber_data', value=data)


def bank_exists(row, messages):
    for m in messages:
        name_value = m['name'] in row['name'] and m['value'] == row['value']
        date_name = m['name'] in row['name'] and m['date'].strftime('%Y-%m-%d') == row['date'].strftime('%Y-%m-%d')
        if name_value or date_name:
            return True
    return False


def uber_exists(row, messages):
    if 'uber' in row['name'].lower():
        for m in messages:
            name_value = m['value'] == row['value']
            date_name = m['date'].strftime('%Y-%m-%d') == row['date'].strftime('%Y-%m-%d')
            if name_value or date_name:
                return True
        return False
    return ''


def consolidate_data(**kwargs):
    transactions = kwargs['task_instance'].xcom_pull(key='transactions', task_ids='get_transactions')
    bank_data = kwargs['task_instance'].xcom_pull(key='bank_data', task_ids='get_bank_data')
    uber_data = kwargs['task_instance'].xcom_pull(key='uber_data', task_ids='get_uber_data')

    transactions['bank_email'] = transactions.apply(lambda row: bank_exists(row, bank_data), axis=1)
    transactions['uber_email'] = transactions.apply(lambda row: uber_exists(row, uber_data), axis=1)

    kwargs['task_instance'].xcom_push(key='result', value=transactions)


def save_transactions(**kwargs):
    transactions = kwargs['task_instance'].xcom_pull(key='transactions', task_ids='get_transactions')
    transactions['dt'] = month
    transactions['value'] = transactions['value'].astype(float)
    transactions['date'] = transactions['date'].dt.strftime('%Y-%m-%d')

    mysql = MySqlHook(mysql_conn_id='credit_card_processor')
    mysql_conn = mysql.get_conn()

    cursor = mysql_conn.cursor()
    cursor.execute(config['mysql']['create_transaction_table'])

    wildcards = ','.join(['%s'] * len(transactions.columns))
    colnames = ','.join(transactions.columns)

    insert_sql = config['mysql']['create_transaction'] % (config['mysql']['transaction_table'], colnames, wildcards)
    data = [tuple([v for v in rw]) for rw in transactions.values]
    cursor.executemany(insert_sql, data)

    mysql_conn.commit()
    cursor.close()


def save_result_files(**kwargs):
    results = kwargs['task_instance'].xcom_pull(key='result', task_ids='consolidate_data')
    results['date'] = results['date'].dt.strftime("%d/%m/%Y")

    excel_file = os.path.join(data_dir, config['result_config']['excel_file'])
    writer = pd.ExcelWriter(excel_file)
    results.to_excel(writer, 'Sheet1')
    writer.save()


def last_day_of_month(any_day):
    next_month = any_day.replace(day=28) + timedelta(days=4)
    return next_month - timedelta(days=next_month.day)


def get_debit_card_data(**kwargs):
    ini_date = datetime.today().replace(day=1).date()
    end_date = last_day_of_month(ini_date)
    emails = get_email_messages(config['debit_card_email_config'], ini_date, end_date)

    data = []
    for email in emails:
        body = get_bank_email_body(email)
        body = body.replace('\r', '')
        sentences = body.split('\n')

        date = None
        value = None
        for sentence in sentences:
            s = sentence.strip()
            if s.startswith('Fecha'):
                date = datetime.strptime(find_in_text(r'\d+/\d\d/\d\d\d\d', s), '%m/%d/%Y')
            elif s.startswith('Valor'):
                value = float(find_in_text(r'\d+\,\d+.\d+', s).replace(',', ''))

        if date and value:
            data.append({
                'date': date,
                'name': '',
                'value': value,
                'dt': month
            })

    df = pd.DataFrame(data)
    kwargs['task_instance'].xcom_push(key='debit_card_data', value=df)


def save_debit_card_data(**kwargs):
    transactions = kwargs['task_instance'].xcom_pull(key='debit_card_data', task_ids='get_debit_card_data')
    transactions['value'] = transactions['value'].astype(float)
    transactions['date'] = transactions['date'].dt.strftime('%Y-%m-%d')

    mysql = MySqlHook(mysql_conn_id='credit_card_processor')
    mysql_conn = mysql.get_conn()

    cursor = mysql_conn.cursor()
    cursor.execute(config['mysql']['create_transaction_table'])

    wildcards = ','.join(['%s'] * len(transactions.columns))
    colnames = ','.join(transactions.columns)

    insert_sql = config['mysql']['create_transaction'] % (config['mysql']['transaction_table'], colnames, wildcards)
    data = [tuple([v for v in rw]) for rw in transactions.values]
    cursor.executemany(insert_sql, data)

    mysql_conn.commit()
    cursor.close()


def get_vouchers_data(**kwargs):
    pass


def clean():
    if os.path.exists(tmp_dir):
        shutil.rmtree(tmp_dir)


args = {
    'owner': 'yotas',
    'depends_on_past': False,
    'start_date': datetime.utcnow(),
    'retries': 0,
}

dag = DAG(dag_id='credit_card_processor', default_args=args, schedule_interval=None, dagrun_timeout=timedelta(minutes=15))


create_dirs_task = PythonOperator(task_id='create_dirs', python_callable=create_dirs, dag=dag)

download_statement_task = PythonOperator(task_id='download_statement', python_callable=download_statement, dag=dag)
download_statement_task.set_upstream(create_dirs_task)

wait_for_statement_task = PythonOperator(task_id='wait_for_statement', python_callable=wait_for_statement, dag=dag)
wait_for_statement_task.set_upstream(download_statement_task)

open_tabula_task = PythonOperator(task_id='open_tabula', python_callable=open_tabula, dag=dag)
open_tabula_task.set_upstream(wait_for_statement_task)

wait_for_csv_task = PythonOperator(task_id='wait_for_csv', python_callable=wait_for_csv, dag=dag)
wait_for_csv_task.set_upstream(open_tabula_task)

get_transactions_task = PythonOperator(task_id='get_transactions', python_callable=get_transactions, dag=dag,
                                       provide_context=True)
get_transactions_task.set_upstream(wait_for_csv_task)

get_bank_data_task = PythonOperator(task_id='get_bank_data', python_callable=get_bank_data, dag=dag,
                                    provide_context=True)
get_bank_data_task.set_upstream(get_transactions_task)

get_uber_data_task = PythonOperator(task_id='get_uber_data', python_callable=get_uber_data, dag=dag,
                                    provide_context=True)
get_uber_data_task.set_upstream(get_bank_data_task)

consolidate_data_task = PythonOperator(task_id='consolidate_data', python_callable=consolidate_data, dag=dag,
                                       provide_context=True)
consolidate_data_task.set_upstream(get_uber_data_task)

save_transactions_task = PythonOperator(task_id='save_transactions', python_callable=save_transactions, dag=dag,
                                        provide_context=True)
save_transactions_task.set_upstream(consolidate_data_task)

save_result_files_task = PythonOperator(task_id='save_result_files', python_callable=save_result_files, dag=dag,
                                        provide_context=True)
save_result_files_task.set_upstream(consolidate_data_task)

clean_task = PythonOperator(task_id='clean', python_callable=clean, dag=dag)
clean_task.set_upstream(save_transactions_task)
clean_task.set_upstream(save_result_files_task)

get_debit_card_data_task = PythonOperator(task_id='get_debit_card_data', python_callable=get_debit_card_data, dag=dag,
                                          provide_context=True)
get_debit_card_data_task.set_upstream(create_dirs_task)

save_debit_card_data_task = PythonOperator(task_id='save_debit_card_data', python_callable=save_debit_card_data,
                                           dag=dag, provide_context=True)
save_debit_card_data_task.set_upstream(get_debit_card_data_task)


get_vouchers_data_task = PythonOperator(task_id='get_vouchers_data', python_callable=get_vouchers_data, dag=dag,
                                        provide_context=True)
get_vouchers_data_task.set_upstream(get_transactions_task)

consolidate_data_task.set_upstream(get_vouchers_data_task)


# TODO: Add document scanner.
# TODO: Refactor para usar get_email_messages en vez de llamar siempre el cliente


