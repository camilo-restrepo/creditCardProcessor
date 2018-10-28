from gmail import Gmail
import json
import sys
from datetime import datetime, timedelta
import pickle as pk
import base64
from PyPDF2 import PdfFileWriter, PdfFileReader
import getpass
import shlex
import subprocess
import pandas as pd
import os
import email
from bs4 import BeautifulSoup
import re
import calendar
import pytesseract
from PIL import Image
from os import listdir
from os.path import isfile, join


def find_in_text(regex, text):
    m = re.search(regex, text)
    if m:
        found = m.group(0)
        return found
    return None


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


def get_email_messages(gmail_client, query):
    print('Gmail query: {}'.format(query))
    message_ids = gmail_client.list_messages_matching_query(query=query)

    all_messages = []
    for message in message_ids:
        complete_message = gmail_client.get_message(message['id'])
        all_messages.append(complete_message)

    return all_messages


def get_credit_card_email(gmail_client, config):
    checkpoint_file = config['cc_email_checkpoint']

    if not isfile(checkpoint_file):
        query = 'from:{} subject:{} in:{}'.format(config['from_citi_email'], config['email_subject'], config['email_folder'])
        complete_message = get_email_messages(gmail_client, query)[0]

        pk.dump(complete_message, open(checkpoint_file, 'wb'))
    else:
        complete_message = pk.load(open(checkpoint_file, 'rb'))

    return complete_message


def get_credit_card_attachment(gmail_client, config):

    if not isfile(config['pdf_filename']):
        email = get_credit_card_email(gmail_client, config)

        for part in email['payload']['parts']:
            if part['mimeType'] == 'application/pdf':
                attachment_id = part['body']['attachmentId']
                attachment = gmail_client.get_attachment(email['id'], attachment_id)

                file_data = base64.urlsafe_b64decode(attachment['data'].encode('UTF-8'))
                f = open(config['pdf_filename'], 'wb')
                f.write(file_data)
                f.close()


def open_tabula_app(tabula_path, open_app):
    if open_app == 'y':
        execute_command('open {}'.format(tabula_path))


def get_citi_transactions(file_path):
    bank_transactions = pd.read_csv(file_path, header=None)
    bank_transactions.columns = ['date', 'ref', 'name', 'rate', 'payments', 'value']
    bank_transactions = bank_transactions[bank_transactions.value.str.contains('-') == False]
    bank_transactions.drop(columns=['ref', 'rate', 'payments'], inplace=True)
    bank_transactions['date'] = pd.to_datetime(bank_transactions['date'], format='%d/%m/%y')
    bank_transactions['value'] = bank_transactions['value'].replace('[\$,]', '', regex=True).astype(float)

    return bank_transactions


def get_min_max_dates(transactions):
    min_date = min(transactions['date']).strftime('%Y/%m/%d')
    max_date = (max(transactions['date']) + timedelta(days=1)).strftime('%Y/%m/%d')
    return min_date, max_date


def get_citi_messages(gmail_client, config, ini_date, end_date):
    checkpoint_file = config['citi_mails_checkpoint']

    if not isfile(checkpoint_file):
        query = 'from:{} subject:{} in:{} after:{} before:{}'
        query = query.format(config['from_citi_email'], config['transaction_email_subject'], config['transactions_email_folder'],
                             ini_date, end_date)

        all_messages = get_email_messages(gmail_client, query)

        pk.dump(all_messages, open(checkpoint_file, "wb"))
    else:
        all_messages = pk.load(open(checkpoint_file, 'rb'))

    return all_messages


def get_citi_message_body(message):
    payld = message['payload']
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


def get_citi_mail_data(gmail_client, config, min_date, max_date):
    citi_messages = get_citi_messages(gmail_client, config, min_date, max_date)

    data = []
    for m in citi_messages:
        r = get_citi_message_body(m)
        r = r.replace('\r', '')
        sentences = r.split('\n')
        for s in sentences:
            if 'Visa Gold Aadvantage' in s:
                text = s.strip()[:-4]
                print(text)
                date = datetime.strptime(find_in_text(r'\d\d/\d\d/\d\d', text), '%d/%m/%y')
                value = float(find_in_text(r'\d+\.\d+', text))
                name = text.split(' en ')[-1][:-1]
                data.append({
                    'date': date,
                    'value': value,
                    'name': name
                })

    print('-----------------------------------------------------------------------------------------------------------')
    return data


def citi_mail_exists(row, messages):
    for m in messages:
        name_value = m['name'] in row['name'] and m['value'] == row['value']
        date_name = m['name'] in row['name'] and m['date'].strftime('%Y-%m-%d') == row['date'].strftime('%Y-%m-%d')
        if name_value or date_name:
            return True
    return False


def get_uber_mail_data(gmail_client, config, min_date, max_date):
    uber_messages = get_uber_messages(gmail_client, config, min_date, max_date)

    data = []
    for m in uber_messages:
        print(m['snippet'])
        value_str = find_in_text(r'\$\d+', m['snippet'])
        value = float(value_str[1:])
        part = m['snippet'].split(' | ')[0].replace(value_str, '').replace(' Thanks for choosing Uber, Camilo ', '')
        try:
            date = datetime.strptime(part.strip(), '%B %d, %Y')
        except:
            date_str = input("Error. Enter date manually: ")
            date = datetime.strptime(date_str, '%B %d, %Y')
        data.append({
            'date': date,
            'value': value
        })

    print('-----------------------------------------------------------------------------------------------------------')
    return data


def get_uber_messages(gmail_client, config, min_date, max_date):
    checkpoint_file = config['uber_mails_checkpoint']

    if not isfile(checkpoint_file):
        query = 'from:{} in:{} after:{} before:{}'
        query = query.format(config['from_uber_email'], config['uber_email_folder'], min_date,
                             max_date)

        all_messages = get_email_messages(gmail_client, query)

        pk.dump(all_messages, open(checkpoint_file, "wb"))
    else:
        all_messages = pk.load(open(checkpoint_file, 'rb'))

    return all_messages


def uber_mail_exists(row, messages):
    if 'uber' in row['name'].lower():
        for m in messages:
            name_value = m['value'] == row['value']
            date_name = m['date'].strftime('%Y-%m-%d') == row['date'].strftime('%Y-%m-%d')
            if name_value or date_name:
                return True
        return False
    return ''


def save_results(results, config):
    results['date'] = results['date'].dt.strftime("%d/%m/%Y")
    writer = pd.ExcelWriter(config['excel_file'])
    results.to_excel(writer, 'Sheet1')
    writer.save()
    results.to_csv(config['csv_file'], index=False)


def get_credit_card_receipts_email(gmail_client, config):
    curr_date = datetime.today()
    last_day = calendar.monthrange(curr_date.year, curr_date.day)[1]
    min_date = curr_date.replace(day=1).strftime('%Y/%m/%d')
    max_date = curr_date.replace(day=last_day).strftime('%Y/%m/%d')

    query = 'in:{} after:{} before:{}'.format(config['credit_card_receipts_folder'], min_date, max_date)
    # complete_message = get_email_messages(gmail_client, query)[0]

    # pk.dump(complete_message, open('./data/tmp/cc_receipts_email', 'wb'))
    complete_message = pk.load(open('./data/tmp/cc_receipts_email', 'rb'))

    return complete_message


def download_credit_card_receipts(gmail_client, config):
    email = get_credit_card_receipts_email(gmail_client, config)

    path = join(config['credit_card_receipts_images_folder'], datetime.today().strftime('%B'))
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)

    for part in email['payload']['parts']:
        if part['mimeType'] == 'application/octet-stream':
            attachment_id = part['body']['attachmentId']
            attachment = gmail_client.get_attachment(email['id'], attachment_id)
            file_data = base64.urlsafe_b64decode(attachment['data'].encode('UTF-8'))

            file_name = os.path.join(path, part['filename'])
            f = open(file_name, 'wb')
            f.write(file_data)
            f.close()


def delete_tmp_data(config):
    delete_data = input('Delete data (y/n)? ')
    if delete_data == 'y':
        os.remove(config['email']['pdf_filename'])
        os.remove(config['email']['cc_email_checkpoint'])
        os.remove(config['email']['uber_mails_checkpoint'])
        os.remove(config['email']['citi_mails_checkpoint'])
        os.remove(config['tabula']['output_file'])
        os.remove(config['results']['excel_file'])
        os.remove(config['results']['csv_file'])


def main():
    config_file = './config/config.json'

    with open(config_file, 'r') as f:
        config = json.load(f)

    gmail_client = Gmail(config['gmail'])

    delete_tmp_data(config)

    get_credit_card_attachment(gmail_client, config['email'])
    input("Open pdf manually and print to remove pass. Continue? ")

    open_app = input('Open tabula (y/n)? ')
    open_tabula_app(config['tabula']['path'], open_app)
    input("Work in tabula to get csv. Continue? ")

    bank_transactions = get_citi_transactions(config['tabula']['output_file'])
    min_date, max_date = get_min_max_dates(bank_transactions)

    citi_mails_data = get_citi_mail_data(gmail_client, config['email'], min_date, max_date)
    bank_transactions['citi_mail'] = bank_transactions.apply(lambda row: citi_mail_exists(row, citi_mails_data), axis=1)

    uber_mails_data = get_uber_mail_data(gmail_client, config['email'], min_date, max_date)
    bank_transactions['uber_mail'] = bank_transactions.apply(lambda row: uber_mail_exists(row, uber_mails_data), axis=1)

    save_results(bank_transactions, config['results'])
    delete_tmp_data(config)
    print("DONE")

    # print(bank_transactions)
    # os.remove(config['email']['pdf_filename'])
    # os.remove(config['tabula']['output_file'])


def get_receipts_data(config):
    pytesseract.pytesseract.tesseract_cmd = r'/usr/local/bin/tesseract'

    path = join(config['credit_card_receipts_images_folder'], datetime.today().strftime('%B'))

    onlyfiles = [join(path, f) for f in listdir(path) if isfile(join(path, f)) and f.endswith('jpg')]
    data = []
    for img in onlyfiles:
        ocr_result = pytesseract.image_to_string(Image.open(img), lang='spa')
        receipt_data = get_receipt_data(ocr_result)
        print(receipt_data)
        print("------------------------------------")
        data.append(receipt_data)
    return data


def get_receipt_data(text):
    lines = list(filter(bool, text.split('\n')))
    print(lines)
    if lines[0].lower() == 'credibanco':
        return get_credibanco_receipt_data(lines)
    elif starts_with_date(lines[0]):
        return get_redeban_receipt_data(lines)
    else:
        return get_credibanco_2_receipt_data(lines)

    return None


def get_credibanco_receipt_data(lines):
    date = datetime.strptime(lines[1], '%d/%m/%Y %H:%M:%S')
    value = float(find_in_text(r'\d+\,\d+', lines[-1]).replace(',', ''))
    name = ' '.join(lines[3].split(' ')[1:])
    data = {
        'date': date,
        'value': value,
        'name': name
    }
    return data


def starts_with_date(first_element):
    try:
        datetime.strptime(' '.join(first_element.split(' ')[:3]), '%b %d %Y')
        return True
    except:
        pass
    return False


def get_redeban_receipt_data(lines):
    date = datetime.strptime(' '.join(lines[0].split(' ')[:3]), '%b %d %Y')
    name = lines[1]
    data = {
        'date': date,
        'value': 0,
        'name': name
    }
    return data


def get_credibanco_2_receipt_data(lines):
    name = lines[0]
    # TODO: Este no queda muy bien. Toca revisar el de redeban porque la linea negra daña todo.


def test():
    config_file = './config/config.json'

    with open(config_file, 'r') as f:
        config = json.load(f)

    gmail_client = Gmail(config['gmail'])

    # download_credit_card_receipts(gmail_client, config['email'])
    get_receipts_data(config['email'])


if __name__ == '__main__':
    main()
    #test()


# fill new df column...take into account name and value and date... I guess... this one is harder.
# Done


def get_message_body_raw(message):
    msg_str = base64.urlsafe_b64decode(message['raw'].encode('ASCII')).decode("utf-8")
    mime_msg = email.message_from_string(msg_str)
    return mime_msg


def decrypt_pdf():
    """
    Cant use this because the version of the pdf is 4 and pypdf2 only works with version 1 and 2.
    """
    with open('./data/att.pdf', 'rb') as input_file, open('./data/credit_card_2.pdf', 'wb') as output_file:
        reader = PdfFileReader(input_file)
        writer = PdfFileWriter()
        p = getpass.getpass(prompt='PDF password: ')
        reader.decrypt(p)
        for i in range(reader.getNumPages()):
            writer.addPage(reader.getPage(i))

        writer.write(output_file)