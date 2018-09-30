# -*- coding: utf-8 -*-
import argparse
import json

from external.gmail import Gmail
from src.credit_card_statement.credit_card_statement import CreditCardStatement
from src.utils.utils import execute_command


def open_tabula_app(config):
    open_app = input('Open tabula (y/n)? ')
    if open_app == 'y':
        execute_command('open {}'.format(config['path']))
        input('Work in tabula to get csv. Continue? ')


def get_config(config_file):
    with open(config_file, 'r') as f:
        config = json.load(f)
    return config


def main(config):
    gmail_client = Gmail(config['gmail'])

    ccs = CreditCardStatement(gmail_client, config['creditCardStatement'])
    ccs.get_credit_card_statement()

    open_tabula_app(config['tabula'])

    # bank_transactions = get_citi_transactions(config['tabula']['output_file'])
    # min_date, max_date = get_min_max_dates(bank_transactions)

    # citi_mails_data = get_citi_mail_data(gmail_client, config['email'], min_date, max_date)
    # bank_transactions['citi_mail'] = bank_transactions.apply(lambda row: citi_mail_exists(row, citi_mails_data), axis=1)

    # uber_mails_data = get_uber_mail_data(gmail_client, config['email'], min_date, max_date)
    # bank_transactions['uber_mail'] = bank_transactions.apply(lambda row: uber_mail_exists(row, uber_mails_data), axis=1)

    # save_results(bank_transactions, config['results'])
    # delete_tmp_data(config)
    # print("DONE")
    pass


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', required=True, help='Configuration file.')
    args = parser.parse_args()

    config_data = get_config(args.config)
    main(config_data)
