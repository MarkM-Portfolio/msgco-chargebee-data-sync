#!/usr/bin/env python3

###################################################################################
#-+- Syntax example: ./chargebee_sync.py -e stg -u user@email.com -+-#
#-+- Batch auto-sync example: ./chargebee_sync.py -e stg -b -f accounts.txt -s -+-#
###################################################################################


import os, sys, argparse, requests, chargebee, boto3, json, xmltodict, pandas as pd


class ChargebeeSync():

    def __init__(self):
        self.initialized = False

    def initialize(self, *kwargs):        
        if not self.initialized:
            self.initialized = True

            try:
                secrets = boto3.client('secretsmanager', region_name='us-east-1' if env in [ 'PROD', 'PRODUCTION' ] else 'us-west-2').get_secret_value(SecretId='chargebee-secrets')
            except (Exception) as e:
                self.err_log('sys', e)
            else:
                secrets = json.loads(secrets['SecretString'])

            self.cb_instance = self.set_cb_instance()

            self.SECRETS = {
                'api_key': secrets.get(f"{ self.cb_instance.upper() }_API_KEY"),
                'api_url': secrets.get('MAILSERVER_URL').replace('[platform]', 'pc4' if self.cb_instance == 'latam' else 'pc5'),
                'username': secrets.get('MAILSERVER_USERNAME'),
                'password': secrets.get('MAILSERVER_PASSWORD')
            }

        # chargebee.configure(self.SECRETS.get('api_key'), self.cb_instance if env in [ 'PROD', 'PRODUCTION' ] else f"{ self.cb_instance }-test")

        print('\nChargebee Instance: ', self.cb_instance)
        print('Environment: ', env)
        print('Customer Email: ', customer.get('email'))
        if batch:
            print('Customer Plan: ', customer.get('plan'))
            print('Customer Cos Profile: ', customer.get('cos_profile_name'))

        self.start()

    def set_cb_instance(self):
        instances = [ 'msgco', 'tasman', 'latam' ] if env in [ 'PROD', 'PRODUCTION' ] else [ 'msgco', 'tasman' ]
        
        print('\n\t>> Select the Chargebee instance <<\n')
        for idx in range(len(instances)):
            print(f"\t[{ idx + 1 }] - { instances[idx] } ({ instances[idx] }.chargebee.com)") if env in [ 'PROD', 'PRODUCTION' ] else print(f"\t[{ idx + 1 }] - { instances[idx] } ({ instances[idx] }-test.chargebee.com)")
            
        try:
            user_input = int(input('\nInput : '))
            instances[user_input - 1] if user_input != 0 else instances[len(instances)]
        except (Exception) as e:
            self.discrepancies = None
            self.err_log('inp', e)

        return instances[user_input - 1]

    def start(self):
        chargebee.configure(self.SECRETS.get('api_key'), self.cb_instance if env in [ 'PROD', 'PRODUCTION' ] else f"{ self.cb_instance }-test")
        # Get Customer and output Customer ID
        self.CUSTOMER = self.chargebee_api('GET', 'customer')
        try:
            print('Customer ID: ', self.CUSTOMER.id)
        except (Exception):
            self.err_log('cb', f"Customer { customer.get('email') } doesn\'t exist.")
            print('Process terminated ! Might be the wrong chargebee instance ?', f"({ self.cb_instance }.chargebee.com)" if env in [ 'PROD', 'PRODUCTION' ] else f"({ self.cb_instance }-test).chargebee.com")
            sys.exit(1)

        self.discrepancies = False
        # Get Active Subscriptions then output Subscription ID & Due Invoices Count
        self.SUBSCRIPTION = self.chargebee_api('GET', 'subscription', { 'status[is]': 'active' })
        # self.SUBSCRIPTION = self.chargebee_api('GET', 'subscription', { 'status[is]': 'cancelled' }) # TEST No subscriptions
        try:
            len(self.SUBSCRIPTION)
        except (Exception):
            print(f"No active subscriptions for customer { self.CUSTOMER.email }")
            # Accounts that are marked active (or not disabled) that don’t have any active subscription.
            self.ACCOUNT = self.mailserver_api('GET', 'accounts', 'view', [ 'account_status' ])
            if self.ACCOUNT.get('account_status') != 'disabled':
                self.discrepancies = True
                ## COMMENT ACTIVE ACCOUNTS MARKED AS DISABLED W/O SUBSCRIPTION
                # print('\n[ Anomaly Detected ] - Accounts that are marked active (or not disabled) that don’t have any active subscription.')
                # if not sync:
                #     user_input = input('\nDo you want to sync [y/N] ? ')
                #     if user_input.lower() not in [ 'yes', 'y' ]:
                #         self.err_log('inp', '\nUpdate Ignored!')
                #     else:
                #         print('\nTriggering Subscription Change event...')
                #         self.SUBSCRIPTION = self.chargebee_api('GET', 'subscription')
                #         self.UPDATE = self.chargebee_api('PUT', 'subscription', { 'cf_Update_Subscription_Toggle': 'True' if self.SUBSCRIPTION[0].cf_Update_Subscription_Toggle == 'False' else 'False' })
                #         self.err_log('inp', f"Update Success! cf_Update_Subscription_Toggle : { self.UPDATE.cf_Update_Subscription_Toggle }")
                # else:
                #     print('\nTriggering Subscription Change event...')
                #     self.SUBSCRIPTION = self.chargebee_api('GET', 'subscription')
                #     self.UPDATE = self.chargebee_api('PUT', 'subscription', { 'cf_Update_Subscription_Toggle': 'True' if self.SUBSCRIPTION[0].cf_Update_Subscription_Toggle == 'False' else 'False' })
                #     self.err_log('inp', f"Update Success! cf_Update_Subscription_Toggle : { self.UPDATE.cf_Update_Subscription_Toggle }")
        else: 
            sub_ids = []
            sub_items = []
            due_inv_cnt = []
            # Accounts with more than one active subscription.
            if len(self.SUBSCRIPTION) > 1:
                for sub in self.SUBSCRIPTION:
                    sub_ids.append(self.SUBSCRIPTION[sub].id)
                    sub_items.append(self.SUBSCRIPTION[sub].subscription_items[0].item_price_id)
                    due_inv_cnt.append(self.SUBSCRIPTION[sub].due_invoices_count)
                print('Subscription IDs: ', sub_ids)
                print('Subscription Items: ', sub_items)
                print('Due Invoices Count: ', due_inv_cnt)
            else:
                sub_ids.append(self.SUBSCRIPTION[0].id)
                sub_items.append(self.SUBSCRIPTION[0].subscription_items[0].item_price_id)
                due_inv_cnt.append(self.SUBSCRIPTION[0].due_invoices_count)
                print('Subscription ID: ', self.SUBSCRIPTION[0].id)
                print('Subscription Item: ', self.SUBSCRIPTION[0].subscription_items[0].item_price_id)
                print('Due Invoices Count: ', self.SUBSCRIPTION[0].due_invoices_count)

            # Accounts marked as disabled (or not active) in any way that don’t owe fees with active subscriptions.
            self.ACCOUNT = self.mailserver_api('GET', 'accounts', 'view', [ 'account_status' ])
            # if self.ACCOUNT.get('account_status') != 'active' and self.SUBSCRIPTION.due_invoices_count == 0:
            if self.ACCOUNT.get('account_status') != 'active' and sum(due_inv_cnt) == 0:
                self.discrepancies = True
                print('\n[ Anomaly Detected ] - Accounts marked as disabled (or not active) in any way that don’t owe fees with active subscriptions.')
                if not sync:
                    user_input = input('\nDo you want to sync [y/N] ? ')
                    if user_input.lower() not in [ 'yes', 'y' ]:
                        self.err_log('inp', '\nUpdate Ignored!')
                    else:
                        print('\nTriggering Subscription Change event...')
                        self.UPDATE = self.chargebee_api('PUT', 'subscription', { 'cf_Update_Subscription_Toggle': 'True' if self.SUBSCRIPTION[0].cf_Update_Subscription_Toggle == 'False' else 'False' })
                        self.err_log('inp', f"Update Success! cf_Update_Subscription_Toggle : { self.UPDATE.cf_Update_Subscription_Toggle }")
                else:
                    print('\nTriggering Subscription Change event...')
                    self.UPDATE = self.chargebee_api('PUT', 'subscription', { 'cf_Update_Subscription_Toggle': 'True' if self.SUBSCRIPTION[0].cf_Update_Subscription_Toggle == 'False' else 'False' })
                    self.err_log('inp', f"Update Success! cf_Update_Subscription_Toggle : { self.UPDATE.cf_Update_Subscription_Toggle }")
            # Accounts marked active (or not rstrBilling) with active subscriptions that owe fees but are not in rstrBilling state.
            self.ACCOUNT = self.mailserver_api('GET', 'accounts', 'view', [ 'account_status' ])
            # if self.ACCOUNT.get('account_status') != 'rstrBilling' and self.SUBSCRIPTION.due_invoices_count > 0:
            if self.ACCOUNT.get('account_status') != 'rstrBilling' and sum(due_inv_cnt) > 0:
                self.discrepancies = True
                print('\n[ Anomaly Detected ] - Accounts marked active (or not rstrBilling) with active subscriptions that owe fees but are not in rstrBilling state.')
                if not sync:
                    user_input = input('\nDo you want to sync [y/N] ? ')
                    if user_input.lower() not in [ 'yes', 'y' ]:
                        self.err_log('inp', '\nUpdate Ignored!')
                    else:
                        print('\nTriggering Subscription Change event...')
                        self.UPDATE = self.chargebee_api('PUT', 'subscription', { 'cf_Update_Subscription_Toggle': 'True' if self.SUBSCRIPTION[0].cf_Update_Subscription_Toggle == 'False' else 'False' })
                        self.err_log('inp', f"Update Success! cf_Update_Subscription_Toggle : { self.UPDATE.cf_Update_Subscription_Toggle }")
                else:
                    print('\nTriggering Subscription Change event...')
                    self.UPDATE = self.chargebee_api('PUT', 'subscription', { 'cf_Update_Subscription_Toggle': 'True' if self.SUBSCRIPTION[0].cf_Update_Subscription_Toggle == 'False' else 'False' })
                    self.err_log('inp', f"Update Success! cf_Update_Subscription_Toggle : { self.UPDATE.cf_Update_Subscription_Toggle }")

            # Accounts with active subscriptions who’s class of service does not reflect their current subscription.
            if batch:
                self.ACCOUNT = self.mailserver_api('GET', 'accounts', 'view', [ 'account_status' ])
                print('\n Subscription Items:: ', sub_items, '- Customer Plan: ', customer.get('plan'))
                print(f"Cos Profile Status: { customer.get('cos_profile_name') }", self.ACCOUNT.get(customer.get('cos_profile_name')))
                if self.ACCOUNT.get('account_status') == 'active' and self.ACCOUNT.get(customer.get('cos_profile_name')) != 'active':
                # if self.ACCOUNT.get('account_status') == 'active' and customer.get('cos_profile_name') not in sub_items:
                    self.discrepancies = True
                    print('\n[ Anomaly Detected ] - Accounts with active subscriptions who’s class of service does not reflect their current subscription.')
                    if not sync:
                        user_input = input('\nDo you want to sync [y/N] ? ')
                        if user_input.lower() not in [ 'yes', 'y' ]:
                            self.err_log('inp', '\nUpdate Ignored!')
                        else:
                            print('\nTriggering Subscription Change event...')
                            self.UPDATE = self.chargebee_api('PUT', 'subscription', { 'cf_Update_Subscription_Toggle': 'True' if self.SUBSCRIPTION[0].cf_Update_Subscription_Toggle == 'False' else 'False' })
                            self.err_log('inp', f"Update Success! cf_Update_Subscription_Toggle : { self.UPDATE.cf_Update_Subscription_Toggle }")
                    else:
                        print('\nTriggering Subscription Change event...')
                        self.UPDATE = self.chargebee_api('PUT', 'subscription', { 'cf_Update_Subscription_Toggle': 'True' if self.SUBSCRIPTION[0].cf_Update_Subscription_Toggle == 'False' else 'False' })
                        self.err_log('inp', f"Update Success! cf_Update_Subscription_Toggle : { self.UPDATE.cf_Update_Subscription_Toggle }")
                    # print('Subscription Items:: ', sub_items)
                    # print(f"Cos Profile Status: { customer.get('cos_profile_name') }", self.ACCOUNT.get(customer.get('cos_profile_name')))

            # Get Invoices if there are outstanding ones or not eq 0.. 
            if len(self.SUBSCRIPTION) > 1:
                inv_ids = []
                inv_status = []
                inv_total = []
                inv_amt_paid = []
                inv_amt_due = []
                for id in sub_ids:
                    try:
                        self.INVOICE = self.chargebee_api('GET', 'invoice', id)
                    except (Exception):
                        self.err_log('cb', f"No invoices for customer { self.CUSTOMER.email }")
                    else:
                        inv_ids.append(self.INVOICE.id)
                        inv_status.append(self.INVOICE.status)
                        inv_total.append(self.INVOICE.total)
                        inv_amt_paid.append(self.INVOICE.amount_paid)
                        inv_amt_due.append(self.INVOICE.amount_due)
                print('Invoice IDs: ', inv_ids)
                print('Invoice Status: ', inv_status)
                print('Invoice Total: ', inv_total)
                print('Invoice Amount Paid: ', inv_amt_paid)
                print('Invoice Amount Due: ', inv_amt_due)


            else:
                try:
                    self.INVOICE = self.chargebee_api('GET', 'invoice', self.SUBSCRIPTION[0].id)
                except (Exception):
                    self.err_log('cb', f"No invoices for customer { self.CUSTOMER.email }")
                else:
                    print('Invoice ID: ', self.INVOICE.id)
                    print('Invoice Status: ', self.INVOICE.status)
                    print('Invoice Total: ', self.INVOICE.total)
                    print('Invoice Amount Paid: ', self.INVOICE.amount_paid)
                    print('Invoice Amount Due: ', self.INVOICE.amount_due)

        self.end(0)

    def chargebee_api(self, method, query, data=None):
        if method == 'GET':
            if query == 'customer':
                try:
                    entries = chargebee.Customer.list({ 'email[is]': customer.get('email') })
                except (Exception) as e:
                    self.err_log('cb', e)

                for entry in entries:
                    result = entry.customer

            if query == 'subscription':
                params = {}

                try:
                    params = { 'customer_id[is]': self.CUSTOMER.id }
                except (Exception) as e:
                    self.err_log('cb', e)
                
                params.update(data) if data else None

                try:
                    entries = chargebee.Subscription.list(params)
                except (Exception) as e:
                    self.err_log('cb', e)

                result = {}

                for entry in range(len(entries)):
                    result[entry] = entries[entry].subscription

            if query == 'invoice':
                try:
                    entries = chargebee.Invoice.list({ 'subscription_id[is]': data })
                except (Exception) as e:
                    self.err_log('cb', e)

                for entry in entries:
                    result = entry.invoice

            return result if len(entries) else None
        
        if method == 'PUT':
            if query == 'subscription':
                try:
                    entries = chargebee.Subscription.update_for_items(self.SUBSCRIPTION[0].id, data)
                except (Exception) as e:
                    self.err_log('cb', e)

                result = entries.subscription

            return result

    def mailserver_api(self, method, query, action, data=None):
        params = { 'username': customer.get('email') }

        if method == 'GET':
            if action == 'view':
                try:
                    response = requests.get(f"{ self.SECRETS.get('api_url') }/{ query }/{ action }", auth=(self.SECRETS.get('username'), self.SECRETS.get('password')), params=params, verify=True)
                    response.raise_for_status()
                except (Exception) as e:
                    self.err_log('ms', e)

            if action == 'update':
                params.update(data) if data else self.err_log('ms', 'API Error update. No data parameters.')

                try:
                    response = requests.get(f"{ self.SECRETS.get('api_url') }/{ query }/{ action }", auth=(self.SECRETS.get('username'), self.SECRETS.get('password')), params=params, verify=True)
                    response.raise_for_status()
                except (Exception) as e:
                    self.err_log('ms', e)
                else:
                    action = 'view'
                    try:
                        response = requests.get(f"{ self.SECRETS.get('api_url') }/{ query }/{ action }", auth=(self.SECRETS.get('username'), self.SECRETS.get('password')), params=params, verify=True)
                        response.raise_for_status()
                    except (Exception) as e:
                        self.err_log('ms', e)

        return self.parse_xml(response.text, data)

    def parse_xml(self, raw_xml, data):
        xml_data = xmltodict.parse(raw_xml)
        parsed_xml = {}

        try:
            if xml_data.get('api').get('accountview').get('status') != 'success':
                raise Exception(f"{ parsed_xml.get('message') }: { self.SECRETS.get('api_url') }. Please check credentials.")
        except (Exception) as e:
            self.err_log('ms', e)
            print('Process terminated ! Might be the wrong chargebee instance ?', f"({ self.cb_instance }.chargebee.com)" if env in [ 'PROD', 'PRODUCTION' ] else f"({ self.cb_instance }-test).chargebee.com")
            sys.exit(1)
        else:
            account_status = xml_data.get('api').get('accountview').get('status')
            parsed_xml['account_status'] = account_status

            cosProfile = xml_data.get('api').get('accountview').get('response').get('results').get('cosProfile').get('key_0').get('profile')
            for key in cosProfile:
                name = cosProfile.get(key).get('name')
                status = 'active' if cosProfile.get(key).get('active') == '1' else 'not-active'
                if name == 'Kakadu-Plan-BV1':
                    parsed_xml[name] = status
            
        return parsed_xml
    
    def err_log(self, source, msg):
        if source == 'cb':
            _errorMsg = f"\n[Error| { msg }"
        if source == 'ms':
            _errorMsg = f"\n[Error| { msg }"
        if source == 'inp':
            _errorMsg = f"{ msg }" if self.discrepancies is not None else f"\n[Error| { msg }"
        if source == 'sys':
            _errorMsg = f"\n[Error| An unexpected error occurred: { msg }"
            
        print(f"{ _errorMsg }")

        self.end(1) if source != 'inp' else self.end(0)

    def end(self, exit_code):
        if exit_code == 0:
            if self.discrepancies is False:
                print(f"\nNo data discrepancies found in user: ({ customer.get('email') })")
                print('\nD O N E !!!')
            else:
                exit_code = 0 if self.discrepancies is True else 1

        if not batch:
            print(f"\nExited with status code [{ exit_code }]")
            sys.exit(exit_code)


if __name__ == '__main__':
    # s3://atmail-bizsys-reporting-data-au/platform/pc5/kakadu/accounts_whos_cos_profile_does_not_match_their_subscription.tsv
    parser = argparse.ArgumentParser(description='Chargebee Sync')
    parser.add_argument('-e', '--env', type=str, help='Environment', required=True)
    parser.add_argument('-u', '--user', type=str, help='Customer Email')
    parser.add_argument('-b', '--batch', action='store_true', help='Batch Processing')
    parser.add_argument('-f', '--file', type=str, help='Batch File')
    parser.add_argument('-s', '--sync', action='store_true', help='Auto Sync')
        
    if parser.parse_args().batch:
        try:
            if parser.parse_args().user is not None:
                raise Exception("The --batch and --user arguments cannot be used together.")
            if parser.parse_args().file is None:
                raise Exception("The --file argument is required when using --batch processing.")
        except (Exception) as e:
            print(f"\n[Error]: { e }")
            sys.exit(1)
    else:
        try:
            if parser.parse_args().user == '':
                raise Exception("The --user argument is required and cannot be empty.")
            elif not parser.parse_args().user:
                raise Exception("Should have at least (1) --batch or --user arguments.")
        except (Exception) as e:
            print(f"\n[Error]: { e }")
            sys.exit(1)

    try:
        env = parser.parse_args().env.upper()
        batch = parser.parse_args().batch
        file = parser.parse_args().file
        sync = parser.parse_args().sync

        if batch:
            df = pd.read_csv(file, delimiter='\t')
            customers = df.to_dict(orient='records')
            chargebee_sync = ChargebeeSync()
            for customer in customers:
                chargebee_sync.initialize(customer) 
        else:
            customer = {}
            customer['email'] = parser.parse_args().user.lower()
            ChargebeeSync().initialize()
    except (KeyboardInterrupt):
        ChargebeeSync().err_log('sys', "\n\n[Error| Program interrupted by the user (Ctrl+C)")
