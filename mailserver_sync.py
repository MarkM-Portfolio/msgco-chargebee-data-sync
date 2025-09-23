import os, sys, argparse, requests, chargebee, boto3, json
import xml.etree.ElementTree as ET


class ChargebeeSync():

    def __init__(self):
        try:
            secrets = boto3.client('secretsmanager', region_name='us-east-1' if env == 'PROD' else 'us-west-2').get_secret_value(SecretId='chargebee-secrets')
        except (Exception) as e:
            self.err_log('sys', e)
        else:
            secrets = json.loads(secrets['SecretString'])

        self.CREDENTIALS = {
            'api_key': secrets.get('CB_API_KEY'),
            'username': secrets.get('MS_USERNAME'),
            'password': secrets.get('MS_PASSWORD')
        }

        chargebee.configure(self.CREDENTIALS.get('api_key'), 'msgco' if env == 'PROD' else 'msgco-test')

        self.MAILSERVER = {
            'url': 'https://admin.pc5.atmailcloud.com' if env == 'PROD' else 'https://admin.pc5-stg.atmailcloud.com',
            'endpoint': 'index.php/api'
        }

    def start(self, *kwargs):
        print('\nEnvironment: ', env)
        print('Customer Email: ', customer_email)
        # Get Customer and output Customer ID
        self.CUSTOMER = self.chargebee_api('GET', 'customer')
        try:
            print('Customer ID: ', self.CUSTOMER.id)
        except (Exception):
            self.err_log('cb', f'Customer { customer_email } doesn\'t exist.')

        self.discrepancies = False
        # Get Active Subscriptions then output Subscription ID & Due Invoices Count
        self.SUBSCRIPTION = self.chargebee_api('GET', 'subscription', { 'status[is]': 'active' })
        # self.SUBSCRIPTION = self.chargebee_api('GET', 'subscription', { 'status[is]': 'cancelled' }) # TEST No subscriptions
        try:
            len(self.SUBSCRIPTION)
        except (Exception):
            print(f'No active subscriptions for customer { self.CUSTOMER.email }')
            # Accounts that are marked active (or not disabled) that don’t have any active subscription.
            self.ACCOUNT = self.mailserver_api('GET', 'accounts', 'view', [ 'account_status' ])
            if self.ACCOUNT.get('account_status') != 'disabled':
                self.discrepancies = True
                print('\n[ Anomaly Detected ] - Accounts that are marked active (or not disabled) that don’t have any active subscription.')
                user_input = input(f'Do you want to update account_status({ self.ACCOUNT.get('account_status') }) to disabled ? [y/N]\n')
                if user_input.lower() not in [ 'yes', 'y' ]:
                    self.err_log('inp', '\nUpdate Ignored!')
                else:
                    print('\nSyncing data ...')
                    self.ACCOUNT = self.mailserver_api('GET', 'accounts', 'update', { 'account_status': 'disabled' })
                    self.err_log('inp', f'Update Success! { self.ACCOUNT }')
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
                user_input = input(f'Do you want to update account_status({ self.ACCOUNT.get('account_status') }) to active ? [y/N]\n')
                if user_input.lower() not in [ 'yes', 'y' ]:
                    self.err_log('inp', '\nUpdate Ignored!')
                else:
                    print('\nSyncing data ...')
                    self.ACCOUNT = self.mailserver_api('GET', 'accounts', 'update', { 'account_status': 'active' })
                    self.err_log('inp', f'Update Success! { self.ACCOUNT }')
            # Accounts marked active (or not rstrBilling) with active subscriptions that owe fees but are not in rstrBilling state.
            self.ACCOUNT = self.mailserver_api('GET', 'accounts', 'view', [ 'account_status' ])
            # if self.ACCOUNT.get('account_status') != 'rstrBilling' and self.SUBSCRIPTION.due_invoices_count > 0:
            if self.ACCOUNT.get('account_status') != 'rstrBilling' and sum(due_inv_cnt) > 0:
                self.discrepancies = True
                print('\n[ Anomaly Detected ] - Accounts marked active (or not rstrBilling) with active subscriptions that owe fees but are not in rstrBilling state.')
                user_input = input(f'Do you want to update account_status({ self.ACCOUNT.get('account_status') }) to rstrBilling ? [y/N]\n')
                if user_input.lower() not in [ 'yes', 'y' ]:
                    self.err_log('inp', '\nUpdate Ignored!')
                else:
                    print('\nSyncing data ...')
                    self.ACCOUNT = self.mailserver_api('GET', 'accounts', 'update', { 'account_status': 'rstrBilling' })
                    self.err_log('inp', f'Update Success! { self.ACCOUNT }')

            # ??? Accounts with active subscriptions who’s class of service does not reflect their current subscription.

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
                        self.err_log('cb', f'No invoices for customer { self.CUSTOMER.email }')
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
                    self.err_log('cb', f'No invoices for customer { self.CUSTOMER.email }')
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
                    entries = chargebee.Customer.list({ 'email[is]': customer_email })
                except (Exception) as e:
                    self.err_log('cb', e)

                for entry in entries:
                    result = entry.customer

            if query == 'subscription':
                params = { 'customer_id[is]': self.CUSTOMER.id }
                
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

    def mailserver_api(self, method, query, action, data=None):
        params = { 'username': customer_email }

        if method == 'GET':
            if action == 'view':
                try:
                    response = requests.get(f'{ self.MAILSERVER.get('url') }/{ self.MAILSERVER.get('endpoint') }/{ query }/{ action }', auth=(self.CREDENTIALS.get('username'), self.CREDENTIALS.get('password')), params=params, verify=True)
                    response.raise_for_status()
                except (Exception) as e:
                    self.err_log('ms', e)

            if action == 'update':
                params.update(data) if data else self.err_log('ms', 'API Error update. No data parameters.')

                try:
                    response = requests.get(f'{ self.MAILSERVER.get('url') }/{ self.MAILSERVER.get('endpoint') }/{ query }/{ action }', auth=(self.CREDENTIALS.get('username'), self.CREDENTIALS.get('password')), params=params, verify=True)
                    response.raise_for_status()
                except (Exception) as e:
                    self.err_log('ms', e)
                else:
                    action = 'view'
                    try:
                        response = requests.get(f'{ self.MAILSERVER.get('url') }/{ self.MAILSERVER.get('endpoint') }/{ query }/{ action }', auth=(self.CREDENTIALS.get('username'), self.CREDENTIALS.get('password')), params=params, verify=True)
                        response.raise_for_status()
                    except (Exception) as e:
                        self.err_log('ms', e)

        return self.parse_xml(response.text, data)

    def parse_xml(self, raw_xml, data):
        filename = 'mailserver.xml'
        
        with open(filename, "w") as xml_file:
            xml_file.write(raw_xml)

        root = ET.parse(filename)
        parsed_xml = {}

        for child in root.iter():
            for tag in data:
                if child.tag == 'status':
                    parsed_xml[child.tag] = child.text
                if child.tag == 'message':
                    parsed_xml[child.tag] = child.text
                if child.tag == tag:
                    # if tag == 'cosProfile':
                    #     print('Child >> ', child.tag, child.text)
                    #     parsed_xml[child.tag] = child.text
                    #     continue
                    parsed_xml[child.tag] = child.text

        if parsed_xml.get('status') == 'success':
            parsed_xml.pop('status')
            parsed_xml.pop('message')
        else:
            self.err_log('ms', f'{ parsed_xml.get('message') }: { self.MAILSERVER.get('url') }. Please check credentials.')

        try:
            os.remove(filename)
        except (Exception) as e:
            self.err_log('sys', e)

        return parsed_xml
    
    def err_log(self, source, msg):
        if source == 'cb':
            _errorMsg = f'\n[Error| { msg }'
        if source == 'ms':
            _errorMsg = f'\n[Error| { msg }'
        if source == 'inp':
            _errorMsg = f'{ msg }'
        if source == 'sys':
            _errorMsg = f'\n[Error| An unexpected error occurred: { msg }'
            
        print(f'{ _errorMsg }')

        self.end(1) if source != 'inp' else self.end(0)

    def end(self, exit_code):
        if exit_code == 0:
            if not self.discrepancies:
                print(f'\nNo data discrepancies found in user: ({ customer_email })')
            print('\nD O N E !!!')
        else:
            print(f'\nExited with non-zero status code [{ exit_code }]')

        sys.exit(exit_code)

        
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Chargebee Sync')
    parser.add_argument('-e', '--env', type=str, help='Environment', required=True)
    parser.add_argument('-u', '--user', type=str, help='Customer Email', required=True)
    env = parser.parse_args().env.upper()
    customer_email = parser.parse_args().user.lower()

    ChargebeeSync().start(env, customer_email)
    