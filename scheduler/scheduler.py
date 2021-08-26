import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
from datetime import datetime
import calendar
from datetime import timedelta
from apscheduler.schedulers.blocking import BlockingScheduler
import os
import yagmail
import json
import traceback

db_url = os.environ['DATABASEURL']
client = MongoClient(db_url)
db = client['ikea_products']
stocks_table = db.stocks
users_table = db.users
scheduler = BlockingScheduler()

print("start crawler schduler...")

@scheduler.scheduled_job('interval', minutes=15)
def crawl_data():
    print("Starting reading from DB...")
    #read from database
    for item in stocks_table.find():
        print("Crawling product: "+str(item['_id']))
        store_list = list()
        user_id = item['user_id']
        try:
            for store in item['stock_info']:
                store_list.append(store['store_id'])
            updated_info = get_stock_info(item['product_id'], item['stock_info'])
            stocks_table.update_one(
                {'_id': item['_id']},
                {'$set':
                    {'stock_info': updated_info, 'update_time': datetime.utcnow()}
                }
            )
            #Notify user
            status, stores = get_notify_status(updated_info, item)
            notify(status, stores, user_id, item, updated_info)
        except Exception as e:
            if os.getenv('SEND_ERROR_EMAIL', '').upper() == 'TRUE':
                err_content = '{}\n{}'.format(str(e), traceback.format_exc())
                send_email(subject='Error Ikea Stock Checker', content=err_content, user_id=user_id)
            print('Error checking stock... {}'.format(os.getenv('SEND_ERROR_EMAIL')))
            return
    print("Finished updating DB...")
    
def get_notify_status(data, item):
    status_map = dict()
    for info in data:
        store_id = info['store_id']
        #if this product is back in stock at a store and previously out of stock
        if info['quantity'] > 0 and (item['last_notify_time'] is None or info['quantity_old']) <= 0:
            statusmap['instock'] = status_map.setdefault('in_stock', []) + [store_id]
            continue
        # if changed to out of stock at one store, previously in stock
        if info['quantity'] <= 0 and info['quantity_old'] > 0:
            statusmap['out_of_stock'] = status_map.setdefault('out_of_stock', []) + [store_id]
            continue
        # if product stock changes in a certain store and last notification time is more than 20mins ago
        if info['quantity'] > 0 and info['quantity_old'] != info['quantity'] and info['quantity_old'] > 0 and ((datetime.utcnow() - item['last_notify_time']).seconds / 60 > 20):
            statusmap['changed'] = status_map.setdefault('changed', []) + [store_id]
            continue
    if len(status_map.get('in_stock', [])) > 0:
        return 'in_stock', status_map['in_stock']
    elif len(status_map.get('out_of_stock', [])) > 0:
        return 'out_of_stock', status_map['out_of_stock']
    else:
        return 'changed', status_map.get('changed', [])

def notify(status, stores, user_id, item, updated_info):
    subject_obj = {
        'in_stock': ' is back in stock at {}!',
        'changed': ' availability changed at {}',
        'out_of_stock': ' now sold out at {}'
    }
    #generate email content
    receiver = users_table.find_one({'_id': user_id})
    content = generate_email_content(receiver['user_name'], item['_id'], item['product_name'], item['product_desc'], item['product_url'], updated_info)
    stores_str = ','.join([get_store_name_by_id(each) for each in stores])
    print('STORE STR', stores_str)
    #send emailc
    send_email(subject='Your IKEA product ' + str(item['product_name']) + subject_obj[status].format(stores_str), content=content, user_id=user_id)
    print("Email sent successfully")
    #update notify time
    stocks_table.update_one(
        {'_id': item['_id']},
        {'$set':
            {'last_notify_time': datetime.utcnow()}
        }
    )

def send_email(subject, content, user_id):
    #get email of receiver
    receiver = users_table.find_one({'_id': user_id})
    print('send to: ' + receiver['user_email'])
    yag = yagmail.SMTP(user=os.environ['MAILACCOUNT'], password=os.environ['MAILPASSWD'])
    yag.send(to=receiver['user_email'], newline_to_break=False , subject=subject, contents=content)

def get_stock_info(product_id, stock_info):
    ## database API
    store_list = [each.get('store_id') for each in stock_info if 'store_id' in each]
    stock_url = '{}/crawl?productId={}&buCodes={}'.format(os.getenv('CRAWLER'), str(product_id), json.dumps(store_list))
    r = requests.get(stock_url)
    assert(r.status_code == 200)
    res = []
    for store in r.json():
        _dict = {
            'store_id': store.get('buCode', 'N/A'),
            'quantity': int(store.get('stock', 0)),
            'quantity_old': -1
        }
        store_old = [each for each in stock_info if each.get('store_id') == store.get('buCode')]
        if len(store_old) > 0:
            _dict.update({'quantity_old': store_old[0].get('quantity')})
        res.append(_dict)
    return res

def get_store_name_by_id(store_id):
    with open('assets/store.json') as f:
        store_data = json.load(f)
    store_dict = dict()
    for store in store_data:
        store_number = store.get('storeNumber', -1)
        store_city = store.get('storeCity', 'N/A')
        if store_number == store_id:
            return store_city
    return 'N/A'

def generate_email_content(receiver_name, crawl_id, product_name, product_desc, product_url, stocks_info):
    f = open('./assets/email_template.html', 'r')
    content = f.read()
    #get images
    req = requests.get(product_url)
    soup = BeautifulSoup(req.content, 'html.parser')
    images = soup.find_all('div', {'class': 'range-revamp-media-grid__media-container'})
    if len(images) > 1:
        main_image = images[1]
    else:
        main_image = image[0]
    store_info = ''
    for info in stocks_info:
        store_info += get_store_name_by_id(info['store_id']) + ' has ' + str(info['quantity']) + " left!<br>" 

    unsubscribe_url = os.environ['DOMAIN'] + 'products/' + str(crawl_id)
    date = calendar.month_name[datetime.today().month] + ' ' + str(datetime.today().day) + '.' + str(datetime.today().year)
    content = content.replace('user_name', receiver_name)
    content = content.replace('product_name', product_name)
    content = content.replace('date', date)
    content = content.replace('product_image', main_image.img['src'])
    content = content.replace('product_url', product_url)
    content = content.replace('store_infos', store_info)
    content = content.replace('unsubscribe_url', unsubscribe_url)
    #for info in stocks_info:
        #content += 'store: ' + info['store_id'] + ' quantity: ' + str(info['quantity']) + '\n'
    return content
 
if 'DEBUG' in os.environ:
    print('DEBUG MODE: ON')
    crawl_data()
else:
    scheduler.start()
    