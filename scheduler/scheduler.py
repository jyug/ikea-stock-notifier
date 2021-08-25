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
            updated_info = get_stock_info(item['product_id'], store_list)
            stocks_table.update_one(
                {'_id': item['_id']},
                {'$set':
                    {'stock_info': updated_info, 'update_time': datetime.utcnow()}
                }
            )
        except Exception as e:
            if os.getenv('SEND_ERROR_EMAIL', '').upper() == 'TRUE':
                err_content = '{}\n{}'.format(str(e), traceback.format_exc())
                send_email(subject='Error Ikea Stock Checker', content=err_content, user_id=user_id)
            print('Error checking stock... {}'.format(os.getenv('SEND_ERROR_EMAIL')))
            return
        #Notify user
        notify = False
        for info in updated_info:
            #if this product is back in stock and we haven't notified user within 20 mins
            if (info['quantity'] > 0) and (item['last_notify_time'] is None or ((datetime.utcnow() - item['last_notify_time']).seconds / 60 > 20)):
                notify = True
            print(info['quantity'], item['last_notify_time'], notify)
        if notify:
            #generate email content
            receiver = users_table.find_one({'_id': user_id})
            content = generate_email_content(receiver['user_name'], item['_id'], item['product_name'], item['product_desc'], item['product_url'], updated_info)
            #send email
            send_email(subject='Your IKEA product ' + str(item['product_name']) + ' is back in stock!', content=content, user_id=user_id)
            print("Email sent successfully")
            #update notify time
            stocks_table.update_one(
                {'_id': item['_id']},
                {'$set':
                    {'last_notify_time': datetime.utcnow()}
                }
            )
    print("Finished updating DB...")
    

def send_email(subject, content, user_id):
    #get email of receiver
    receiver = users_table.find_one({'_id': user_id})
    print('send to: ' + receiver['user_email'])
    yag = yagmail.SMTP(user=os.environ['MAILACCOUNT'], password=os.environ['MAILPASSWD'])
    yag.send(to=receiver['user_email'], newline_to_break=False , subject=subject, contents=content)

def get_stock_info(product_id, store_list):
    ## database API
    stock_url = '{}/crawl?productId={}&buCodes={}'.format(os.getenv('CRAWLER'), str(product_id), json.dumps(store_list))
    r = requests.get(stock_url)
    assert(r.status_code == 200)
    res = []
    for store in r.json():
        res.append({'store_id': store.get('buCode', 'N/A'), 'quantity': int(store.get('stock', 0))})
    return res

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
    #get stoock info
    with open('assets/store.json') as f:
        store_data = json.load(f)
    store_dict = dict()
    for store in store_data:
        store_city = store['storeCity']
        store_number = store['storeNumber']
        if store_city != "" and store_number != "":
            store_dict[store_number] = store_city
    
    store_info = ''
    for info in stocks_info:
        store_info += store_dict[info['store_id']] + ' has ' + str(info['quantity']) + " left!<br>" 

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
    