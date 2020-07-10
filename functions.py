from tqdm import tqdm_notebook as tqdm
import re, requests, bs4, logging, threading, time, random
import pandas as pd
import numpy as np

from config import *

logging.basicConfig(level=logging.DEBUG, format=' %(asctime)s - %(levelname)s - %(message)s')
logging.disable(logging.DEBUG)


def make_row(link_soup, link):
    
    # Create row dict containing the column names
    row = dict.fromkeys(['Brand', 'Model', 'Governerate', 'City', 'Date', 'Year', 'Kilometers', 'Pay_type',
                         'Ad_type','Transmission', 'CC', 'Chasis', 'Features', 'Color', 'Price', 'URL'])
    
    # car price
    row['Price'] = get_price(link_soup)
    
    # ad link
    row['URL'] = link['href']

    # ad date
    row['Date'] = get_date(link_soup)

    # ad location
    row['City'], row['Governerate'] = get_ad_location(link_soup)

    # car brand
    row['Brand'] = get_brand(link_soup, row['City'])

    # scrape the full info the ad 
    full_info = []
    for info in link_soup.select('strong a'):
        # collect all ad info from html
        full_info.append(re.sub('\t|\n', '', info.get_text()))
        
    # collect the headers for the ad info in order to sort them into their places
    ad_info_id = [x.get_text() for x in link_soup.select('th')]
    
    for info_id, info in zip(ad_info_id[:7], full_info[:7]):
        if info_id == 'موديل':
            row['Model'] = info
        elif info_id == 'ناقل الحركة':
            row['Transmission'] = info
        elif info_id == 'السنة':
            row['Year'] = info
        elif info_id == 'كيلومترات':
            row['Kilometers'] = info
        elif info_id == 'طريقة الدفع':
            row['Pay_type'] = info
        elif info_id == 'الحالة':
            row['State'] = info
        elif info_id == 'المحرك (سي سي)':
            row['CC'] = info
            
    # make a separate list for car features
    car_features = []
    
    # make a copy for ad info so as to remove all features except one, to add other features that go after
    full_info_c = full_info.copy()

    for i, info in enumerate(full_info[7:]):
        if info in ['EBD', 'إطارات خاصة','باور ستيرنج','بلوتوث', 'تكييف', 'تنبيه / نظام مضاد للسرقة', 'حامل السقف',
                     'حساسات ركن', 'راديو اف ام', 'زجاج كهربائي', 'زر تشغيل / إيقاف المحرك', 'سنتر لوك',
                     'شاحن يو اس بي', 'شاشة تعمل باللمس', 'عجلات للطرق الوعرة', 'فتحة سقف', 'فوانيس شبورة',
                     'كاميرا خلفية', 'مثبت سرعة', 'مدخل aux اوديو', 'مرايا كهربائية', 'مقاعد جلد', 'مقاعد كهربائية',
                     'نظام فرامل ABS', 'نظام ملاحة', 'وسائد هوائية']:
            car_features.append(info)
            if (i + 3) != len(full_info[7:]):
                full_info_c.remove(info)
                
    # removes the features id since all features are removed            
    if 'إضافات' in ad_info_id:
        ad_info_id.remove('إضافات')
    
    row['Features'] = car_features
    for info_id, info in zip(ad_info_id[7:], full_info_c[7:]):
        if info_id == 'اللون':
            row['Color'] = info
        elif info_id == 'نوع الهيكل':
            row['Chasis'] = info
        elif info_id == 'نوع الإعلان':
            row['Ad_type'] = info
            
    return row

def get_date(link_soup):
    try:
        date = re.sub('\t|\n', '', link_soup.select('p small span')[0].get_text()).split(',')[1]
        return date
    except:
        return 0

def get_ad_location(link_soup):
    try:
        location = link_soup.select('p span strong')[0].get_text().replace('\t', '').replace('\n', '').split('،')
        if len(location) != 2:
            location = link_soup.select('p span strong')[0].get_text().replace('\t', '').replace('\n', '').split(',')
        return location
    except:
        logging.critical('Error in get_ad_lcation')
        logging.critical(link_soup.select('p span strong'))
        return 0

def get_price(link_soup):
    try:
        price = link_soup.select('div .pricelabel strong')[0].get_text()
        price = re.search('\d*,\d*', price).group().replace(',','')
    except:
        try:
            price = link_soup.select('div .pricelabel strong')[0].get_text()
            price = re.search('\d*', price).group().replace(',','') 
        except:
            logging.critical('Error in get_price.')
            logging.critical(link_soup.select('div .pricelabel strong'))
            return 0
    return price

def get_brand(link_soup, city): 
    brand = link_soup.select('td.middle span')[-1].get_text().replace(city, '')
    return brand

def get_res(url, MAX_RETRIES, headers):
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(max_retries=MAX_RETRIES)
    session.mount('https://', adapter)
    session.mount('http://', adapter)

    res = session.get(url, headers=headers)
    
    return res


def scrape_ad(link, headers, sem, sleep=False):
    global all_rows
    
    sem.acquire(blocking=False)

    if np.random.random_sample() < 0.1:
        time.sleep(5)
        
    link_res = get_res(link['href'], max_retries, headers)
    link_soup = bs4.BeautifulSoup(link_res.text, features="lxml")
    row = make_row(link_soup, link)
    all_rows.append(row)
    
    sem.release()
    
    
def scrape_pages(startPage, endPage, max_retries, headers1, headers2, max_threads, ad_sleep=False):
    global n_pages
    global n_cars
    
    scraping_threads = []
    for page in range(startPage, endPage):
        
        url = 'https://www.olx.com.eg/vehicles/cars-for-sale/?page={}'.format(page)
        headers = random.choice([headers1, headers2])
        page_res = get_res(url, max_retries, headers)

        page_soup = bs4.BeautifulSoup(page_res.text, features="lxml")
        page_links = page_soup.select('.ads__item__ad--title')
        
        sem = threading.Semaphore(max_threads)
        for i, link in enumerate(page_links):
            headers = random.choice([headers1, headers2])
            scraping_thread = threading.Thread(target=scrape_ad, args=[link, headers, sem, ad_sleep])
            scraping_threads.append(scraping_thread)
            scraping_thread.start()
            
            n_cars+=1
        n_pages+=1
               
    for i, thread in enumerate(scraping_threads):
        thread.join()