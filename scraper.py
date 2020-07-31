from tqdm import tqdm_notebook as tqdm
import re, requests, bs4, logging, threading, time, random
import pandas as pd
import numpy as np

from config import *

logging.basicConfig(level=logging.DEBUG, format=' %(asctime)s - %(levelname)s - %(message)s')
logging.disable(logging.DEBUG)

def get_full_ad_info(link_soup):
    """ Returns the full ad infomation in a readable format """
    full_raw_info = link_soup.select('strong a')  # full ad information in soupified html format
    full_processed_info = []
    for info in full_raw_info:
        processed_info = re.sub('\t|\n', '', info.get_text())
        full_processed_info.append(processed_info)
    return full_processed_info

def get_full_ad_info_ids(link_soup):
    """ Returns the ad information identifiers (headers) """
    raw_ids = link_soup.select('th')  # identifiers in soupified html format
    processed_ids = [id.get_text() for id in raw_ids]
    return processed_ids

def get_info_from_id(id, full_info, full_info_ids):
    """ Return a single ad info by specifing it's id counterpart """
    if id in full_info_ids:
        info_idx = full_info_ids.index(id)  # get the id index to return its info counterpart
        return full_info[info_idx]

def get_car_features(full_info, full_info_ids):
    """ Returns a list containig all car features mentioned in the ad """
    car_features = full_info[7:-3]  # the slice of the full info containing the ads

    # since all car features are accounted for with only one id, we need to remove
    # these features and their counterpart id in order to be able to extract 
    # the remaining ad info by using the index counterpart way used in get_info_from_id
    for feature in car_features:
        full_info.remove(feature)
    full_info_ids.remove('إضافات')
    return car_features


def make_ad_dict(link_soup, link):
    """ Returns a dictionary with all the relevant ad info """
    
    ad_dict = dict.fromkeys(['Brand', 'Model', 'Governerate', 'City', 'Date', 'Year', 'Kilometers', 'Pay_type',
                         'Ad_type','Transmission', 'CC', 'Chasis', 'Features', 'Color', 'Price', 'URL'])

    ad_dict['Date'] = get_date(link_soup)
    ad_dict['Price'] = get_price(link_soup)
    ad_dict['City'], ad_dict['Governerate'] = get_ad_location(link_soup)
    ad_dict['Brand'] = get_brand(link_soup, ad_dict['City'])
    ad_dict['URL'] = link['href']

    ad_info = get_full_ad_info(link_soup)
    ad_info_ids = get_full_ad_info_ids(link_soup)

    ad_dict['CC'] = get_info_from_id('المحرك (سي سي)', ad_info, ad_info_ids)
    ad_dict['Year'] = get_info_from_id('السنة', ad_info, ad_info_ids)
    ad_dict['Model'] = get_info_from_id('موديل', ad_info, ad_info_ids)
    ad_dict['State'] = get_info_from_id('الحالة', ad_info, ad_info_ids)
    ad_dict['Pay_type'] = get_info_from_id('طريقة الدفع', ad_info, ad_info_ids)
    ad_dict['Kilometers'] = get_info_from_id('كيلومترات', ad_info, ad_info_ids)
    ad_dict['Transmission'] = get_info_from_id('ناقل الحركة', ad_info, ad_info_ids)

    if 'إضافات' in ad_info_ids:
        ad_dict['Features'] = get_car_features(ad_info, ad_info_ids)

    ad_dict['Color'] = get_info_from_id('اللون', ad_info, ad_info_ids)
    ad_dict['Chasis'] = get_info_from_id('نوع الهيكل', ad_info, ad_info_ids)
    ad_dict['Ad_type'] = get_info_from_id('نوع الإعلان', ad_info, ad_info_ids)

    print("Finished ad dict")

    return ad_dict

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
    global all_ad_dicts
    
    sem.acquire(blocking=False)

    if sleep:
        if np.random.random_sample() < 0.5:
            time.sleep(1)

    link_res = get_res(link['href'], max_retries, headers)
    link_soup = bs4.BeautifulSoup(link_res.text, features="lxml")
    ad_dict = make_ad_dict(link_soup, link)
    all_ad_dicts.append(ad_dict)
    
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


if __name__ == "__main__":
    count = 1
    for batch in range(1, 501, batch_count):
        scrape_pages(batch, batch+batch_count, max_retries, headers1, headers2, 5, True)
        print("  Finished mini-batch {}  ".format(count).center(100, '='))
        count += 1
        
    df = pd.DataFrame(all_ad_dicts)

    df.to_csv('olx_raw.csv', index=False)
    print('The scraped data was succesfully converted into olx_raw.csv')