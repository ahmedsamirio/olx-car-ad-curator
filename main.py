from functions import *
from config import batch_count

headers1 = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36',
    }
headers2 =  {"User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.119 Safari/537.36"}

count = 1
for batch in range(1, 501, batch_count):
    scrape_pages(batch, batch+batch_count, max_retries, headers1, headers2, 5, True)
    print("  Finished mini-batch {}  ".format(count).center(100, '='))
    count += 1
    
df = pd.DataFrame(all_rows)

df.to_csv('olx_raw.csv', index=False)
print('The scraped data was succesfully converted into olx_raw.csv')