import timeit, time
from functions import *

t = timeit.Timer('scrape_pages(1, 1+batch, max_retries, headers1, headers2, True, True)',
                 setup='from __main__ import scrape_pages, batch, max_retries, headers1, headers2')

print("%s: %10s" % ("N-batches", "Time in hours"))
for batch in [1, 5, 10]:
    sec = t.timeit(number=1)
    total_time = (sec * (500 / batch)) / 3600
    print("%d: %10.5f hour" % (batch, total_time))