from bloom import get_bloom_thread
import os
import  pickle
from pathlib import Path
to_crawl_bloom = get_bloom_thread(save_file='to_crawl_bloom_filter.pkl', scalable=True) 

our_unique = ['https://www.bbc.com/nepali', 'https://kantipurtv.com/', 'https://www.janaboli.com/', 'https://ekagaj.com/', 'https://swasthyakhabar.com/', 'https://deshsanchar.com/', 'https://www.ukaalo.com/', 'https://www.ukeraa.com/', 'https://cijnepal.org.np/', 'https://www.dekhapadhi.com/', 'https://nepaltvonline.com/', 'https://bizmandu.com/', 'https://www.news24nepal.com/', 'https://www.ajakoartha.com/', 'https://nepalipaisa.com/', 'https://www.aakarpost.com/', 'https://aarthiknews.com/', 'https://arthasansar.com/', 'https://himalpress.com/', 'https://newspolar.com', 'https://www.merolifestyle.com/', 'https://www.corporatenepal.com/', 'https://halokhabar.com/', 'https://www.sutranews.com/', 'https://clickmandu.com/', 'https://www.nepalpage.com/', 'https://nepalbahas.com/', 'https://arthadabali.com/', 'https://laganisutra.com/', 'https://neplays.com/', 'https://rashtriyadainik.com/']
nepberta_unique = ['https://thahakhabar.com/', 'https://abhiyandaily.com/', 'https://lokaantar.com/', 'https://nayapage.com/', 'https://nayapatrikadaily.com/', 'https://dhangadhikhabar.com/', 'https://www.kalakarmi.com/', 'https://dainiknewsnepal.com/']
iriis_unique = ['https://' + url for url in ['khabarhub.com', 'setokhari.com', 'arghakhanchi.com', 'janaaastha.com', 'nepalijanta.com', 'safalnews.com', 'lokpath.com', 'avenues.tv', 'samudrapari.com', 'arthasarokar.com', 'ujyaaloonline.com', 'kendrabindu.com', 'nepalviews.com', 'purbelinews.com', 'imagekhabar.com', 'pardafas.com', 'daunnenews.com', 'souryaonline.com', 'sansarnews.com', 'sancharkendra.com', 'nayanepalnews.com', 'hamrakura.com', 'mysansar.com', 'rajdhanidaily.com', 'reportersnepal.com', 'canadanepal.com', 'chitawan.com', 'hamrokhelkud.com', 'nepalkhabar.com', 'aajakonews.com', 'bizshala.com', 'shikharnews.com', 'kharibot.com', 'enepalese.com', 'emountaintv.com', 'makalukhabar.com', 'brtnepal.com', 'nepalghatana.com', 'nepalpress.com', 'kakhara.com', 'kathmandupati.com', 'annapurnapost.com', 'diyopost.com', 'capitalnepal.com', 'technologykhabar.com', 'healthpati.com', 'samacharpati.com', 'ekantipur.com', 'pokharanews.com', 'spacesamachar.com', 'onlinekhabar.com', 'hellokhabar.com', 'saralpatrika.com', 'khojtalashonline.com', 'onlinetvnepal.com', 'barnanmedia.com', 'hamrobiratnagar.com', 'mahendranagarpost.com', 'ratopati.com', 'nepalgunjnews.com', 'madheshvani.com', 'nepallive.com', 'nonstopkhabar.com', 'techpana.com', 'nagariknews.com', 'nagariknews.nagariknetwork.com', 'ejanakpuronline.com', 'bhaktapurpost.com', 'eadarsha.com', 'shilapatra.com', 'nepalsamaya.com', 'ictsamachar.com', 'sagarmatha.tv', 'narimag.com.np', 'abcnepal.tv', 'sunaulonepal.com', 'hamrokhotang.com', 'bisalnepal.com', 'nepalihealth.com', 'nepalwatch.com', 'baahrakhari.com', 'kantipath.com', 'etajakhabar.com', 'nepalipatra.com', 'newsofnepal.com', 'himalkhabar.com', 'dcnepal.com', 'gorkhapatraonline.com', 'everestdainik.com', 'setopati.com', 'kathmandupress.com', 'farakdhar.com', 'karobardaily.com', 'palikakhabar.com', 'onlinemajdoor.com', 'dainiknepal.com', 'newskarobar.com', 'pahilopost.com', 'pariwartankhabar.com', 'drishtinews.com']]

# add to to_crawl_bloom_filter
[to_crawl_bloom.add(url) for url in our_unique_urls]

temp_pickle_path = Path('output/redirect_links/our_unique_temp.pickle')
pickle_path = Path('output/redirect_links/our_unique.pickle')
urls_to_crawl = our_unique

os.makedirs('output/redirect_links', exist_ok=True)

'''
# Todo

# Stage-2
temp_pickle_path = 'output/redirect_links/nepberta_unique_temp.pickle'
pickle_path = 'output/redirect_links/nepberta_unique.pickle'
urls_to_crawl = nepberta_unique

# Stage-3
temp_pickle_path = 'output/redirect_links/iriis_unique_temp.pickle'
pickle_path = 'output/redirect_links/iriis_unique.pickle'
urls_to_crawl = iriis_unique
'''

# save bloom filter
to_crawl_bloom.save()

# save urls to pickle file
with temp_pickle_path.open('wb') as rf:
    pickle.dump(urls_to_crawl, rf)

os.rename(temp_pickle_path, pickle_path)
