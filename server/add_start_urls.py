from bloom import get_bloom_thread
import os
import  pickle
from pathlib import Path
import uuid

def add_start_urls(include_our_unique=True, include_nepberta_unique=False, include_iriis_unique=False):
    to_crawl_bloom = get_bloom_thread(save_file='to_crawl_bloom_filter.pkl', scalable=True) 
    our_unique_urls = ['https://www.bbc.com/nepali', 'https://kantipurtv.com/', 'https://www.janaboli.com/', 'https://ekagaj.com/', 'https://swasthyakhabar.com/', 'https://deshsanchar.com/', 'https://www.ukaalo.com/', 'https://www.ukeraa.com/', 'https://cijnepal.org.np/', 'https://www.dekhapadhi.com/', 'https://nepaltvonline.com/', 'https://bizmandu.com/', 'https://www.news24nepal.com/', 'https://www.ajakoartha.com/', 'https://nepalipaisa.com/', 'https://www.aakarpost.com/', 'https://aarthiknews.com/', 'https://arthasansar.com/', 'https://himalpress.com/', 'https://newspolar.com', 'https://www.merolifestyle.com/', 'https://www.corporatenepal.com/', 'https://halokhabar.com/', 'https://www.sutranews.com/', 'https://clickmandu.com/', 'https://www.nepalpage.com/', 'https://nepalbahas.com/', 'https://arthadabali.com/', 'https://laganisutra.com/', 'https://neplays.com/', 'https://rashtriyadainik.com/']
    nepberta_unique_urls = ['https://thahakhabar.com/', 'https://abhiyandaily.com/', 'https://lokaantar.com/', 'https://nayapage.com/', 'https://nayapatrikadaily.com/', 'https://dhangadhikhabar.com/', 'https://www.kalakarmi.com/', 'https://dainiknewsnepal.com/']
    iriis_unique_urls = ['https://' + url for url in ['khabarhub.com', 'setokhari.com', 'arghakhanchi.com', 'janaaastha.com', 'nepalijanta.com', 'safalnews.com', 'lokpath.com', 'avenues.tv', 'samudrapari.com', 'arthasarokar.com', 'ujyaaloonline.com', 'kendrabindu.com', 'nepalviews.com', 'purbelinews.com', 'imagekhabar.com', 'pardafas.com', 'daunnenews.com', 'souryaonline.com', 'sansarnews.com', 'sancharkendra.com', 'nayanepalnews.com', 'hamrakura.com', 'mysansar.com', 'rajdhanidaily.com', 'reportersnepal.com', 'canadanepal.com', 'chitawan.com', 'hamrokhelkud.com', 'nepalkhabar.com', 'aajakonews.com', 'bizshala.com', 'shikharnews.com', 'kharibot.com', 'enepalese.com', 'emountaintv.com', 'makalukhabar.com', 'brtnepal.com', 'nepalghatana.com', 'nepalpress.com', 'kakhara.com', 'kathmandupati.com', 'annapurnapost.com', 'diyopost.com', 'capitalnepal.com', 'technologykhabar.com', 'healthpati.com', 'samacharpati.com', 'ekantipur.com', 'pokharanews.com', 'spacesamachar.com', 'onlinekhabar.com', 'hellokhabar.com', 'saralpatrika.com', 'khojtalashonline.com', 'onlinetvnepal.com', 'barnanmedia.com', 'hamrobiratnagar.com', 'mahendranagarpost.com', 'ratopati.com', 'nepalgunjnews.com', 'madheshvani.com', 'nepallive.com', 'nonstopkhabar.com', 'techpana.com', 'nagariknews.com', 'nagariknews.nagariknetwork.com', 'ejanakpuronline.com', 'bhaktapurpost.com', 'eadarsha.com', 'shilapatra.com', 'nepalsamaya.com', 'ictsamachar.com', 'sagarmatha.tv', 'narimag.com.np', 'abcnepal.tv', 'sunaulonepal.com', 'hamrokhotang.com', 'bisalnepal.com', 'nepalihealth.com', 'nepalwatch.com', 'baahrakhari.com', 'kantipath.com', 'etajakhabar.com', 'nepalipatra.com', 'newsofnepal.com', 'himalkhabar.com', 'dcnepal.com', 'gorkhapatraonline.com', 'everestdainik.com', 'setopati.com', 'kathmandupress.com', 'farakdhar.com', 'karobardaily.com', 'palikakhabar.com', 'onlinemajdoor.com', 'dainiknepal.com', 'newskarobar.com', 'pahilopost.com', 'pariwartankhabar.com', 'drishtinews.com']]

    urls_to_crawl = []
    if include_our_unique:
        for url in our_unique_urls:
            if url not in to_crawl_bloom:
                # add to to_crawl_bloom_filter
                to_crawl_bloom.add(url)
                urls_to_crawl.append(url)
    
    if include_nepberta_unique:
        for url in nepberta_unique_urls:
            if url not in to_crawl_bloom:
                # add to to_crawl_bloom_filter
                to_crawl_bloom.add(url)
                urls_to_crawl.append(url)
    
    if include_iriis_unique:
        for url in iriis_unique_urls:
            if url not in to_crawl_bloom:
                # add to to_crawl_bloom_filter
                to_crawl_bloom.add(url)
                urls_to_crawl.append(url)

    
    temp_pickle_path = Path('output/redirect_links/start_urls_temp.pickle')
    pickle_path = Path(f'output/redirect_links/start_urls{uuid.uuid4()}.pickle')

    os.makedirs('output/redirect_links', exist_ok=True)

    # save urls to pickle file
    with temp_pickle_path.open('wb') as rf:
        pickle.dump(urls_to_crawl, rf)

    os.rename(temp_pickle_path, pickle_path)

    # save bloom filter
    to_crawl_bloom.save()

if __name__=="__main__":
    add_start_urls()