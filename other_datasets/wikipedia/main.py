import wikipediaapi
import os

# Custom User-Agent
user_agent = 'NepaliWikipediaScraper/1.0 (your_email@example.com)'

# Initialize Wikipedia API for Nepali language with User-Agent
wiki_nepali = wikipediaapi.Wikipedia(
    user_agent=user_agent,
    language='ne',
    extract_format=wikipediaapi.ExtractFormat.WIKI
)

# Directory to save articles
output_dir = "nepali_wikipedia_articles"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Counter for downloaded articles
article_count = 0

# Set to keep track of already downloaded articles
downloaded_articles = set()

# Function to save the article content to a file
def save_article(page):
    global article_count
    if page.exists() and page.title not in downloaded_articles:
        try:
            title = page.title.replace("/", "_")
            content = page.text
            file_path = os.path.join(output_dir, f"{title}.txt")
            
            if content:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(content)
                article_count += 1
                downloaded_articles.add(page.title)
                if article_count%100 == 0:
                    print(f"{article_count}: Saved article: {title}")
                else:print('.', end='')
            else:
                print(f"No content for article: {title}")
        except Exception as e:
            print(f"Error saving article {title}: {e}")
    else:
        if page.title in downloaded_articles:
            # print(f"Article '{page.title}' already downloaded. Skipping.")
            print('*',end='')
            pass
        else:
            print(f"Page '{page.title}' does not exist.")

# Recursive function to download articles from a given category
def fetch_articles_from_category(category_title, level=0, max_level=5):  # Increase max_level to 5
    category_page = wiki_nepali.page(category_title)
    
    if category_page.exists():
        print(f"Fetching category: {category_title}")
        categorymembers = category_page.categorymembers

        for member in categorymembers.values():
            if member.ns == wikipediaapi.Namespace.CATEGORY and level < max_level:
                # Recurse into subcategories
                fetch_articles_from_category(member.title, level + 1, max_level)
            elif member.ns == wikipediaapi.Namespace.MAIN:
                # Save the article
                save_article(member)
    else:
        print(f"Category '{category_title}' does not exist.")


'''
Categories listed in: https://ne.wikipedia.org/wiki/%E0%A4%B5%E0%A4%BF%E0%A4%95%E0%A4%BF%E0%A4%AA%E0%A4%BF%E0%A4%A1%E0%A4%BF%E0%A4%AF%E0%A4%BE:%E0%A4%B6%E0%A5%8D%E0%A4%B0%E0%A5%87%E0%A4%A3%E0%A5%80
'''
# Try fetching articles from multiple categories
categories = [
    "श्रेणी:नेपाल",            # Nepal category
    "श्रेणी:नेपालका जिल्ला",   # Districts of Nepal
    "श्रेणी:व्यक्तिहरू",        # People
    "श्रेणी:इतिहास",    # History
    "श्रेणी:गणित",      # Mathematics
    "श्रेणी:जीवनी",      # Biographies
    "श्रेणी:पकवान",      # Cuisine
    "श्रेणी:प्रविधि",     # Technology
    "श्रेणी:भूगोल",      # Geography
    "श्रेणी:विज्ञान",     # Science
    "श्रेणी:समाज",       # Society
    "श्रेणी:संस्कृति",    # Culture
    "श्रेणी:शिक्षा"       # Education
]


for category in categories:
    fetch_articles_from_category(category, max_level=5)

print(f"Article download completed. Total unique articles downloaded: {article_count}")

import os
print(f"Number of articles downloaded: {len(os.listdir('nepali_wikipedia_articles'))}")
# 19679