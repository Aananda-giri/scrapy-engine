import requests
from bs4 import BeautifulSoup

def get_tweets(username, max_tweets=10):
    # Set up the URL to the user's Twitter page
    url = f'https://twitter.com/{username}'
    
    # Send a GET request to fetch the page content
    response = requests.get(url)
    
    # Check if the request was successful
    if response.status_code != 200:
        print(f"Failed to retrieve tweets for {username}. Status code: {response.status_code}")
        return []

    # Parse the page content with BeautifulSoup
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Find all tweet text blocks (you may need to adjust the class name)
    tweets = []
    tweet_blocks = soup.find_all('div', {'data-testid': 'tweet'})

    # Extract tweet text
    for tweet in tweet_blocks[:max_tweets]:
        content = tweet.find('div', {'lang': True})  # Find the div with the tweet text
        if content:
            tweets.append(content.get_text(strip=True))
    
    return tweets

# Example usage
username = "jack"  # Replace with the desired username
tweets = get_tweets(username)

if tweets:
    print(f"Recent tweets from {username}:")
    for i, tweet in enumerate(tweets, 1):
        print(f"{i}. {tweet}")
else:
    print(f"No tweets found for {username}.")
