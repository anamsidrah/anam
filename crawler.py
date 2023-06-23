import requests
from bs4 import BeautifulSoup
import os

def save_to_file(file_name, content):
    with open(file_name, 'w', encoding='utf-8') as file:
        file.write(content)

# URL of the website you want to scrape
url = "https://www.quarafinance.com/"

# Send HTTP request to the specified URL and save the response from server in a response object called r
r = requests.get(url)

# Create a BeautifulSoup object and specify the parser library at the same time
soup = BeautifulSoup(r.text, 'html.parser')
print(soup)
# English content extraction
english_content = soup.find('div', attrs={'lang': 'en'})
print(english_content)
if english_content:
    save_to_file('english_content.txt', english_content.text)

# Arabic content extraction
arabic_content = soup.find('div', attrs={'lang': 'ar'})
if arabic_content:
    save_to_file('arabic_content.txt', arabic_content.text)
