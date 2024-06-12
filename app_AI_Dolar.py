## 1. generacion de scraping
import aiohttp
import asyncio
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime

def format_date(date_str):
    date_obj = datetime.fromisoformat(date_str)
    return date_obj.strftime('%d %b. %Y - %I:%M %p')

async def fetch(session, url):
    async with session.get(url) as response:
        return await response.text()

async def get_news_info_valora(session, url):
    html_text = await fetch(session, url)
    soup = BeautifulSoup(html_text, 'html.parser')
    news_modules = soup.find_all('div', class_='td_module_19 td_module_wrap td-animation-stack')
    news_titles = []
    news_urls = []
    news_dates = []
    first_date = None

    for module in news_modules:
        title_tag = module.find('h3', class_='entry-title td-module-title').find('a')
        date_tag = module.find('time', class_='entry-date updated td-module-date')

        if title_tag and date_tag:
            title = title_tag.text.strip()
            href = title_tag['href']
            date = date_tag['datetime']

            news_titles.append(title)
            news_urls.append(href)
            if not first_date:
                first_date = date
            news_dates.append(date)

    if first_date:
        news_dates.pop(0)
        news_dates.append(first_date)

    return news_titles, news_urls, news_dates

async def scrape_multiple_pages(base_url, num_pages):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for page in range(1, num_pages + 1):
            url = f"{base_url}/page/{page}/" if page > 1 else base_url
            tasks.append(get_news_info_valora(session, url))
        
        results = await asyncio.gather(*tasks)
        
        all_titles = []
        all_urls = []
        all_dates = []
        
        for titles, urls, dates in results:
            all_titles.extend(titles)
            all_urls.extend(urls)
            all_dates.extend(dates)

    return all_titles, all_urls, all_dates

base_url = 'https://www.valoraanalitik.com/noticias-del-mercado-financiero'
num_pages = 5

news_titles, news_urls, news_dates = asyncio.run(scrape_multiple_pages(base_url, num_pages))

# Combinar noticias con el mismo enlace web
def combine_duplicate_urls(news_titles, news_urls, news_dates):
    unique_urls = []
    unique_titles = []
    unique_dates = []

    for title, url, date in zip(news_titles, news_urls, news_dates):
        if url not in unique_urls:
            unique_urls.append(url)
            unique_titles.append(title)
            unique_dates.append(date)
        else:
            index = unique_urls.index(url)
            unique_titles[index] += f"\n{title}"
            unique_dates[index] += f"\n{date}"

    return unique_titles, unique_urls, unique_dates

news_titles, news_urls, news_dates = combine_duplicate_urls(news_titles, news_urls, news_dates)
formatted_dates = [format_date(date.split('\n')[0]) for date in news_dates]

df = pd.DataFrame({'Fecha y Hora': formatted_dates,
                   'Titular': news_titles,
                   'Web': news_urls})

df = df[['Fecha y Hora', 'Titular', 'Web']]
print("\nDataFrame de noticias en el portal Valora Analitik:\n")
print(df)

csv_file = 'valora_news.csv'
df.to_csv(csv_file, index=False)
print(f"\nEl DataFrame ha sido guardado en {csv_file}.")
