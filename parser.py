import asyncio
import hashlib
import logging
import os
import re

import aiofiles
import aiohttp

from urllib.parse import urljoin

from bs4 import BeautifulSoup


URL = 'https://news.ycombinator.com'
ROOT_DOMAIN_URL = 'ycombinator.com'
DATA_DIR = './hacker_news_data'
NEWS_NUMBER = 30
TIMEOUT = 5


async def fetch_url(session, url, timeout=None):
    async with session.get(url, timeout=timeout) as response:
        logging.info(f'Fetching url {url}')
        return await response.text()


async def save_data(filename, data):
    async with aiofiles.open(filename, 'w') as file_handler:
        logging.info(f"Starting to save data in file with filename {filename}")
        await file_handler.write(data)


def get_filename_from_url(url):
    logging.info(f'Hashing url {url}')
    url_hash = hashlib.md5(url.encode()).hexdigest()
    return "/" + f"{url_hash}.html"


def create_dir_for_data(path):
    if not os.path.exists(path):
        os.mkdir(path)
        logging.info(f"Directory {path} created.")
        return True
    return False


def get_absolute_url(base_url, link):
    if link.startswith('http'):
        return link
    return urljoin(base_url, link)


def check_link_is_external(url):
    return 'https' == url[:5] and ROOT_DOMAIN_URL not in url


async def get_external_link(session, url):
    try:
        html_content = await fetch_url(session, url, TIMEOUT)
        soup = BeautifulSoup(html_content, 'html.parser')
        links = map(lambda a_tag: a_tag['href'], soup.findAll('a'))
        return list(filter(check_link_is_external, links))
    except (aiohttp.ClientConnectorError, aiohttp.ClientResponseError, asyncio.Timeout) as err:
        logging.warning(f"No response from link {url}")
        return []


async def find_news(session, url):
    news = []
    try:
        html_content = await fetch_url(session, url, TIMEOUT)
        soup = BeautifulSoup(html_content, 'html.parser')
        news_headers = soup.findAll('tr', class_='athing')

        for tr in news_headers:
            comments = tr.findNextSibling('tr')
            comments_link = comments.find('a', string=re.compile('comment(s|)$'))
            news_link = tr.find('span', class_='titleline')
            news_link = news_link.a if news_link else None
            news_link = news_link['href'] if news_link else None
            news_link = get_absolute_url(URL, news_link)

            news_block = {
                'news': news_link,
                'comment': get_absolute_url(URL, comments_link['href']) if comments_link else None
            }
            news.append(news_block)
        return news
    except (asyncio.Timeout, aiohttp.ClientConnectorError, aiohttp.ClientResponseError) as err:
        logging.exception(f"Exception in response from {url}")
        return []


async def parse(loop, session):
    news_found = await find_news(session, URL)
    if news_found:
        news_found = news_found[:NEWS_NUMBER]

        for news in news_found:
            link = news['news']
            if link:
                filename = get_filename_from_url(link)
                filedir = DATA_DIR + filename[:-5]
                dir = create_dir_for_data(filedir)
                if not dir:
                    logging.warning(f"URL {link} has been already downloaded!")
                else:
                    news_data = await fetch_url(session, link)
                    await save_data(filedir + filename, data=news_data)
                    comment_link = news['comment']
                    download_comment_tasks = []
                    if comment_link:
                        comment_links = await get_external_link(session, comment_link)
                        if comment_links:
                            for comm_link in comment_links:
                                comment_dir = filedir + '/comments'
                                create_dir_for_data(comment_dir)
                                comment_file_name = comment_dir + get_filename_from_url(comm_link)
                                comment_data = await fetch_url(session, comm_link)
                                task = loop.create_task(save_data(comment_file_name, comment_data))
                                download_comment_tasks.append(task)
                    await asyncio.gather(*download_comment_tasks)
    else:
        raise ValueError


async def parsing_loop(loop, session, timeout):
    while True:
        await parse(loop, session)
        await asyncio.sleep(timeout)


async def start_parse(timeout):
    loop = asyncio.get_event_loop()
    async with aiohttp.ClientSession(loop=loop) as session:
        await parsing_loop(loop, session, timeout)
    loop.close()


if __name__ == '__main__':
    create_dir_for_data(DATA_DIR)
    try:
        asyncio.run(start_parse(TIMEOUT))
    except KeyboardInterrupt:
        logging.info("Exit by keyboard interrupt")
        exit(1)
    except ValueError:
        logging.warning("No news found")
        exit(1)
