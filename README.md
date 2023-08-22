# YCrawler

## Description

Parser for news.ycombinator.com. What is done:
- downloads top 30 news
- for each news downloads all links from comments
- hashes links to create filenames and directories names
- saves data to ./hacker_news_data
- works asyncroniously and awaits for new news

## Usage
```bash
python parser.py
```