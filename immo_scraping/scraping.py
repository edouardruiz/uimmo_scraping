import os
import sys
import getpass
import requests

from bs4 import BeautifulSoup


URL_SE_LOGER_SEARCH = r"http://www.seloger.com/list.htm?ci=750116&idtt=2&idtypebien=1"
URL_SE_LOGER_AD = r"http://www.seloger.com/annonces/achat/appartement/paris-18eme-75/la-chapelle-marx-dormoy/106474505.htm?cp=75018&idtt=2&org=advanced_search&bd=Li_LienAnn_1"

PROXIES = {
  'http': 'http://{}:{}@proxy.int.world.socgen:8080',
  'https': 'http://{}:{}@proxy.int.world.socgen:8080',
}


def build_article_json(article):
    return {
        'title': ''.join(article.h2.a.strings),
        'price': ''.join(article.find('div', 'price').a.strings),
        'link': article.a['href']
    }


def build_flat_list(soup):
    return list(map(build_article_json, soup.body.find_all('article')))


def get_proxies():
    if sys.stdin.isatty():
        l = input('Login: ')
        p = getpass.getpass()
    else:
        l = sys.stdin.readline().rstrip()
        p = sys.stdin.readline().rstrip()

    PROXIES['http'] = PROXIES['http'].format(l, p)
    PROXIES['https'] = PROXIES['https'].format(l, p)

    return PROXIES


def main():
    if os.environ.get('USE_PROXY', False) == 'True':
        se_loger = requests.get(URL_SE_LOGER_SEARCH, proxies=get_proxies())
    else:
        se_loger = requests.get(URL_SE_LOGER_SEARCH)

    soup = BeautifulSoup(se_loger.content, 'html.parser')
    articles = build_flat_list(soup)

    print(articles)


if __name__ == '__main__':
    get_proxies()
    # main()
