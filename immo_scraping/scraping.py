from __future__ import print_function
from __future__ import unicode_literals

import os
import sys
import asyncio
import aiohttp
import getpass
import requests
import collections

import pandas as pd

from enum import IntEnum
from bs4 import BeautifulSoup


# https://github.com/pasnox/housing/blob/master/SeLoger.com.api.txt
WS_SE_LOGER_BASE = "http://ws.seloger.com/baseurl/annonceDetail.xml?idAnnonce=104313655"
WS_SE_LOGER_ANNONCE = "http://ws.seloger.com/baseurl/annonceDetail.xml?idAnnonce={}"
WS_SE_LOGER_SEARCH = "http://ws.seloger.com/baseurl/search.xml"

URL_SE_LOGER_SEARCH = "http://www.seloger.com/list.htm?ci=750116&idtt=2&idtypebien=1"
URL_SE_LOGER_SEARCH_WITH_PAGE = "http://www.seloger.com/list.htm?ci=750118&idtt=2&idtypebien=1&LISTING-LISTpg={page}"
URL_SE_LOGER_AD = "http://www.seloger.com/annonces/achat/appartement/paris-18eme-75/la-chapelle-marx-dormoy/106474505.htm?cp=75018&idtt=2&org=advanced_search&bd=Li_LienAnn_1"

PATH_EXPORT_FOLDER = "/Users/edouard/Downloads"

PROXIES = {
  'http': 'http://{}:{}@proxy.int.world.socgen:8080',
  'https': 'http://{}:{}@proxy.int.world.socgen:8080',
}


class TypeBien(IntEnum):
    Appartement = 1
    MaisonVilla = 2
    ParkingBox = 3
    Terrain = 4
    Boutique = 6
    LocalCommercial = 7
    Bureaux = 8
    LoftAtelierSurface = 9
    Immeuble = 11
    Betiment = 12
    Chateau = 13
    HotelParticulier = 14
    Programme = 15


class TypeRecherche(IntEnum):
    Location = 1
    Achat = 2


def build_article_json(article_page_content):
    soup = BeautifulSoup(article_page_content)
    print(soup)
    return {
        'title': ' '.join(soup.find('h1', 'detail-title').stripped_strings)
    }


async def fetch_one_article(article_url):
    article_response = await aiohttp.request('GET', article_url)
#    if article_response.
    return build_article_json(article_response.read())


async def build_flat_list(search_page_content):
    soup = BeautifulSoup(search_page_content, 'html.parser')
    articles_url = map(lambda article: article.a['href'], soup.body.find_all('article', 'listing'))
    artciles_json = await fetch_one_article


def build_search_pages_list():
    search_pages_list = []
    i = 1

    print("Fetching page {}".format(i))
    current_page = requests.get(URL_SE_LOGER_SEARCH_WITH_PAGE.format(page=i))

    while current_page.status_code == 200 and i <= 1:
        search_pages_list.append(current_page)
        i += 1
        print("Fetching page {}".format(i))
        current_page = requests.get(URL_SE_LOGER_SEARCH_WITH_PAGE.format(page=i))

    return search_pages_list


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


async def build_one_annonce_dic(annonce_tag):
    return {item.name: item.string if item.string is not None else item.contents for item in annonce_tag.find_all()}


async def build_annonces_list(annonces_tag):
    res = []
    for annonce_tag in annonces_tag.find_all('annonce'):
        res.append(await build_one_annonce_dic(annonce_tag))
    return res


def build_url(**kwargs):
    return "{}?{}".format(WS_SE_LOGER_SEARCH, '&'.join("{}={}".format(k, ','.join(map(str, v)) if isinstance(v, collections.Iterable) else v) for k, v in kwargs.items()))


async def dump_annonces(**kwargs):
    res = []
    url = build_url(**kwargs)
    while url is not None:
        print(url)
        async with aiohttp.get(url) as response:
            assert response.status == 200
            soup = BeautifulSoup(await response.text(), 'lxml')
            # TODO: use page_max to generate the urls
            page_max = soup.pagemax.string if soup.pagemax else None
            if soup.annonces is not None:
                res.extend(await build_annonces_list(soup.annonces))
            url = soup.pagesuivante.string if soup.pagesuivante else None
    annonces_df = pd.DataFrame(res)
    if 'ci' in kwargs:
        export_path = os.path.join(PATH_EXPORT_FOLDER, kwargs['ci'] if not isinstance(kwargs['ci'], collections.Iterable) else '_'.join(kwargs['ci']), '.xlsx')
        annonces_df.to_excel(export_path)


def main():
    # if os.environ.get('USE_PROXY', False) == 'True':
    #     se_loger = requests.get(URL_SE_LOGER_SEARCH, proxies=get_proxies())
    # else:
    #     se_loger = requests.get(URL_SE_LOGER_SEARCH)

    # search_pages_list = build_search_pages_list()

    loop = asyncio.get_event_loop()
    tasks = [
        dump_annonces(idtt=TypeRecherche.Achat, ci=750118, idtypebien=TypeBien.Appartement, pxmin=1000000, pxmax=1200000),
        # dump_annonces(idtt=2, ci=750118, idtypebien=1, pxmin=1000000, pxmax=1500000),
    ]
    loop.run_until_complete(asyncio.wait(tasks))
    loop.close()


if __name__ == '__main__':
    main()
