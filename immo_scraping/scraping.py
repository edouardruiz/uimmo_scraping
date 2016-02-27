from __future__ import print_function
from __future__ import unicode_literals

import os
import sys
import asyncio
import aiohttp
import getpass
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

PATH_EXPORT_FOLDER = "/Users/edouard/Documents/Projets/python/immo_scraping/output"

PROXIES = {
  'http': 'http://{}:{}@proxy.int.world.socgen:8080',
  'https': 'http://{}:{}@proxy.int.world.socgen:8080',
}

LIMIT_CONNECTIONS = 10

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
    return {item.name: item.string if item.string is not None else (item.contents if item.contents else '') for item in annonce_tag.find_all()}


async def build_annonces_list(annonces_tag):
    res = []
    for annonce_tag in annonces_tag.find_all('annonce'):
        res.append(await build_one_annonce_dic(annonce_tag))
    return res


def build_url(**kwargs):
    return "{}?{}".format(WS_SE_LOGER_SEARCH, '&'.join("{}={}".format(k, ','.join(map(str, v)) if isinstance(v, collections.Iterable) else v) for k, v in kwargs.items()))


async def dump_annonces(session, **kwargs):
    res = []
    url = build_url(**kwargs)
    while url is not None:
        print(url)
        async with session.get(url) as response:
            assert response.status == 200
            soup = BeautifulSoup(await response.text(), 'lxml')
            # TODO: use page_max to generate the next urls after the first query
            page_max = soup.pagemax.string if soup.pagemax else None
            if soup.annonces is not None:
                res.extend(await build_annonces_list(soup.annonces))
            url = soup.pagesuivante.string if soup.pagesuivante else None
    annonces_df = pd.DataFrame(res)
    if 'ci' in kwargs:
        file_name = str(kwargs['ci']) if not isinstance(kwargs['ci'], collections.Iterable) else '_'.join(kwargs['ci'])
        annonces_df.to_excel(os.path.join(PATH_EXPORT_FOLDER, "{}.xlsx".format(file_name)))
        annonces_df.to_pickle(os.path.join(PATH_EXPORT_FOLDER, "{}.pkl".format(file_name)))
    return annonces_df


def main():
    if os.environ.get('USE_PROXY', False) == 'True':
        conn = aiohttp.ProxyConnector(proxy=get_proxies()['http'], limit=LIMIT_CONNECTIONS)
    else:
        conn = aiohttp.TCPConnector(limit=LIMIT_CONNECTIONS)

    loop = asyncio.get_event_loop()
    with aiohttp.ClientSession(loop=loop, connector=conn) as session:
        tasks = [
            dump_annonces(session, idtt=TypeRecherche.Achat, ci=750118, idtypebien=TypeBien.Appartement, pxmin=1000000, pxmax=1200000),
            dump_annonces(session, idtt=TypeRecherche.Achat, ci=750119, idtypebien=TypeBien.Appartement, pxmin=1000000, pxmax=1200000),
        ]
        loop.run_until_complete(asyncio.wait(tasks))
    loop.close()


if __name__ == '__main__':
    main()
