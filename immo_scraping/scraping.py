from __future__ import print_function
from __future__ import unicode_literals

import os
import sys
import json
import asyncio
import aiohttp
import getpass
import itertools
import collections

import pandas as pd

from functools import partial

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

LIMIT_CONNECTIONS = 40


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
        async with session.get(url) as response:
            assert response.status == 200
            soup = BeautifulSoup(await response.text(), 'lxml')
            nb_found = soup.nbtrouvees.string if soup.nbtrouvees else None
            if nb_found and int(nb_found) >= 1:
                if int(nb_found) > 200:
                    print("Warning: can't fetch all items for {}".format(url))
                # TODO: use page_max to generate the next urls after the first query
                page_max = soup.pagemax.string if soup.pagemax else None
                page_cur = soup.pagecourante.string if soup.pagecourante else None
                if page_cur:
                    nb_found_curr = int(nb_found) - (int(page_cur) - 1) * 50 if page_cur == page_max else 50
                else:
                    nb_found_curr = int(nb_found)
                print("Info: {} items found for url {}".format(nb_found_curr, url))
                if soup.annonces is not None:
                    res.extend(await build_annonces_list(soup.annonces))
            url = soup.pagesuivante.string if soup.pagesuivante else None
    return kwargs['ci'] if 'ci' in kwargs else 0, res


def get_insee_codes(departements, insee_path):
    """Get the INSEE codes from a JSON file that are in a department.

    The JSON file: http://data.iledefrance.fr/explore/dataset/correspondances-code-insee-code-postal/download?format=json
    :param departements: list of departements
    :type departements: list of int
    :param insee_path: JSON file
    :type insee_path: str
    :return: list of INSEE codes
    """
    def convert_insee_code_to_ci(insee_code):
        str_insee_code = str(insee_code)
        return int(str_insee_code[:2] + '0' + str_insee_code[2:])

    with open(insee_path) as f:
        insee_json = json.load(f)
        res = [f['fields']['insee_com'] for f in filter(lambda d: d['fields']['code_dept'] in departements, insee_json)]
        return map(convert_insee_code_to_ci, res)


def print_ci_annonces_df_tuples(tasks_done, path_export_folder=PATH_EXPORT_FOLDER):
    def tuple_key(t):
        return t[0]
    res = {}
    tuples_results = [t.result() for t in tasks_done]
    tuples_results = sorted(tuples_results, key=tuple_key)
    for ci, tuples_group in itertools.groupby(tuples_results, tuple_key):
        # annonces_df = pd.concat([t[1] for t in tuples_group], axis=0, ignore_index=True)
        annonces_df = pd.DataFrame([sub_list for t in tuples_group for sub_list in t[1]])
        if not annonces_df.empty:
            annonces_df.to_excel(os.path.join(path_export_folder, "{}.xlsx".format(str(ci))))
            # annonces_df.to_pickle(os.path.join(path_export_folder, "{}.pkl".format(str(ci))))
            res[ci] = annonces_df
    return res


def main():
    if os.environ.get('USE_PROXY', False) == 'True':
        conn = aiohttp.ProxyConnector(proxy=get_proxies()['http'], limit=LIMIT_CONNECTIONS)
    else:
        conn = aiohttp.TCPConnector(limit=LIMIT_CONNECTIONS)

    # insee_codes = get_insee_codes([75, 77, 78, 91, 92, 93, 94, 95], os.path.join(os.path.dirname(__file__), '../input/correspondances-code-insee-code-postal.json'))
    insee_codes = get_insee_codes([75], os.path.join(os.path.dirname(__file__), '../input/correspondances-code-insee-code-postal.json'))
    # TODO: remove test values
    # prices = list(range(0, 4000000, 100000))
    # min_max_prices = [prices[i:i+2] for i in range(len(prices))]
    # insee_codes = [750118]
    min_max_prices = [
        [0, 100000],
        [100000, 150000],
        [150000, 200000],
        [200000, 250000],
        [250000, 300000],
        [300000, 350000],
        [350000, 400000],
        [400000, 450000],
        [450000, 500000],
        [500000, 550000],
        [550000, 600000],
        [600000, 650000],
        [650000, 700000],
        [700000, 800000],
        [800000, 900000],
        [900000, 1000000],
        [1000000, 1100000],
        [1100000, 1200000],
        [1200000, 1300000],
        [1300000, 1400000],
        [1400000, 1500000],
        [1500000, 1600000],
        [1600000, 1700000],
        [1700000, 1800000],
        [1800000, 1900000],
        [1900000, 2000000],
        [2000000, 2200000],
        [2200000, 2400000],
        [2400000, 2600000],
        [2600000, 2800000],
        [2800000, 3000000],
        [3000000, 3400000],
        [3400000, 3800000],
        [3800000, 4200000],
        [4200000, 4600000],
        [4600000, 5000000],
        [5000000]
    ]

    loop = asyncio.get_event_loop()
    with aiohttp.ClientSession(loop=loop, connector=conn) as session:
        def dump_annonces_ci(t):
            insee_code, min_max = t
            if len(min_max) > 1:
                return dump_annonces(session, ci=insee_code, pxmin=min_max[0], pxmax=min_max[1], idtt=TypeRecherche.Achat, idtypebien=TypeBien.Appartement)
            else:
                return dump_annonces(session, ci=insee_code, pxmin=min_max[0], idtt=TypeRecherche.Achat, idtypebien=TypeBien.Appartement)
        tasks = map(dump_annonces_ci, itertools.product(insee_codes, min_max_prices))
        tasks_done, _ = loop.run_until_complete(asyncio.wait(tasks))
        annonces_df_dic = print_ci_annonces_df_tuples(tasks_done)
        print("annonces_df_dic contains {} df".format(len(annonces_df_dic)))
        full_df = pd.concat(annonces_df_dic.values(), axis=0, ignore_index=True)
        full_df.to_excel(os.path.join(PATH_EXPORT_FOLDER, "full_export.xlsx"))
    loop.close()


if __name__ == '__main__':
    main()
