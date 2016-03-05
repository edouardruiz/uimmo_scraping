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
            nb_trouvees = soup.nbtrouvees.string if soup.nbtrouvees else None
            if nb_trouvees and int(nb_trouvees) >= 1:
                # TODO: use page_max to generate the next urls after the first query
                page_max = soup.pagemax.string if soup.pagemax else None
                if soup.annonces is not None:
                    res.extend(await build_annonces_list(soup.annonces))
            url = soup.pagesuivante.string if soup.pagesuivante else None
    return (kwargs['ci'] if 'ci' in kwargs else 0, pd.DataFrame(res))


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
        annonces_df = pd.concat([t[1] for t in tuples_group], axis=0, ignore_index=True)
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

    insee_codes = get_insee_codes([75, 77, 78, 91, 92, 93, 94, 95], os.path.join(os.path.dirname(__file__), '../input/correspondances-code-insee-code-postal.json'))
    # TODO: remove test values
    # insee_codes = [750101, 750111]
    nb_rooms = range(1, 25)

    loop = asyncio.get_event_loop()
    with aiohttp.ClientSession(loop=loop, connector=conn) as session:
        def dump_annonces_ci(t):
            insee_code, nb_room = t
            return dump_annonces(session, ci=insee_code, nb_pieces=nb_room, idtt=TypeRecherche.Achat, idtypebien=TypeBien.Appartement)
        tasks = map(dump_annonces_ci, itertools.product(insee_codes, nb_rooms))
        tasks_done, _ = loop.run_until_complete(asyncio.wait(tasks))
        annonces_df_dic = print_ci_annonces_df_tuples(tasks_done)
        print("annonces_df_dic contains {} df".format(len(annonces_df_dic)))
    loop.close()


if __name__ == '__main__':
    main()
