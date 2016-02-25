import os
import requests

from bs4 import BeautifulSoup


URL_SE_LOGER = r"http://www.seloger.com/list.htm?ci=750116&idtt=2"

proxies = {
  'http': 'http://edouard.ruiz:hedv1084@proxy.int.world.socgen:8080',
  'https': 'http://edouard.ruiz:hedv1084@proxy.int.world.socgen:8080',
}


def main():
    if os.environ.get('USE_PROXY', True):
        se_loger = requests.get(URL_SE_LOGER, proxies=proxies)
    else:
        se_loger = requests.get(URL_SE_LOGER)


if __name__ == '__main__':
    main()
