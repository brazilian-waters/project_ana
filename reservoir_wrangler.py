""" Module to scrape reservoir data from National Water Agency of Brazil (ANA  
for short in Portuguese).
The data is available on the Reservoir Follow-up System (SAR for short in 
Portuguese), available on this URL: https://www.ana.gov.br/sar0/Home.
Data is grouped on three different systems, as follows: 'SIN', 
'Nordeste e Semiárido' e 'Sistema Cantareira'.
"""

import pandas as pd
import requests
import datetime
from bs4 import BeautifulSoup
from collections import namedtuple


def scrape_systems(URL = r"https://www.ana.gov.br"):
    """Scrape all available systems and its URLs.

    Args:
    url: string. The URL which contains all systems.

    Return:
    systems: list of named tuples holding the systems and its URLs.
    """    
    page = requests.get(URL + r'/sar0/Home', verify=False)
    soup = BeautifulSoup(page.text, 'html.parser')
    elements = soup.find_all(string="Dados Históricos")

    systems = list()
    SystemNamedTuple = namedtuple("system", ["name", "url"])
    for elm in elements:
        url = URL + elm.parent.parent.parent.find('a', href=True)['href']
        name = elm.parent.parent.parent.parent.parent.find_next('strong').text
        systems.append(SystemNamedTuple(name, url))
    return systems

def scrape_reservoirs(systems):
    """Scrape the names and codes of all reservoirs, of all systems.

    Args:
    systems: list of named tuples holding the systems names and its URLs.

    Returns:
    df: Pandas DataFrame holding reservoirs codes and names of all systems.
    """
    temp = [None] * len(systems) # List of all systems data frames
    for i, sys in enumerate(systems):
        page = requests.get(sys.url, verify=False)
        soup = BeautifulSoup(page.text, 'html.parser')
        element = soup.find('select', 
                            class_="form-control input-m-sm", 
                            id="dropDownListReservatorios")
        reservoirs = element.find_all_next("option")[1:]

        names = [None] * len(reservoirs)
        codes = [None] * len(reservoirs)
        for j, res in enumerate(reservoirs):            
            names[j] = " ".join(res.text.split())           
            codes[j] = int(res['value'])
                    
        df = pd.DataFrame({"res_code": codes,
                           "res_name": names,
                           "system_id": [i] * len(reservoirs)})
        temp[i] = df    
    return pd.concat(temp, ignore_index=True)

def scrape_history(df_systems, df_reservoirs):
    """"Scrape all available data of all reservoirs
    
    Args:
    df_systems: Pandas DataFrame holding the systems names and its URLs.
    df_reservoirs: Pandas DataFrame holding reservoirs codes and names of all 
    systems.

    Returns:
    df: Pandas Dataframe holding all data of all reservoirs.
    """
    now = datetime.datetime.now()
#    df_tmp = df_reservoirs.merge(df_systems,
#                                 how='left',
#                                 on='system_id')

#    df_tmp.url = df_tmp.url + r"?dropDownListReservatorios=" + df_tmp.res_code
#    df_tmp.url = df_tmp.url + r"&dataInicial=22%2F04%2F1500"
#    df_tmp.url = df_tmp.url + r"&dataFinal={:2d}%2F{:2d}%2F{:2d}"\
#                .format(now.day, now.month, now.year)
    df = None
    return df

if __name__ == '__main__':
    systems = scrape_systems()
    df_reservoirs = scrape_reservoirs(systems)
    print(df_reservoirs.tail())
    #df_reservoirs.to_excel("reservoirs.xlsx")
    #df_history = scrape_history(df_systems, df_reservoirs)
