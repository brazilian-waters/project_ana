""" Module to scrape reservoir data from National Water Agency of Brazil (ANA  
for short in Portuguese).
The data is available on the Reservoir Follow-up System (SAR for short in 
Portuguese), available on this URL: https://www.ana.gov.br/sar0/Home.
Data is grouped on three different systems, as follows: 'SIN', 
'Nordeste e Semiárido' e 'Sistema Cantareira'.
"""

import pandas as pd
import requests
from bs4 import BeautifulSoup
import datetime

def scrape_systems(URL = r"https://www.ana.gov.br"):
    """Scrape all available systems and its URLs.

    Args:
    url: string. The URL containing all systems.

    Return:
    df: Pandas DataFrame holding the systems and its URLs.
    """    
    page = requests.get(URL + r'/sar0/Home', verify=False)
    soup = BeautifulSoup(page.text, 'html.parser')
    systems = soup.find_all(string="Dados Históricos")

    urls = [None] * len(systems)
    name = [None] * len(systems)    
    for i, system in enumerate(systems):
        urls[i] = system.parent.parent.parent.find('a', href=True)['href']
        name[i] = system.parent.parent.parent.parent.parent.find_next('strong')
    
    names = [n.text for n in name]
    urls = [URL + url for url in urls]
    df = pd.DataFrame({"system_id": range(len(systems)),
                       "system": names, 
                       "url": urls})
    return df

def scrape_reservoirs(df_systems):
    """Scrape the names and codes of all reservoirs, of all systems.

    Args:
    df_systems: Pandas DataFrame holding the systems names and its URLs.

    Returns:
    df: Pandas DataFrame holding reservoirs codes and names of all systems.
    """
    temp = [None] * len(df_systems)
    for index, row in df_systems.iterrows():   
        page = requests.get(row.url, verify=False)
        soup = BeautifulSoup(page.text, 'html.parser')
        element = soup.find('select', 
                            class_="form-control input-m-sm", 
                            id="dropDownListReservatorios")
        reservoirs = element.find_all_next("option")[1:]

        names = [None] * len(reservoirs)
        codes = [None] * len(reservoirs)
        for i, res in enumerate(reservoirs):            
            names[i] = " ".join(res.text.split())           
            codes[i] = int(res['value'])
                    
        df = pd.DataFrame({"res_code": codes,
                           "res_name": names,
                           "system_id": [row.system_id] * len(reservoirs)})
        temp[index] = df
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
    df_systems = scrape_systems()    
    df_reservoirs = scrape_reservoirs(df_systems)
    df_reservoirs.to_excel("reservoirs.xlsx")
    df_history = scrape_history(df_systems, df_reservoirs)
