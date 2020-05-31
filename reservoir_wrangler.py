""" Module to scrape reservoir data from National Water Agency of Brazil (ANA
for short in Portuguese).
The data is available on the Reservoir Follow-up System (SAR for short in
Portuguese), available on this URL: https://www.ana.gov.br/sar0/Home.
Data is grouped on three different systems, as follows: 'SIN',
'Nordeste e Semiárido' e 'Sistema Cantareira'.
"""

import datetime
import os
from collections import namedtuple
import pandas as pd
import requests
from bs4 import BeautifulSoup
from config import DATA_DIR


##############################################################################

# Variables X text to scrape "Nordeste e Semiárido" system:
NORDESTE = [("capacity_m3", "text-center coluna_4"), # Capacidade (hm³)
            ("level_m", "text-center coluna_3"), # Cota (m)
            ("vol_hm3", "text-center coluna_5"), # Volume (hm³)
            ("vol_100", "text-center coluna_6"), # Volume (%)
            ("date", "text-center coluna_7")] # Data da Medição

# Variables X text to scrape "SIN - Sistema Interligado Nacional" system:
SIN = [("level_m", "text-center coluna_3"), # Cota (m)
       ("inflow_m3s", "text-center coluna_4"), # Afluência (m³/s)
       ("outflow_m3s", "text-center coluna_5"), # Defluência (m³/s)
       ("poured_flow_m3s", "text-center coluna_6"), # Vazão Vertida (m³/s)
       ("turbine_flow_m3s", "text-center coluna_7"), # Vazão Turbinada (m³/s)
       ("natural_flow_m3s", "text-center coluna_8"), # Vazão Natural (m³/s)
       ("natural_flow_m3s", "text-center coluna_9"), # Volume Útil (%)
       ("incremental_flow_m3s", "text-center coluna_11"),
       # Vazão Incremental (m³/s)
       ("date", "text-center coluna_13")] # Data da Medição

# Variables X text to scrape "Cantareira" system:
CANTAREIRA = ([("level_m", "text-center coluna_3"), # Cota (m)
               ("net_vol_hm3", "text-center coluna_4"), # Volume Útil (hm³)
               ("net_vol_100", "text-center coluna_5"), # Volume Útil (%)
               ("inflow_m3s", "text-center coluna_6"), # Afluência (m³/s)
               ("outflow_m3s", "text-center coluna_7"), # Defluência (m³/s)
               ("date", "text-center coluna_8")]) # Data da Medição

##############################################################################

def scrape_systems(url=r"https://www.ana.gov.br"):
    """Scrape all available systems and its URLs.

    Args:
    url: string. The URL which contains all systems.

    Return:
    systems: list of named tuples holding the systems and its URLs.
    """
    page = requests.get(url + r'/sar0/Home', verify=False)
    soup = BeautifulSoup(page.text, 'html.parser')
    elements = soup.find_all(string="Dados Históricos")

    systems = list()
    SystemNamedTuple = namedtuple("system", ["name", "url"])
    for elm in elements:
        sys_url = url + elm.parent.parent.parent.find('a', href=True)['href']
        name = elm.parent.parent.parent.parent.parent.find_next('strong').text
        systems.append(SystemNamedTuple(name, sys_url))
    return systems

##############################################################################

def scrape_reservoirs(all_systems):
    """Scrape the names and codes of all reservoirs, of all systems.

    Args:
    all_systems: list of named tuples holding the all systems names and its 
    URLs.

    Returns:
    df_res: Pandas DataFrame holding reservoirs codes and names of all systems.
    """
    temp = [None] * len(all_systems) # List of all all_systems data frames
    for i, sys in enumerate(all_systems):
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

        df_res = pd.DataFrame({"res_code": codes,
                               "res_name": names,
                               "system_id": [i] * len(reservoirs)})
        temp[i] = df_res
    return pd.concat(temp, ignore_index=True)

##############################################################################

def get_url(sys_url, reservoir_code, day, month, year):
    """"Return the URL of a given reservoir, of a given system.

    Args:
    sys_url: String. The URL of the system.
    reservoir_code: Integer. The code of the reservoir.
    day, month, year: String for day, month and year respectively.

    Returns:
    url: String. The reservoir URL."""

    reservoir = r"?dropDownListReservatorios=" + str(reservoir_code)
    initial = r"&dataInicial=22%2F04%2F1500"
    final = r"&dataFinal={}%2F{}%2F{}".format(day, month, year)
    url = sys_url + reservoir + initial + final
    return url

##############################################################################

def scrape_history(sys_url, struct, reservoir_codes):
    """"Scrape all available data of all reservoirs of one system.

    Args:
    struct: list of tuples (constant) holding the relationship between the
    variables and the text to lookup within the page.

    reservoir_codes: List of all system's reservoir codes.

    Returns:
    df: Pandas Dataframe holding all data of all reservoirs of ONE system.
    """

    today = datetime.datetime.now()
    day = today.strftime("%d")
    month = today.strftime("%m")
    year = today.strftime("%Y")

    dataframes = [None] * len(reservoir_codes)
    for i, code in enumerate(reservoir_codes):
        print(">>> Downloading:", code)
        url = get_url(sys_url, code, day, month, year)
        page = requests.get(url, verify=False)
        soup = BeautifulSoup(page.text, 'html.parser')
        elements = soup.find('tbody', class_="list")

        df_temp = pd.DataFrame()
        for var_col_map in struct:
            var = var_col_map[0]
            text = var_col_map[1]
            temp = elements.find_all('td', class_=text)
            temp = [v.text for v in temp]
            df_temp[var] = temp
        df_temp["reservoir_code"] = [code] * len(temp)
        dataframes[i] = df_temp

    df_sys = pd.concat(dataframes)
    for var_col_map in struct[:-1]:
        var = var_col_map[0]
        df_sys[var] = df_sys[var].str.replace(',', '.').astype('float')
    df_sys["date"] = pd.to_datetime(df_sys["date"], dayfirst=True)
    return df_sys

if __name__ == '__main__':

    systems = scrape_systems()        
    df_reservoirs = scrape_reservoirs(systems)

    if not os.path.isdir(DATA_DIR):
        os.mkdir(DATA_DIR)

    # Scrape all data, of all reservoirs of 'Nordeste e Semiárido' system:
    res_codes = df_reservoirs.loc[df_reservoirs.system_id == 0,
                                  "res_code"].to_list()
    df_nordeste = scrape_history(systems[0].url,
                                 NORDESTE,
                                 res_codes)

    # Scrape all data, of all reservoirs of 'SIN' system:
    res_codes = df_reservoirs.loc[df_reservoirs.system_id == 1,
                                  "res_code"].to_list()
    df_sin = scrape_history(systems[1].url,
                            SIN,
                            res_codes)

    # Scrape all data, of all reservoirs of 'Cantareira' system:
    res_codes = df_reservoirs.loc[df_reservoirs.system_id == 2,
                                  "res_code"].to_list()
    df_cantareira = scrape_history(systems[2].url,
                                   CANTAREIRA,
                                   res_codes)
    os.chdir(DATA_DIR)
    df_cantareira.to_excel("cantareira.xlsx", index=False)


