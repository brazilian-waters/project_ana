""" Module to scrape reservoir data from National Water Agency of Brazil (ANA
for short in Portuguese).
The data is available on the Reservoir Follow-up System (SAR for short in
Portuguese), available on this URL: https://www.ana.gov.br/sar0/Home.
Data is grouped on three different systems, as follows: 'SIN',
'Nordeste e Semiárido' e 'Sistema Cantareira'.
"""

import datetime
import os
import concurrent.futures
import pandas as pd
import requests
from bs4 import BeautifulSoup
from config import DATA_DIR, SYSTEMS_DIR, RESERVOIR_DIR

##############################################################################

DATE_COL = "date"

# Variables X text to scrape "Nordeste e Semiárido" system:
NORDESTE = [("capacity_m3", "text-center coluna_4"), # Capacidade (hm³)
            ("level_m", "text-center coluna_3"), # Cota (m)
            ("vol_hm3", "text-center coluna_5"), # Volume (hm³)
            ("vol_100", "text-center coluna_6"), # Volume (%)
            (DATE_COL, "text-center coluna_7")] # Data da Medição

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
       (DATE_COL, "text-center coluna_13")] # Data da Medição

# Variables X text to scrape "Cantareira" system:
CANTAREIRA = ([("level_m", "text-center coluna_3"), # Cota (m)
               ("net_vol_hm3", "text-center coluna_4"), # Volume Útil (hm³)
               ("net_vol_100", "text-center coluna_5"), # Volume Útil (%)
               ("inflow_m3s", "text-center coluna_6"), # Afluência (m³/s)
               ("outflow_m3s", "text-center coluna_7"), # Defluência (m³/s)
               (DATE_COL, "text-center coluna_8")]) # Data da Medição

##############################################################################

def download_page(page_url, file_name):
    """Download one web page and stores in a file on disk.
    Args:
    page_url: String. The URL of the web page to be downloaded.
    file_name: String. The file to store the web page.

    Returns:
    response.status_code: Integer. Status of requests library, 200 is success.
    """
    response = requests.get(page_url, verify=False)
    if response.status_code == 200:
        with open(file_name, mode='wb') as file:
            file.write(response.content)
    print(">>> File: {} - Status: {}.".format(file_name, response.status_code))
    return response.status_code

class AllSystems():
    """Handle systems data."""
    def __init__(self, url=r"https://www.ana.gov.br"):
        self.url = url
        if not os.path.isdir(SYSTEMS_DIR):
            os.makedirs(SYSTEMS_DIR)
    
    def get_systems_list(self):
        download_page(self.url)
        



def scrape_systems(url=r"https://www.ana.gov.br"):
    """Scrape all available systems and its corresponding URLs.

    Args:
    url: string. The URL which contains all systems.

    Return:
    systems: list of named tuples holding the systems names, ids and its URLs.
    """
    page = requests.get(url + r'/sar0/Home', verify=False)
    soup = BeautifulSoup(page.text, 'html.parser')
    elements = soup.find_all(string="Dados Históricos")

    sys_id = list(range(len(elements)))
    urls = []
    name = []

    for elm in elements:
        urls.append(elm.parent.parent.parent.find('a', href=True)['href'])
        name.append(elm.parent.parent.parent.parent.parent.find_next('strong'))

    urls = [url + u for u in urls]
    name = [n.text for n in name]
    df_sys = pd.DataFrame({"system_id": sys_id,
                           "system_name": name,
                           "system_url": urls})
    return df_sys

##############################################################################

def scrape_reservoirs(df_sys):
    """Scrape the names and the codes of all reservoirs, of all systems.

    Args:
    df_sys: Pandas DataFrame holding the systems ids, names and URLs.

    Returns:
    df_res: Pandas DataFrame holding reservoirs' codes, names, URLs, and its
    corresponding system id and system name.
    """
    now = datetime.datetime.now()
    temp = [] # List of all all_systems data frames

    for sys_id, sys_url in zip(df_sys["system_id"], df_sys["system_url"]):
        page = requests.get(sys_url, verify=False)
        soup = BeautifulSoup(page.text, 'html.parser')
        element = soup.find('select',
                            class_="form-control input-m-sm",
                            id="dropDownListReservatorios")
        reservoirs = element.find_all_next("option")[1:]
        names = [None] * len(reservoirs)
        codes = [None] * len(reservoirs)

        for j, res in enumerate(reservoirs):
            names[j] = " ".join(res.text.split())
            codes[j] = res['value']

        df_tmp = pd.DataFrame({"reservoir_code": codes,
                               "reservoir_name": names,
                               "system_id": [sys_id] * len(reservoirs)})
        temp.append(df_tmp)

    df_res = pd.concat(temp, ignore_index=True)
    df_res = df_res.merge(df_sys, how='left', on="system_id")
    df_res.rename(columns={"system_url": "reservoir_url"}, inplace=True)
    # Assembly the reservoir URL:
    df_res["reservoir_url"] = df_res["reservoir_url"] + \
                              r"?dropDownListReservatorios=" + \
                              df_res["reservoir_code"] + \
                              r"&dataInicial=22%2F04%2F1500" + \
                              r"&dataFinal=" + now.strftime("%d") + \
                              r"%2F" + now.strftime("%m") + \
                              r"%2F" + now.strftime("%Y") + \
                              r"&button=Buscar"
    # Reorder columns:
    df_res = df_res[["reservoir_code", "reservoir_name", "reservoir_url",
                     "system_id", "system_name"]]
    return df_res

##############################################################################

def get_file_name(sys_id, res_code):
    """Return the file name of a reservoir.
    Args:
    sys_id: Integer. The system id.
    res_code: The reservoir code.

    Returns:
    file_name: String with the file name.
    """
    file_name = "{:02d}_{}.html".format(sys_id, res_code)
    return file_name

##############################################################################



##############################################################################

def download_all_history(df_res):
    """"Download all reservoir's home pages. Each reservoir home page is stored
    in one single .html file using the following name pattern:
    "SYSTEM_ID" + "_" + "RESERVOIR_CODE" + ".html"

    Args:
    df_res: Pandas DataFrame holding reservoirs' codes, names, URLs, and its
    corresponding system id and system name.

    Returns:
    df: Pandas DataFrame holding reservoirs' codes, names, URLs, and its
    corresponding system id and system name, the corresponding .html file name
    and the status of the page request.
    """
    df_tmp = df_res.copy()
    df_tmp["file"] = df_tmp.apply(lambda x: get_file_name(x["system_id"],
                                                          x["reservoir_code"]),
                                  axis=1)
    df_tmp["file"] = df_tmp["file"].apply(lambda x:
                                          os.path.join(RESERVOIR_DIR, x))

    with concurrent.futures.ThreadPoolExecutor() as executor:
        future = executor.map(download_page,
                              df_tmp["reservoir_url"],
                              df_tmp["file"])

    df_tmp["status"] = list(future)
    return df_tmp

if __name__ == '__main__':

    df_systems = scrape_systems()
    df_reservoirs = scrape_reservoirs(df_systems)

    df_reservoirs = download_all_history(df_reservoirs)

    if not os.path.isdir(RESERVOIR_DIR):
        os.makedirs(RESERVOIR_DIR)

    df_systems.to_excel(os.path.join(DATA_DIR, "systems.xlsx"),
                        index=False)
    df_reservoirs.to_excel(os.path.join(DATA_DIR, "reservoirs.xlsx"),
                           index=False)