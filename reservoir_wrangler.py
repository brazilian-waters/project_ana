""" Module to scrape reservoir data from National Water Agency of Brazil (ANA
for short in Portuguese).
The data is available on the Reservoir Follow-up System (SAR for short in 
Portuguese), available on this URL: https://www.ana.gov.br/sar0/Home.
Data is grouped on three different regions,as follows: 'SIN', 
'Nordeste e Semiárido' e 'Sistema Cantareira'.
"""
import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import re
import os

DATA_DIR = "data" # Folder to store all data.

URL = r"https://www.ana.gov.br/sar0/Home"

REGIONS = ['SIN', 'Nordeste e Semiárido', 'Sistema Cantareira']

URLs = [r"https://www.ana.gov.br/sar0/MedicaoSin",
        r"https://www.ana.gov.br/sar0/Medicao",
        r"https://www.ana.gov.br/sar0/MedicaoCantareira"]    

def get_5_digit_int(string):
    """Return a 5 digit number from a string."""
    regex = r"[0-9]{5}"
    five_digits = re.findall(regex, string)[0]
    return five_digits

def get_name(string):
    """Return the name between '>' and '<' from a string."""
    regex = r">(.+)<"
    name = re.findall(regex, string)[0]
    return name

def get_reservoirs_from_region(url):
    """Get the names and codes of reservoirs, of a given system, which are
    available on a given URL.

    Args:
    url: string. The URL containing reservoir data of a given system.

    Returns:
    res_table: Pandas DataFrame holding reservoirs codes and names.
    """
    page = requests.get(url, verify=False)
    
    if page.status_code == 200: # Success        
        soup = BeautifulSoup(page.text, 'html.parser')
        soup = soup.find('select',
                         class_="form-control input-m-sm",
                         id="dropDownListReservatorios")        
        options = soup.find_all_next("option")[1:] # ZERO is not a reservoir.
        codes = [0] * len(options)
        names = [''] * len(options)
        for i, option in enumerate(options):
            names[i] = get_name(str(option))
            codes[i] = get_5_digit_int(str(option))
    
    else: # Fail will be coded later.        
        codes = [np.nan]
        names = [np.nan]    
    return pd.DataFrame({"code": codes, "name": names})

def set_regions_df(regions, urls):
    """Create a data frame containing the name of the regions and its URLs
    
    Args:
    regions: List of string holding the name of the regions.
    urls: List of strings holding the URLs of each corresponding region.
    
    Returns:
    df: Pandas DataFrame holding regions and URLs.     
    """    
    return pd.DataFrame({"region": REGIONS, "url": URLs})

def get_all_reservoirs(df_regions):
    """ Get codes and names of all reservoirs from all regions.
    
    Args:
    df_regions: Pandas DataFrame holding names and URLs of all regions.
    
    Returns:
    df: Pandas DataFrame holding all names and codes of all reservoirs of all
    regions.
    """
    df_list = []
    for index, row in df_regions.iterrows():
        df_region = get_reservoirs_from_region(row["url"])
        df_region["region_id"] = index
        df_list.append(df_region)
    return pd.concat(df_list, ignore_index=True)
        
if __name__ == '__main__':
    df_regions = set_regions_df(REGIONS, URLs)    
    df_reservoirs = get_all_reservoirs(df_regions)
    df_reservoirs.to_excel("reservoirs.xlsx")
    