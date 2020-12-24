""" Module to scrape reservoir data from National Water Agency of Brazil (ANA
for short in Portuguese).

The data is available on the Reservoir Follow-up System (SAR for short in
Portuguese), available on this URL: https://www.ana.gov.br/sar0/Home. Data is
grouped in three different systems, as follows: 'SIN', 'Nordeste e Semiárido' e
'Sistema Cantareira'.

Classes
-------
    _Template : class
        A class used as a template.
    _SARSystems : class
        Handle the data wrangling of SAR systems web page.
    _ReservoirsOfSystems : class
        Handle the data wrangling of reservoirs from a single system.
    _HistoryOfReservoir : class
        Handle the data wrangling of the time history from a single reservoir.
    _DataBase : class
        Handle SQLite3 database operations.

Functions
---------
    scrape_reservoirs_of_system(cfg, name_and_address) -> list
    scrape_history_of_reservoir(cfg, name_and_address) -> None
    main(cfg) -> None

Constants
----------
    DOMAIN : str
        The URL of the domain of ANA web page.
    SAR_URL : str
        The URL of ANA's SAR system.
"""

from abc import ABC, abstractmethod
from datetime import date, datetime
from time import sleep
from time import time
from collections import namedtuple
import random
import requests
import concurrent.futures
import os
import json
import sqlite3
import pickle
import csv
import itertools

from bs4 import BeautifulSoup
import urllib3

from config import config

# ANA's (Agência Nacional de Águas) URL:
DOMAIN: str = r"https://www.ana.gov.br"

# SAR's (Sistema de Acompanhamento de Reservatórios) URL:
SAR_URL: str = "".join([DOMAIN, r"/sar0/Home"])

# Disable the InsecureRequestWarning messages of requests library:
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class _Template(ABC):
    """A class used as a template.

    It will work as a blueprint for other classes that represent other entities
    of ANA's page like: systems, reservoirs, and time history.
    ...

    Attributes
    ----------
    address : str
        The web page address.
    name : str
        The name of the entity that the page represents.
    code : str
        The code of the entity that the page represents. The only entity that
        has a code available is the reservoir.
    _page : requests.models.Response
        The variable returned by requests.get() method.
    status : int
        The status returned by the request.

    Methods
    -------
    _clean(input_string)
        Clean any given string.
    scrape()
        Calls the methods to download and parse the page.
    _download()
        Download the page.
    _get_file_name()
        Assembly the full path of the file name to be saved to the disk.
    to_disk()
        Save the data to disk.
    _row()
        Provides the namedtuple that represents one row of data.
    data()
        Exposes the data gathered from the web page.
    _parse()
        Parse the data available on the web page to a list of namedtuples.
    """

    def __init__(self,
                 address: str,
                 name: str = None,
                 code: str = None) -> None:
        """
        Parameters
        ----------
        address : str
            The web page address.
        name : str
            The name of the entity that the page represents.
        code : str
            The code of the entity that the page represents. The only entity
            that has a code available is the reservoir.
        """
        self.address: str = address
        self.name: str = self._clean(name)
        self.code: str = code

    def _clean(self, input_string: str):
        """Clean any given string.

        Returns the string in lower case, replacing blank spaces (' ') with
        underscores ('_') and removing accents.
        ...

        Parameters
        ----------
        input_str : str
            The string to be cleaned.

        Returns
        -------
        out : str
            The cleaned string.
        """
        if input_string:
            out: str = input_string.strip()
            out = "_".join(out.split())
            out = out.lower()
            out = out.replace('á', 'a') \
                     .replace('ã', 'a') \
                     .replace('â', 'a') \
                     .replace('é', 'e') \
                     .replace('ê', 'e') \
                     .replace('í', 'i') \
                     .replace('ó', 'o') \
                     .replace('õ', 'o') \
                     .replace('ô', 'o') \
                     .replace('ú', 'u') \
                     .replace('ç', 'c')
        else:
            raise NotImplementedError("An empty string is not supported.")
        return out

    def scrape(self) -> None:
        """Calls the methods to download and parse the page.

        The page will only be parsed if the status returned is 200.
        """
        self._download()
        if self.status == 200:
            print("Parsing: {}".format(self.name))
            self._parse()
        else:
            # TODO: Log it.
            # TODO: Show a warning.
            pass

    def _download(self) -> None:
        """Download the page."""
        # TODO: Log it.
        # TODO: Handle exceptions.
        # Add a friendly sleep to not overload the server.
        delay = random.uniform(0.0, 10.)
        print("Fetching in {:05.2f} seconds: {}".format(delay, self.name))
        sleep(delay)
        self._page: requests.models.Response = requests.get(self.address,
                                                            verify=False)
        self.status: int = self._page.status_code
        print("Request Status: {:3d} - {}".format(self.status, self.name))

    def _get_file_name(self, folder: str, extension: str) -> str:
        """Assembly the full path of the file name to be saved to the disk.

        It will join the folder, the name (self.name) and the file extension.
        ...

        Parameters
        ----------
        folder : str
            The folder name.
        extension : str
            The file extension.

        Returns
        -------
        __out_name : str
            Output string. Folder, self.name, and extension all concatenated.
        """
        __out_name: str = ".".join([self.name, extension])
        __out_name = os.path.join(folder, __out_name)
        return __out_name

    def to_disk(self, cfg: dict) -> None:
        """
        Save the data to disk.
        ...

        Parameters
        ----------
        cfg : dict
            A dictionary holding boolean ["SQLITE3", "JSON", "PICKLE", "CSV"]
            and string entries ["DIR"].
            The string entry ["DIR"] holds the name of the folder to save the
            files. The booleans entries represent if the corresponding file
            extension has to be saved to the disk.
        """
        print("Saving {} to disk.".format(self.name))
        # TODO: Log it.
        # TODO: Raise exceptions when it is not possible to save the file.

        # Save the data to a .db file.
        if cfg["SQLITE3"]:
            __db_name: str = self._get_file_name(cfg["DIR"], 'db')
            __db = _DataBase(__db_name)
            __db.write_data(self.data)
            del __db

        if cfg["JSON"] or cfg["PICKLE"] or cfg["CSV"]:
            # Convert all rows from namedtuples to dictionaries, otherwise,
            # depending on the file extension, it won't work or the resulting
            # file loses the field names, resulting in poor readability.
            __data: list = list(map(lambda row: dict(row._asdict()),
                                    self.data))

            # Save the data to a json file.
            if cfg["JSON"]:
                __json_name: str = self._get_file_name(cfg["DIR"], 'json')
                with open(__json_name, 'w') as __file:
                    json.dump(__data, __file)

            # Save the data to a pickle file.
            if cfg["PICKLE"]:
                __pickle_name: str = self._get_file_name(cfg["DIR"], 'pickle')
                with open(__pickle_name, 'wb') as __file:
                    pickle.dump(__data, __file)

            # Save the data to a csv file.
            if cfg["CSV"]:
                __csv_name: str = self._get_file_name(cfg["DIR"], 'csv')
                with open(__csv_name, 'w',
                          encoding='utf-8',
                          newline='') as __file:
                    fcsv = csv.DictWriter(__file,
                                          fieldnames=__data[0].keys(),
                                          delimiter=';')
                    fcsv.writeheader()
                    fcsv.writerows(__data)
            del __data  # Free memory.

    @property
    @abstractmethod
    def _row(self) -> namedtuple:
        """Provides the namedtuple that represents one row of data.

        The implementation is left to subclasses.
        """
        ...

    @property
    @abstractmethod
    def data(self) -> list:
        """Exposes the data gathered from the web page.

        The implementation is left to subclasses.
        """
        ...

    @abstractmethod
    def _parse(self) -> None:
        """Parse the data available on the web page to a list of namedtuples.

        The implementation is left to subclasses.
        """
        ...

    def __str__(self) -> str:
        """"Show a human readable representation of one class object."""
        strings = []
        for row in self.data:
            strings.append(f"Name: {row.name} \n\
                             Address: {row.address}")
        return "\n".join(strings)


class _SARSystems(_Template):
    """Handle the data wrangling of SAR systems web page.

    Currently (november, 2020), there are 3 systems available on SAR:
    'Nordeste e Semiárido', 'SIN' and 'Cantareira.
    The available data are the names and addresses of each SAR system.
    ...

    Attributes
    ----------
    __systems : list of namedtuples.
        Holds the list of all systems parsed from the web page.

    Methods
    -------
    _row()
        Provides the namedtuple that represents one row of data.
    _parse()
        Parse the data available on the web page to self.__systems.
    data
        Exposes the data wrangled from the web page.
    """

    def __init__(self, address: str) -> None:
        """
        Parameters
        ----------
        address : str
            The web page address.
        """
        super().__init__(name="SARSystems", address=address)
        self.__systems: list = []

    @property
    def _row(self) -> namedtuple:
        """Provides the namedtuple that represents one row of data.

        Each row of data will be stored on self.__systems.
        ...

        Returns
        -------
        blueprint : namedtuple
        It holds the fields of each row of data. Systems have 'name' and
        'address' fields.
        """
        # Every single system has 'name' and 'address', thus these are the
        # fields of the namedtuple.
        blueprint: namedtuple = namedtuple("systems", ['name', 'address'])
        return blueprint

    @property
    def data(self) -> list:
        """Exposes the data gathered from the web page.

        Should be run after self.scrape(), otherwise wll return an empty list.

        Returns
        -------
        self.__systems : list of namedtuples
            A list of namedtuples that holds all SAR systems.
        """
        return self.__systems

    def _parse(self) -> None:
        """Parse the data available on the web page to a list of namedtuples.

        The data will be stored in self.__systems.
        """
        soup = BeautifulSoup(self._page.content, "html.parser")
        results = soup.find_all(string="Dados Históricos")
        # Build the list of systems that belong to SAR.
        for result in results:
            # Parse the system name:
            sys_name: str = result.parent.parent.parent.parent.parent. \
                find_next('strong').text
            # Parse the system web address:
            sys_address: str = result.parent.parent.parent. \
                find('a', href=True)['href']
            sys_address: str = "".join([DOMAIN, sys_address])
            # Store the data in a list of named tuples:
            self.__systems.append(self._row(sys_name, sys_address))


class _ReservoirsOfSystem(_Template):
    """Handle the data wrangling of reservoirs from a single system.
    ...

    Attributes
    ----------
    __reservoirs : list of named tuples
        Holds the list of all reservoirs parsed from the web page.
    _date : str, optional
        Final date to search for data. Format is DD/MM/YYYY.

    Methods
    -------
    _row()
        Provides the namedtuple that represents one row of data.
    data()
        Exposes the data wrangled from the web page.
    _parse()
        Parse the data available on the web page to self.__systems.
    __reservoir_address()
        Address of the web page that stores the reservoir time history.
    """

    def __init__(self,
                 system_name: str,
                 system_address: str,
                 _date: str = date.today().strftime("%Y/%m/%d")) -> None:
        """
        Parameters
        ----------
        system_name : str
            The name of the system.
        system_address : str
            The web page address.
        _date: str, optional
            Final date to search for data. Format is DD/MM/YYYY.
        """
        super().__init__(name=system_name,
                         address=system_address)
        self.__reservoirs: list = []
        self._date: str = _date

    @property
    def _row(self) -> namedtuple:
        """Provides the namedtuple that represents one row of data.

        Each row of data will be stored on self.__reservoirs.
        ...

        Returns
        -------
        blueprint : namedtuple
        It holds the fields of each row of data. Reservoirs have 'code', 'name'
        , 'address' and 'system' as fields.
        """
        blueprint: namedtuple = namedtuple(
                        "reservoirs", ["code", "name", "address", "system"])
        return blueprint

    @property
    def data(self) -> list:
        """Exposes the data gathered from the web page.

        Should be run after self.scrape(), otherwise wll return an empty list.

        Returns
        -------
        self.__reservoirs : list of namedtuples
            A list of namedtuples that holds all reservoirs that belongs to the
            system.
        """
        return self.__reservoirs

    def _parse(self) -> None:
        """Parse the data available on the web page to a list of namedtuples.
        The data will be stored in self.__reservoirs.
        """
        # Parse using BeautifulSoup library.
        soup = BeautifulSoup(self._page.content, "html.parser")
        results = soup.find(
            'select',
            class_="form-control input-m-sm",
            id="dropDownListReservatorios"
        ).find_all_next("option")[1:]
        # Build the list of reservoirs that belongs to the system.
        for result in results:
            res_code: str = result["value"]
            res_name: str = self._clean(result.text)
            res_address: str = self.__reservoir_address(res_code)
            self.__reservoirs.append(self._row(res_code,
                                               res_name,
                                               res_address,
                                               self.name))

    def __reservoir_address(self, code: str) -> str:
        """Assembly the web address of the reservoirs' web page.
        ...

        Parameters
        ----------
        code : str
            The reservoir's code. It is a 5 digit integer stored as string.

        Returns
        -------
        __url : str
            The web address of the web page that stores the reservoir's time
            history.
        """
        __query: str = \
            r'?dropDownListEstados=6&dropDownListReservatorios=' + code + \
            '&dataInicial=22%2F04%2F1500&dataFinal=' + \
            self._date[8:10] + '%2F' + \
            self._date[5:7] + '%2F' + \
            self._date[0:4] + \
            '&button=Buscar'
        __url: str = "".join([self.address, __query])
        return __url


class _HistoryOfReservoir(_Template):
    """Handle the data wrangling of the time history from a single reservoir.
    ...

    Constants
    ---------
    __PARSING_MAP: dict of dicts
        Maps the relation between the system and its corresponding data
        available on the time history. For each system it maps the relation
        between the variable and its correponding tag on the web page html
        code.

    Attributes
    ----------
    system : str
        The name of the system that the time history belongs.

    __history: list of namedtuples
        Holds the time history. Each row corresponds to one day, available on
        the the web page.

    Methods
    -------
    _row()
        Provides the namedtuple that represents one row of data.
    data()
        Exposes the data wrangled from the web page.
    _parse()
        Parse the data available on the web page to self.__history.
    """

    __PARSING_MAP: dict = {
        "nordeste_e_semiarido": {
            # Capacidade (hm3):
            "capacity_hm3": "text-center coluna_4",
            # Cota (m):
            "level_m": "text-center coluna_3",
            # Volume (hm3):
            "volume_hm3": "text-center coluna_5",
            # Volume (%):
            "volume_100": "text-center coluna_6",
            # Data da Medição:
            "date": "text-center coluna_7"
        },
        "sin": {
            # Cota (m):
            "level_m": "text-center coluna_3",
            # Afluência (m3/s):
            "influx_m3_s": "text-center coluna_4",
            # Defluência (m3/s):
            "defluence_m3_s": "text-center coluna_5",
            # Vazão Vertida (m3/s):
            "poured_flow_rate_m3_s": "text-center coluna_6",
            # Vazão Turbinada (m3/s):
            "turbined_flow_rate_m3_s": "text-center coluna_7",
            # Vazão Natural (m3/s):
            "natural_flow_rate_m2_s": "text-center coluna_5",
            # Volume Útil (%):
            "useful_volume_100": "text-center coluna_9",
            # Vazão Incremental (m3/s):
            "incremental_flow_rate_m3_s": "text-center coluna_11",
            # Data da Medição:
            "date": "text-center coluna_13"
        },
        "sistema_cantareira": {
            # Cota (m):
            "level_m": "text-center coluna_3",
            # Volume Útil (hm3):
            "useful_volume_hm3": "text-center coluna_4",
            # Volume Útil (%):
            "useful_volume_100": "text-center coluna_5",
            # Afluência (m3/s):
            "influx_m3_s": "text-center coluna_6",
            # Defluência (m3/s):
            "defluence_m3_s": "text-center coluna_7",
            # Data da Medição:
            "date": "text-center coluna_8"
        }
    }

    def __init__(self,
                 system: str,
                 reservoir_name: str,
                 reservoir_address: str) -> None:
        """
        Parameters
        ----------
        system : str
            The name of the system that the reservoirs' time history belongs
            to.
        reservoir_name : str
            The name of the reservoir that the time history belongs to.
        reservoir_address: str
            The web page address.
        """
        super().__init__(name=reservoir_name, address=reservoir_address)
        self.system: str = system
        self.__history: list = []

    @property
    def _row(self) -> namedtuple:
        """Provides the namedtuple that represents one row of data.

        Each row of data will be stored on self.__history.

        Returns
        -------
        blueprint : namedtuple
        It holds the fields of each row of data. Since it varies from system to
        system, it will be assembled in real-time.
        """
        # The available fields vary depending on the system that the
        # reservoirs' time history belongs to. This is the reason why the list
        # of fields is built-in real-time, and is not constant.
        # It will be built according to the data got from __PARSING_MAP
        # dictionary. The fields will be taken from the dictionaries keys.
        __fields: list = list(
            _HistoryOfReservoir.__PARSING_MAP.get(self.system).keys())
        blueprint: namedtuple = namedtuple(
                                        self.system, __fields + ["reservoir"])
        return blueprint

    @property
    def data(self) -> list:
        """Exposes the data gathered from the web page.

        Should be run after self.scrape(), otherwise wll return an empty list.
        ...

        Returns
        -------
        self.__history : list of namedtuples
            A list of namedtuples that holds all reservoir time history.
        """
        return self.__history

    def _parse(self) -> None:
        """Parse the data available on the web page to a list of namedtuples.

        The data will be stored in self.__history.
        """
        # Parse using BeautifulSoup library.
        soup = BeautifulSoup(self._page.content, "html.parser")
        results = soup.find('tbody', class_="list")
        __tags: list = list(
                    _HistoryOfReservoir.__PARSING_MAP.get(self.system).items())
        # TODO: Raise NotImplementedError when the self.system is not found.
        for __tag in __tags:
            __values = results.find_all('td', class_=__tag)
            # Get the value:
            __values: list = [v.text for v in __values]
            self.__history.append(__values)

        # Append the name of the reservoir:
        self.__history.append(len(self.__history[0]) * [self.name])

        # Transpose and rearrange:
        self.__history = list(map(tuple, zip(*self.__history)))
        self.__history = [self._row._make(v) for v in self.__history]


class _DataBase():
    """"Handle SQLite3 database operations.
    ...

    Attributes
    ----------
    __connection : sqlite3.Connection
        A connection to the SQLite database.

    __cursor : sqlite3.Cursor
        The cursor of the SQLite database.

    Methods
    -------
    __create_table(table_name: str, columns: list)
        Create tables in the database, only if the table doesn't exist.

    write_data(__list: list)
        Insert a list of namedtuples in the database. The name of the table
        will be taken from the instance of the namedtuple passed to the method.
    """

    def __init__(self, db_file) -> None:
        """
        Parameters
        ----------
        db_file : str
            The name of the database file.
        """
        # Create the connection toi the database.
        self.__connection: sqlite3.Connection = sqlite3.connect(
                                            db_file, check_same_thread=False)
        self.__cursor: sqlite3.Cursor = self.__connection.cursor()

    def __create_table(self, table_name: str, columns: list):
        """Create tables in the database, only if the table doesn't exist.
        ...

        Parameters
        ----------
        table_name : str
            The name of the table o be created  if it doesn't exist.
        columns: list of str
            A list containning the names of the columns of the table.
        """
        __fields = ['\t' + col for col in columns]
        __fields = " TEXT, \n".join(__fields) + " TEXT"

        __sql = "".join(["CREATE TABLE IF NOT EXISTS ",
                         table_name.upper(),
                         " (\n",
                         __fields,
                         "\n\t);"])
        self.__cursor.execute(__sql)

    def write_data(self, __list: list):
        """

        Parameters
        ----------
        _list : list of namedtuples
            List of all named tuples to be inserted in the database. The name
            of the table will be taken from the name of the namedtuple instance
            of the 1st element of the list (zero indexed).
        """
        # Get the name  of the namedtuple instance of the 1st element of the
        # list
        __table_name = __list[0].__class__.__name__.upper()
        # Get the name of the fields.
        __fields = [field.upper() for field in __list[0]._fields]
        # Create the table if it doesn't exist.
        self.__create_table(__table_name, __fields)
        __sql = "".join(["INSERT INTO ",
                         __table_name,
                         "(",
                         ", ".join(__fields),
                         ")\n",
                         "\t VALUES(",
                         ", ".join(['?' for _ in range(len(__fields))]),
                         ")"])
        self.__cursor.executemany(__sql, __list)
        # Commit the changes and close the connection.
        self.__connection.commit()
        self.__connection.close()


def scrape_reservoirs_of_system(cfg: dict,
                                name_and_address: namedtuple) -> list:
    """Scrape all reservoirs from a single system.

    Each system has a set of reservoirs. This function gathers this list.
    ...

    Parameters
    ----------
    cfg : dict
        A dictionary holding boolean ["SQLITE3", "JSON", "PICKLE", "CSV"]
        and string entries ["DIR"].
        The string entry ["DIR"] holds the name of the folder to save the
        files. The booleans entries represent if the corresponding file
        extension has to be saved to the disk.
    name_and_address : namedtuple
        A namedtuple with two fields: 'name', that is the name of the system
        and 'address', that is the web page's URL.

    Returns
    -------
    reservoirs.data : list of namedtuples holding the systems' reservoirs names
    and addresses.
    """
    reservoirs = _ReservoirsOfSystem(name_and_address.name,
                                     name_and_address.address)
    reservoirs.scrape()
    reservoirs.to_disk(cfg)
    return reservoirs.data


def scrape_history_of_reservoir(cfg: dict,
                                name_and_address: namedtuple) -> None:
    """Scrape all history from a single reservoir.

    The data will be saved to the disk according to the settings of the config
    dictionary. The following file formats are available: SQLITE3, JSON, PICKLE
    , and CSV. The file wwil lbe written according to the 'DIR' dictionary key.
    ...

    Parameters
    ----------
    cfg : dict
        A dictionary holding boolean ["SQLITE3", "JSON", "PICKLE", "CSV"]
        and string entries ["DIR"].
        The string entry ["DIR"] holds the name of the folder to save the
        files. The booleans entries represent if the corresponding file
        extension has to be saved to the disk.
    name_and_address : namedtuple
        A namedtuple with three fields: 'system', that is the name of the
        system which the reservoir belongs to, 'name', that is the name of the
        reservoir, 'address', that is the web page's URL.
    """
    history = _HistoryOfReservoir(name_and_address.system,
                                  name_and_address.name,
                                  name_and_address.address)
    history.scrape()
    history.to_disk(cfg)
    del history  # Free the memory.


def scrape(cfg: dict):
    """Scrape all available data.

    The first step taken is to gather the systems available on SAR. Currently
    (november 20202), there are 3 systems: 'nordeste_e_semiarido', 'sin', and
    'cantareira'.
    The second step is to gather the list of all reservoirs that belongs to
    every system.
    The third step is to gather all time history of all reservoirs of all
    that were gathered in the previous steps.
    ...

    Parameters
    ----------
    cfg : dict
        A dictionary holding boolean ["SQLITE3", "JSON", "PICKLE", "CSV"]
        and string entries ["DIR"].
        The string entry ["DIR"] holds the name of the folder to save the
        files. The booleans entries represent if the corresponding file
        extension has to be saved to the disk.
    """
    # -----> Start:
    initial: float = time()
    print("Starting: {}".format(datetime.now().strftime("%Y/%m/%d %H:%M:%S")))

    # -----> First step:
    # Download, parse and save the table holding all SAR systems' and its URL.
    systems = _SARSystems(SAR_URL)
    systems.scrape()
    systems.to_disk(cfg)

    # -----> Second step:
    # Download, parse and save the tables holding all reservoirs, of all
    # system, and its URL.
    random.shuffle(systems.data)
    args = ((cfg, data) for data in systems.data)
    with concurrent.futures.ThreadPoolExecutor() as executor:
        result = executor.map(lambda p: scrape_reservoirs_of_system(*p), args)
    reservoirs = list(itertools.chain.from_iterable(result))

    # -----> Third step:
    # Downolad, parse and save the tables holding all history, for all
    # reservoirs.
    #
    # During the development fase, it showed up that
    # 'executor.map(scrape_history_of_reservoir, reservoirs)' was eating all
    # memory. The choice was to use the solution provided by this post:
    # https://stackoverflow.com/questions/37445540/
    # memory-usage-with-concurrent-futures-threadpoolexecutor-in-python3

    random.shuffle(reservoirs)
    args = ((cfg, res) for res in reservoirs)

    MAX_WORKERS = min(32, os.cpu_count() + 4)
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as e:
        futures_not_done = {e.submit(scrape_history_of_reservoir, *(cfg, res))
                            for res in reservoirs}
        concurrent.futures.wait(futures_not_done,
                                return_when=concurrent.futures.FIRST_COMPLETED)

    # -----> Finish:
    print("Finishing: {}".format(datetime.now().strftime("%Y/%m/%d %H:%M:%S")))
    final: float = time()
    print("Elapsed Time: {:05.2f} minutes.".format((final-initial)/60))


if __name__ == "__main__":
    scrape(config)
