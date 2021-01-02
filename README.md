# Brazilian National Water Agency (ANA) Data Analysis

This project aims to wrangle data from [national water agency of Brazil](https://www.ana.gov.br/) (Agência Nacional de
Águas in portuguese, ANA for short).<br>
<br>
The data source for the project is listed here:<br>

> * [Reservoirs' time series.](https://www.ana.gov.br/sar0/Home)<br>

## Usage

> 1 - Update the `config.json` as follows:
>> * `DIR` is the folder in which you want to save th data. If the configuration file is missing, then will use
`__output__` as deafult.<br>

>> * Set the entries `JSON`, `SQLITE3`, `PICKLE` and `CSV` to `true` or `false`, according to file format you want to
use as output. You may choose more than one file format, but it will make it slower to run. The `CSV` is the default.<br>

>> * Save the changes to `config.json`.

> 2 - Run `reservoir_wrangler.py` and wait.

> 3 - When finished, you will find all files inside the `DIR` folder.<br>
The file `sarsystems.*` holds all available systems on ANA SAR website. Currently (November2020) there are only three available systems: Cantareira, SIN, and Nordeste e Semiárido.<br>
The files `nordestes_e_semirarido.*`, `sin.json.*` and `sistema_cantareira.*` contains all reservoirs available on its corresponding system. All other files are holds the time series of the reservoirs thta has the sae name of the file.<br>

## Dependencies

> Python - Version 3.7.9<br>
> urllib3 - Version 1.25.9<br>
> BeautifulSoup - Version 4.9.1