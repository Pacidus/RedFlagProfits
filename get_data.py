import requests
import pandas as pd
import numpy as np

from io import StringIO

headers = {
    "authority": "www.forbes.com",
    "cache-control": "max-age=0",
    "upgrade-insecure-requests": "1",
    "user-agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.89 Mobile Safari/537.36",
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "sec-fetch-site": "none",
    "sec-fetch-mode": "navigate",
    "sec-fetch-user": "?1",
    "sec-fetch-dest": "document",
    "accept-language": "en-US,en;q=0.9",
    "cookie": "client_id=4f023ae61ab633d8e7e2410a838a6ef93b8; notice_preferences=2:1a8b5228dd7ff0717196863a5d28ce6c; notice_gdpr_prefs=0,1,2:1a8b5228dd7ff0717196863a5d28ce6c; _cb_ls=1; _cb=DV5nM5D9GHBDCzpjkB; _ga=GA1.2.611301459.1592235761; __tbc=%7Bjzx%7DIFcj-ZhxuNCMjI4-mDfH1HGM-3PFKcN8Miwl1Jhx9eZNEmuQGlLmxXFL-9qM-F_OBO51AtKdJ3qgOfi3P9vM0qBHA3PyvmasSB5xaCbWibdU2meZrLoZ92gJ8xiw07mk3E9l5ifC0NcYbET3aSZxuA; xbc=%7Bjzx%7DGUDHEU3rvhv6-gySw5OY32YdbGDIZI_hJ7AHN4OvkbydVClZ3QNjNrlQVyHGl3ynSJzzGsKf0w3VfH3le6pYqMAfTQAzgDTJbUHa-cJS7p3ITwLt3PmPKKvsIVyFnHji; __gads=ID=a5ac1829fa387f90:T=1592235777:S=ALNI_MZfOqlh-TglrQCWbFNtjcjgFfkMGQ; _fbp=fb.1.1592235779290.60238202; __qca=P0-59264648-1592235777617; xdibx=N4Ig-mBGAeDGCuAnRIBcoAOGAuBnNAjAKwCcATGQMxEDsAHHQAyUkA0IGAbrAHbaHtc-VMXJVaDZmw6dcvfiPaIkAGzQgAFtmwZcqAPT6A7iYB0AMwD2iSAFNcp2JYC2-3AEts9.c.cBrW0sALwBDHncw.TJGaP1GADZ9Yn0eWyNYENxsFVsAWnhwrwATXNwQnNyQrERLTnLK3PMVdwxcy3Nc7A08p3cQdhVVdTdPb18A4LCIniiYxjjE5NT0zOy8gtGSsoqqjBq6lQamlraOrp7Ldx5SkIBPXFy9219bRFyckIBzeDz-kBU8IRSBRqPQmCwAL7sCAwJ6cNCgIp3YQAbVEIIkTAALDQALpQ8BQaC2Ti2PjCUDRBKUSgIkDw9AgWACEAKNHA8T0EiMEiUfGCOnM1CMdhs.kgFCMoUi1loFHioqCtAysUEoWgaWiuX4gnRMhYxiMOkMjUstnozl0ch0IjiilM5Va1DygmS03Cp0u9iKqWO2XO8Xqh0e.0uiEEmFwdw-kAhLHRLEkIodWyQEKwXJYrHxeK5SBEG2Z2A0EJFSCUMsJDMW0F0GhY4ggCFAA__; _chartbeat2=.1592235763483.1592235790833.1.CZMImKDrkr6iBB9QkcCHzJBoDWn8ZI.3",
}

rqst = requests.get(
    "https://www.forbes.com/forbesapi/person/rtb/0/-estWorthPrev/true.json",
    headers=headers,
)

data = pd.json_normalize(pd.read_json(StringIO(rqst.text))["personList"]["personsLists"])

time = pd.to_datetime(data["timestamp"], unit="ms")
time = np.datetime_as_string(time.dt.floor("D").unique()[:1], unit="D")[0]

Keys = [
    "finalWorth",
    "estWorthPrev",
    "privateAssetsWorth",
    "archivedWorth",
    "personName",
    "gender",
    "birthDate",
    "countryOfCitizenship",
    "state",
    "city",
    "source",
    "industries",
    "financialAssets",
]
data = data[Keys]

data.to_csv(f"data/{time}.csv", index=False)

