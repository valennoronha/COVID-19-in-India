import requests
from bs4 import BeautifulSoup as soup
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe

data = requests.get(
    "https://en.wikipedia.org/wiki/Template:COVID-19_pandemic_data/India_medical_cases_by_state_and_union_territory")
soup = soup(data.text, 'html.parser')

table = soup.find('div', id='covid19-container')

rows = table.find_all('tr')
columnstates = [v.replace('\n', '') for v in rows[1].find_all('th')[0]]
columns = [v.text.replace('\n', '') for v in rows[1].find_all('th')[1:]]
df1 = pd.DataFrame(columns=columnstates)
df = pd.DataFrame(columns=columns)

for x in range(3, len(rows) - 2):
    state_names = ([y.text.replace('\n', '') for y in rows[x].find_all('th')])
    df1 = df1.append(pd.Series(state_names, index=columnstates), ignore_index=True)

for i in range(3, len(rows) - 2):
    tds = rows[i].find_all('td')
    values = [
        tds[0].text.replace('\n', ''),
        tds[1].text.replace('\n', ''),
        tds[2].text.replace('\n', ''),
        tds[3].text.replace('\n', '')
    ]
    df = df.append(pd.Series(values, index=columns), ignore_index=True)

dffinal = pd.merge(df1, df, left_index=True, right_index=True, how='outer')
dffinal = dffinal.rename({'State/Union Territory': 'State&UT', 'Cases[a]': 'Cases'}, axis=1)

dee = dffinal.copy()


# dee = dee.replace({'Andaman and Nicobar Islands': 'A&N', 'Andhra Pradesh': 'AP',
#                    'Arunachal Pradesh': 'AR',
#                    'Chandigarh': 'CN',
#                    'Chhattisgarh': 'CG',
#                    'Dadra and Nagar Haveli and Daman and Diu': 'D&D',
#                    'Himachal Pradesh': 'HP',
#                    'Jammu and Kashmir': 'J&K',
#                    'Madhya Pradesh': 'MP',
#                    'Maharashtra': 'MH',
#                    'Lakshadweep': 'LD',
#                    'Puducherry': 'PY',
#                    'Tamil Nadu': 'TN',
#                    'Uttarakhand': 'UK',
#                    'Uttar Pradesh': 'UP'})


def clean_values(value):
    if isinstance(value, str):
        return (value.replace('[b]', '').replace(',', '')).replace('[c]', '')
    return value


dee['State&UT'] = dee['State&UT'].astype('str')
dee['Cases'] = dee['Cases'].apply(clean_values).astype('int')
dee['Deaths'] = dee['Deaths'].apply(clean_values).astype('int')
dee['Recoveries'] = dee['Recoveries'].apply(clean_values).astype('int')
dee['Active'] = dee['Active'].apply(clean_values).astype('int')

# dee.to_excel('covidlive.xlsx', index=False)

gc = gspread.service_account(filename='COVID-19 in India-26fce3c6eceb.json')
sh = gc.open_by_key('1hHGkDCS3ApO4mnifHDtSNA2aDExTBCNxbgY910_0w0w')
worksheet = sh.get_worksheet(0)
set_with_dataframe(worksheet, dee)
