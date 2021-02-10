import requests
from bs4 import BeautifulSoup as soup
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe

print("Modules imported without an error.")

# sending request to the url
data = requests.get(
    "https://en.wikipedia.org/wiki/Template:COVID-19_pandemic_data/India_medical_cases_by_state_and_union_territory")

# parsing the html with beautiful soup
soup = soup(data.text, 'html.parser')

# extracting the relevant data from the html
table = soup.find('div', id='covid19-container')
rows = table.find_all('tr')

# extracting the heading of the states column and correcting the typos within the list comprehension.
columnstates = [v.replace('\n', '') for v in rows[1].find_all('th')[0]]
# extracting the heading of the columns that hold numbers and correcting the typos within the list comprehension.
columns = [v.text.replace('\n', '') for v in rows[1].find_all('th')[1:]]

# making two separate dataframes using pandas with the respective columns
df1 = pd.DataFrame(columns=columnstates)
df = pd.DataFrame(columns=columns)

# using a for loop to iterate over each row in the state column, extracting each state,
# and correcting the typos within the list comprehension.
for x in range(3, len(rows) - 2):
    state_names = ([y.text.replace('\n', '') for y in rows[x].find_all('th')])
    df1 = df1.append(pd.Series(state_names, index=columnstates), ignore_index=True)

# using a for loop to iterate over each row in the values column, extracting each value from the next three columns,
# and correcting the typos within the list comprehension.
for i in range(3, len(rows) - 2):
    tds = rows[i].find_all('td')
    values = [
        tds[0].text.replace('\n', ''),
        tds[1].text.replace('\n', ''),
        tds[2].text.replace('\n', ''),
        tds[3].text.replace('\n', '')
    ]
    # appending the values as pandas series.
    df = df.append(pd.Series(values, index=columns), ignore_index=True)

# merging the two dataframes into one using the outer join.
dffinal = pd.merge(df1, df, left_index=True, right_index=True, how='outer')

# renaming the column headings
dffinal = dffinal.rename({'State/Union Territory': 'State&UT', 'Cases[a]': 'Cases'}, axis=1)


# cleaning some values that contain unnecessary letters and characters from hyperlinks from the original data source.
def clean_values(value):
    if isinstance(value, str):
        return (value.replace('[b]', '').replace(',', '')).replace('[c]', '')
    return value


# changing the data type of individual columns
dffinal['State&UT'] = dffinal['State&UT'].astype('str')
dffinal['Cases'] = dffinal['Cases'].apply(clean_values).astype('int')
dffinal['Deaths'] = dffinal['Deaths'].apply(clean_values).astype('int')
dffinal['Recoveries'] = dffinal['Recoveries'].apply(clean_values).astype('int')
dffinal['Active'] = dffinal['Active'].apply(clean_values).astype('int')

# uploading the acquired dataframe to google sheets using the Google API.
# make sure all json file is in the same folder as your py file.
gc = gspread.service_account(filename='enter_you_json_filename')
sh = gc.open_by_key('enter_your_google_sheet_key')
worksheet = sh.get_worksheet(0)
set_with_dataframe(worksheet, dffinal)
