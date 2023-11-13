from bs4 import BeautifulSoup
import requests
import pandas as pd
from datetime import date, timedelta, datetime

csv_path = 'tsa_market/data/tsa_checkins.csv'


def create_tsa_data():
    """Creates a pandas data frame for tsa checkin. Will always be missing day before yesterday's data"""

    tsa_data = _get_tsa_df()

    # Range of dates in the data
    # tsa_data['Date'].iloc[-1][:-4] + headers[-1]

    start_date = '1/1/' + tsa_data.columns[-1]
    end_date = date.today() + timedelta(days=-2)
    date_range = pd.date_range(start_date, end_date)[::-1]
    # Yesterday's row is always missing
    date_range = date_range.delete([1460, 1095, 729, 364])
    # Remove leap year dates
    dates = pd.DataFrame(date_range, columns=['Date'])
    dates = dates.loc[~(dates['Date'].dt.month.eq(
        2) & dates['Date'].dt.day.eq(29))]

    # No longer have use for date column
    tsa_data.drop(['Date'], axis=1, inplace=True)

    # First blank on first column gives us index of 12/31 for last year
    blank_row_index = tsa_data[tsa_data.iloc[:, 0] == ""].index[0]

    checkins = pd.DataFrame()

    for year in tsa_data.columns:
        checkins = pd.concat([checkins,
                              tsa_data[year][blank_row_index:],
                              tsa_data[year][:blank_row_index]], ignore_index=True)

    checkins = checkins[checkins.iloc[:, 0] != ""]
    checkins.rename(columns={0: 'Checkins'}, inplace=True)
    checkins.reset_index(drop=True, inplace=True)
    dates.reset_index(drop=True, inplace=True)
    checkins = pd.concat([dates, checkins], axis=1)

    checkins.to_csv(
        csv_path, index=False)

    return checkins


def update(filename=csv_path):
    """Updates an already existing file"""
    checkins = pd.read_csv(filename)

    try:
        start_date = datetime.strptime(
            checkins['Date'].loc[0], '%m/%d/%Y') + timedelta(days=1)
        end_date = date.today() + timedelta(days=-2)
    except:
        print("Data up to date")
        return

    tsa_data = _get_tsa_df()
    year = tsa_data.columns[1]

    unfinished_range = pd.date_range(start_date, end_date,)
    for index in reversed(range(len(unfinished_range))):
        checkins.loc[-1] = [tsa_data[tsa_data.columns[0]]
                            [index], tsa_data[year][index]]
        checkins.index = checkins.index + 1
        checkins.sort_index(inplace=True)

    print(checkins)
    checkins.to_csv(
        csv_path, index=False)


def _get_tsa_df():
    url = 'https://www.tsa.gov/travel/passenger-volumes'
    # Render table
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    table = soup.find_all('table')[0]

    headers = []

    # Loop through available years, ignore data column
    for header in table.find_all('th'):
        title = header.text
        headers.append(title)

    df = pd.DataFrame(columns=headers)

    for row in table.find_all('tr')[1:]:
        row_data = row.find_all('td')
        entry = [td.text.replace(",", "").rstrip() for td in row_data]
        df.loc[len(df)] = entry

    return df


if __name__ == '__main__':
    update()
