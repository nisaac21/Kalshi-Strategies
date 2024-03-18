from bs4 import BeautifulSoup
import requests
import pandas as pd
from datetime import date, timedelta, datetime

csv_path = 'tsa_market/data/tsa_checkins.csv'


csv_path = 'tsa_market/data/tsa_checkins.csv'

def _get_tsa_df(year : int) -> pd.DataFrame:
    """Returns the table as pandas DataFrame for corresponding year from offical TSA website"""
    
    ext = f"/{year}" if year else ''
    
    url = 'https://www.tsa.gov/travel/passenger-volumes' + ext
    
    df = pd.DataFrame(columns=['Date', 'Checkins'])
    
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    table = soup.find_all('table')[0]
    
    
    for row in table.find_all('tr')[1:]:
        row_data = row.find_all('td')
        entry = [td.text.replace(",", "").rstrip() for td in row_data]
        df.loc[len(df)] = entry[:2]
        
    
    if year: return df 
    return df.iloc[::-1].reset_index(drop=True)

def create_tsa_data():
    """Creates a pandas data frame for tsa checkin. Will always be missing day before yesterday's data"""
    
    checkins = pd.DataFrame(columns=['Date', 'Checkins'])

    for year in list(range(2019, date.today().year)) + [None]:
        
        checkins = pd.concat([checkins, _get_tsa_df(year)], ignore_index=True, axis=0)
    
    checkins.to_csv(csv_path)
    
    return checkins
    
def update(filename : str =csv_path) -> pd.DataFrame:
    """Updates an already existing file, only works if data from other years is complete, else that data will stay missing"""
    checkins = pd.read_csv(filename, index_col=0)

    start_date = datetime.strptime(
        checkins['Date'].iloc[-1], '%m/%d/%Y')
    end_date = date.today() + timedelta(days=-1)
    
    
    if start_date.date() >= end_date:
        print("Data up to date")
        return
     

    tsa_data = _get_tsa_df(None)
    tsa_data['Date'] = pd.to_datetime(tsa_data['Date'])
    
    missing_range = tsa_data[tsa_data['Date'] > start_date].copy()
    missing_range['Date'] = missing_range['Date'].dt.strftime('%#m/%#d/%Y')
    
    checkins = pd.concat([checkins, missing_range], ignore_index=True, axis=0)
    checkins.to_csv(filename)
    return checkins


if __name__ == '__main__':
    print(update())
