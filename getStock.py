from getCompany import company_name
import json
import csv
import pandas as pd
import requests

# changing the time format
def change_time_format(time):
    return time[:10]

# getting the stock of  each company
def get_historic_data(ticket_symbol):
    url = "https://stock-market-data.p.rapidapi.com/stock/historical-prices"

    querystring = {"ticker_symbol": ticket_symbol, "years": "1", "format": "json"}

    headers = {
        "X-RapidAPI-Key": "d1e438ac41msh43e59d0f1ff9ac3p1627c7jsn390f055bff1e",
        "X-RapidAPI-Host": "stock-market-data.p.rapidapi.com"
    }
    response = requests.request("GET", url, headers=headers, params=querystring)
    response = json.loads(response.text)
    response = response['historical prices']
    df = pd.DataFrame(response)
    date = df['Date'].apply(change_time_format)
    # changing the date format
    df['Date'] = date
    # adding the company column in the list of stock data
    df['Company'] = ticket_symbol
    file_name = str(ticket_symbol) + '.csv'
    df.to_csv(file_name)


# due to limit of access to url i used count to get 25 company data
count = 0

# from company_name in the list we get all company names
for company in company_name:
    get_historic_data(company)
    count += 1
    if count >= 25:
        break


