import json
import requests

url = "https://stock-market-data.p.rapidapi.com/market/index/nasdaq-one-hundred"

headers = {
	"X-RapidAPI-Key": "8baa973a9bmshd904d3de602f169p1208fdjsna9fc305ae5c4",
	"X-RapidAPI-Host": "stock-market-data.p.rapidapi.com"
}

# url request
response = requests.request("GET", url, headers=headers)
# we get data in text format
data = response.text
# converting text to json format
company_names = json.loads(data)

# storing in a list of company
company_name = company_names['stocks']


