import requests
import pandas as pd
import io

ANGEL_SCRIP_URL="https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
NSE_CSV_URL = "https://archives.nseindia.com/content/indices/ind_nifty50list.csv"
BROWSER_HEADER = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36"
}

def get_nifty_symbols_from_nse():
    print("Step 1: Downloading Nifty 50 list from NSE website...")
    
    #Download the csv file from NSE, we pass browser_header so NSE does not block us
    response=requests.get(NSE_CSV_URL, headers=BROWSER_HEADER, timeout=15)
    
    #IF NSE returned an error , stop here and show the error
    response.raise_for_status()

    #We convert it to a pandas table so we can read columns easily
    #pandas.read_csv(response.text)   # WRONG - pandas thinks this is a file path
    #Pandas will think response.text is a file path name and try to open a file with that name — which does not exist. It will crash.

    # io.StringIO() wraps the string
    # now pandas thinks it is reading from a file
    # but actually it is reading from memory

    df=pd.read_csv(io.StringIO(response.text)) # when we do request.get(url) , the response comes back as a text string like : Company name, Industry, Symbol etc

    
    # The CSV has a column called 'Symbol' with stock names like RELIANCE, INFY
    # .str.strip() removes any extra spaces before or after the name
    # .tolist() converts the column to a simple Python list
    symbol_list = nse_table['Symbol'].str.strip().tolist()
    print("NSE gave us: "+str(len(symbol_list))+" stocks")
    return symbol_list

def get_tokens_from_angel_one(symbol_list):
    #this function download Angel one ScripMaster & finds the token number for each stocks in our list
    #Toke number is Angel One's internal ID for each stock, Websocket needs this token no. to subscribe to live prices
    print("Step 2: Downloading ScripMaster from Angel One...")
    
    #Downloading the ScripMaster JSON from Angel One
    response=requests.get(ANGEL_SCRIP_URL, timeout=30)
    response.raise_for_status()

    #Convert the JSON to a pandas table
    all_instruments=pandas.DataFrame(response.json())
    print("Angel One ScripMaster has "+str(len(all_instruments))+" instruments")

    #Angel one stores NSE equity stock with -EQ at the end, so Reliance in NSE list becomes Reliance-EQ in ScripMaster
    #We add -EQ to every symbol in our list
    symbol_list_eq=[]
    for symbol in symbol_list:
        symbol_list_eq.append(symbol+'-EQ')

    #Now filter the big table to keep only our 50 stocks
    #Condition 1: exch_seg must be NSE, Con. 2: instrumenttype must be -EQ, Con. 3: Symbol must be in our list of 5 stocks
    condition1=all_instruments['exch_seg']=='NSE'
    condition2=all_instruments['instrumenttype']=='EQ'
    condition3=all_instruments['symbol'].isin(symbol_list_eq)


    nifty50_instruments=all_instruments[condition1 & condition2 & condition3] 

    #Keep only 3 columns we need: symbol, token, name
    nifty50_instruments=nifty50_instruments[['symbol','token','name']]

    #Reset index numbers from 0 to 49
    nifty50_instruments=nifty50_instruments.reset_index(drop=True)

    print("Found "+str(len(nifty50_instruments))+" Nifty 50 stocks in ScripMaster")
    print(nifty50_instruments.to_string())

    #Convert table to list of dictionaries
    #Each dict looks like: {'symbol':"Reliance-EQ","token":"2885","name":"RELIANCE"}  
    result=nifty50_instruments.to_dict(orient='records')
    return result


def fetch_all_nifty50():
     #This is the main function that other files will call
     #It combines both steps above and returns the final instrument list
     #Step 1: Get stock names from NSE
     symbol_list=get_nifty50_symbols_from_nse()

     #Step 2:Get token names from Angel One
     instruments=get_tokens_from_angel_one(symbol_list)

     return instruments

     