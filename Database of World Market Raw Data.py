import datetime
import mysql.connector

def Symbol_Candle_Read (EnglishSymbol):

    path = "D:\\Bourse\\Advanced Get 9.1\\Noavaran Amin\\NoavaranAmin\\ExportedFile\\WorldMarketIndices\\" + EnglishSymbol + ".txt"
    f = open(path, 'r')
    lines = f.readlines()

    data = list()

    for i in range(1, len(lines)):
        line = lines[i]
        line = line.strip()
        a = line.find(',')

        a = a + 3
        line = line[a:]
        line = line[:8] + line[15:]                                             # Making arrays strip
        values = [int(float(j)) for j in line.split(',')]
        data.append(values)                                 # data = [date, open, high, low, close, volume]

    return data

def Start_Date_Finder(Stock, Date_Start):
    
    Released_Date_int = Price_Data[0][0]
    y = Released_Date_int//10**4
    m = (Released_Date_int%10**4)//10**2
    d = Released_Date_int % 10**2
    Released_Date = datetime.date(y, m, d)                      # Released date of the index

    Start_Date = max(Date_Start, Released_Date)

    cursor.execute('SELECT max(Date) FROM world_market_raw_data WHERE Name = \'%s\';' %Stock)
    result = cursor.fetchall()
    
    if result[0][0] != None:
        Start_Date = result[0][0] + OneDayDelta                 # If this stock data were inserted 
    
    return Start_Date

def Date_obj2int(date):
    return 10000*date.year + 100*date.month + date.day

def Find_Price_And_Date(date_int, StockPrice):
    output = None

    for line in StockPrice:
        if line[0] == date_int:
            output = line[1:]               # output = [open, high, low, close, volume]

    return output

def market_names():
    
    Stock_Name_Path = 'C:\\Users\\Hessum\\OneDrive\\Bourse\\Stock Market Python\\Financial Market Projects\\World-Market-Database\\World Market.txt'
    Stock_Name_Per2En = open(Stock_Name_Path, 'r')
    lines = Stock_Name_Per2En.readlines()
    Per2En = {}

    for line in lines:
        line = line.strip()
        values = line.split(',')
        Per2En[values[1]] = values[0]

    return Per2En

Name2Address = market_names()

Date_Start = datetime.date(1980, 3, 21)                                         # Start Date
Date_Stop = datetime.date.today()                                               # Stop Date

OneDayDelta = datetime.timedelta(days=1)                                        # Make a one-day delta time

cnx = mysql.connector.connect(user='root', password='harchi',
                              host='127.0.0.1',
                              database='market')
cursor = cnx.cursor()

for item in Name2Address:
    
    Name = item
    Address = Name2Address[item]
    Price_Data = Symbol_Candle_Read (Address)
    Date = Start_Date_Finder(Name, Date_Start)
    print(Name, Date)
    
    while Date <= Date_Stop:

        Market_Date_Info = []

        Market_Date_Info.append(Name)                                       # add market name
        Market_Date_Info.append(Address)                                    # add address name of the market
        Market_Date_Info.append(Date.isoformat())                           # add date in iso format
        Date_int = Date_obj2int(Date)                                       # convert Gregorian date as an object to an integer
        Price = Find_Price_And_Date(Date_int, Price_Data)                   # price data of a specific date
        
        if Price == None:                                                   # if there isn't any data of that date skip it 
            Date += OneDayDelta
            continue

        Market_Date_Info.extend(Price)                                       # add adjusted open high low close prices and total volume to the list
        
        format_strings = ','.join(['%s'] * len(Market_Date_Info))
        cursor.execute('INSERT INTO world_market_raw_data VALUES (%s)' % format_strings, tuple(Market_Date_Info))
        cnx.commit()
        
        Date += OneDayDelta