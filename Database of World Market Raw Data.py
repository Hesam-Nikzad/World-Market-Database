import datetime
import mysql.connector

def Symbol_Candle_Read (EnglishSymbol):

    path = "D:\\Bourse\\Advanced Get 9.1\\Noavaran Amin\\NoavaranAmin\\ExportedFile\\Index\\" + EnglishSymbol + ".txt"
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

def Start_Date_Finder(Stock):
    
    Released_Date_int = Price_Data[0][0]
    y = Released_Date_int//10**4
    m = (Released_Date_int%10**4)//10**2
    d = Released_Date_int % 10**2
    Released_Date = datetime.date(y, m, d)                      # Released date of the index

    Start_Date = max(GregorianDate_start, Released_Date)

    cursor.execute('SELECT max(Gregorian_Date) FROM raw_data WHERE EnglishSymbol = \'%s\';' %Stock)
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


Date_Start = datetime.date(1980, 3, 21)                                         # Start Date
Date_Stop = datetime.date.today()                                               # Stop Date

OneDayDelta = datetime.timedelta(days=1)                                        # Make a one-day delta time

cnx = mysql.connector.connect(user='root', password='harchi',
                              host='127.0.0.1',
                              database='market')
cursor = cnx.cursor()
