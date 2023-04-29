#Import modules
from bs4 import BeautifulSoup
import pandas as pd
import requests
from time import sleep
from datetime import date
import datetime as dt
from sqlalchemy import create_engine
import creds as creds

#Create engine and connection to postgresql database
engine = create_engine(f'postgresql://{creds.DB_USER}:{creds.DB_PASS}@{creds.DB_HOST}:{creds.DB_PORT}/{creds.DB_NAME}',echo=False)
conn = engine.connect()

# Create request for ufc scorecard website 
website = 'http://mmadecisions.com/decisions-by-event/'
result = requests.get(website)
content = result.text
soup = BeautifulSoup(content, 'lxml')

root = 'http://mmadecisions.com/'
year_links = ['decisions-by-event/2023']

#select table with years listed and table with fight cards with href links
table_of_years = soup.select("a[href*=decisions-by-event]")
table_of_fight_cards = soup.select("a[href*=UFC]")

#Create empty lists to be appended to
judges = []
fighter1 = []
fighter2 = []
fight_card_data_frame = []
fight_card_date = []
fight_card_links = []
fight_links = []
fight_date_cleaned = []

#Create dataframe and headers
Headers = ['round', 'fighter_1_score', 'fighter_2_score']
df = pd.DataFrame(columns = Headers)

#Create headers for second dataframe
headers2 = ['judge', 'fighter_1', 'fighter_2', "fight_card", "fight_date"]

#create dataframe to extract from database
db = pd.read_sql('SELECT fight_date FROM fight_scorecards', conn)

def get_year_links():
    for year in table_of_years[:1]:
        if year['href'] != 'decisions-by-event/':
            year_links.append(year['href'])
        else: pass
    return

def get_fight_card_links():
    
    for year in year_links[:1]:
        yearresult = requests.get(root + year + '/')
        yearcontent = yearresult.text
        yearsoup = BeautifulSoup(yearcontent, 'lxml')
        
        #get the fight_card links
        fight_card = yearsoup.select("a[href*=UFC]")
        #append fights to list
        for card in fight_card[:1]:
            fight_card_links.append(card['href'])
    return 

def get_fight_links():
    for card_link in fight_card_links:
        cardresult = requests.get(root + card_link)
        cardcontent = cardresult.text
        cardsoup = BeautifulSoup(cardcontent, 'lxml')
        
        #Find the link to each individual fight on each card
        
        fight_links_table = cardsoup.select("a[href*=vs]")
        
        for fight in fight_links_table:
            fight_links.append(fight['href'].strip())
    return
        
def get_scorecards():        
    #follow each fight link to the scorecard page
    for fight_link in fight_links:
        scorecardresult = requests.get(root + fight_link)
        scorecard_content = scorecardresult.text
        scorecardsoup = BeautifulSoup(scorecard_content, 'lxml')
        tables = scorecardsoup.find_all('table', style="border-spacing: 1px; width: 100%")
        for table in tables:
            for row in table.find_all('tr')[2:-1]:
                data = row.find_all('td')
                row_data = [td.text.strip() for td in data]
                length = len(df)
                df.loc[length] = row_data


        for table in tables:
            for row in table.find_all('tr')[2:-1]:
                judges.append(table.find('td', class_='judge').text.strip())
                fighter1.append(table.select('td.top-cell b')[0].text.strip())
                fighter2.append(table.select('td.top-cell b')[1].text.strip())
                fight_card_data_frame.append(scorecardsoup.find('td', class_='decision-top2').find('a').text.strip())
                fight_card_date.append(scorecardsoup.find('td', class_='decision-top2').text.strip())
                length = len(df)
    sleep(5)
    return

def clean_scorecards():
    for day in fight_card_date:
        fight_date_cleaned.append(day.replace('\t', '').replace('\r','').replace('\xa0', ' ').replace('\n',';').split(';')[1])


get_year_links()
get_fight_card_links()
get_fight_links()
get_scorecards()
clean_scorecards()

df['fighter_1_score'] = df['fighter_1_score'].replace('-', 0)
df['fighter_2_score'] = df['fighter_2_score'].replace('-', 0)
df2 = pd.DataFrame(list(zip(judges,fighter1,fighter2,fight_card_data_frame, fight_date_cleaned)), columns=headers2)
df2['fight_date'] = pd.to_datetime(df2['fight_date']).dt.strftime('%Y-%m-%d')
df2['fight'] = df2['fighter_1'] + " vs. " + df2['fighter_2']
combined_dfs = pd.merge(df,df2, left_index=True, right_index=True)

for index, row in combined_dfs.iterrows():
    try:
        if row['fight_date'] in db.values:
            print('already in dataset')
        else: 
            round = row[0]
            fighter_1_score = row[1]
            fighter_2_score = row[2]
            judge = row[3]
            fighter_1 = row[4]
            fighter_2 = row[5]
            fight_card = row[6]
            fight_date = row[7]
            fight = row[8]
            conn.execute(f'''INSERT INTO fight_scorecards ("round", "fighter_1_score", "fighter_2_score", "judge", "fighter_1", "fighter_2", "fight_card", "fight_date", "fight") 
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)''', 
                (round, fighter_1_score, fighter_2_score, judge, fighter_1, fighter_2, fight_card, fight_date, fight))
    except: pass