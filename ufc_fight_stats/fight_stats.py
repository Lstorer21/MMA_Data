#import packages
from bs4 import BeautifulSoup
import pandas as pd
import requests
from time import sleep
from datetime import date
import creds
from sqlalchemy import create_engine

#Create engine and connection to postgresql database
engine = create_engine(f'postgresql://{creds.DB_USER}:{creds.DB_PASS}@{creds.DB_HOST}:{creds.DB_PORT}/{creds.DB_NAME}',echo=False)
conn = engine.connect()

#create dataframe to extract from database
db = pd.read_sql('SELECT * FROM fight_stats', conn)

#Create Headers for DataFrames
Totals_headers = ['Fighter', 'KD', 'Sig. str.', 'Sig. str. %', 'Total str.', 'Td', 'Td %', 'Sub. att', 'Rev.', 'Ctrl']
Significant_Strikes_Headers = ['Fighter', 'Sig. str', 'Sig. str. %', 'Head', 'Body', 'Leg', 'Distance',	'Clinch', 'Ground']

##Lists to append to
Rounds = []
Fight_Card = []
Fight_Card_Links = []

#Create DataFrames
df1 = pd.DataFrame(columns = Totals_headers)
df2 = pd.DataFrame(columns = Significant_Strikes_Headers)


#Request ufc_stats website
website = 'http://ufcstats.com/statistics/events/completed?page=all'
result = requests.get(website)
content = result.text
statssoup = BeautifulSoup(content, 'lxml')


#Add all href links to Fight_Card_Links list
[Fight_Card_Links.append(link['href']) for link in statssoup.find_all('a', class_='b-link')]

# Follow all Links in Fight_Card_Links
for fight_card_link in Fight_Card_Links[1:2]:
    try:
        fight_card_result = requests.get(fight_card_link)
        fight_card_content = fight_card_result.text
        fight_card_soup = BeautifulSoup(fight_card_content, 'lxml')
        fight_links = []
        fights = fight_card_soup.select("a[href*=fight-details]")
        # Find all fight links
        [fight_links.append(link['href']) for link in fights if link['href'] not in fight_links]

        # Follow all fight links
        for fight_link in fight_links:
            fight_result = requests.get(fight_link)
            fight_content = fight_result.text
            fight_soup = BeautifulSoup(fight_content, 'lxml')
        #Create table for stat totals
            table1 = fight_soup.find_all('table', class_="b-fight-details__table")[0]
            #Create table for significant strikes
            table2 = fight_soup.find_all('table', class_="b-fight-details__table")[1]
            #Loop through stat totals adding data to dataframe
            for row in table1.find_all('tr', class_="b-fight-details__table-row")[1:]:
                data = row.find_all('td')
                row_data = [td.text.strip() for td in data]
                length = len(df1)
                df1.loc[length] = row_data
            for row in table2.find_all('tr', class_="b-fight-details__table-row")[1:]:
                data = row.find_all('td')
                row_data = [td.text.strip() for td in data]
                length = len(df2)
                df2.loc[length] = row_data        

            for round in table1.find_all('th', colspan=10):
                Rounds.append(round.text.strip())
                Fight_Card.append(fight_soup.find('h2').text.strip())   
    except:
        pass
    sleep(5)


df1["Rounds"] = Rounds
df1['Fight_Card'] = Fight_Card

df_combined = pd.merge(df1, df2, left_index=True,right_index=True)

#Drop duplicate columns
df_combined=df_combined.drop(['Fighter_y', 'Sig. str', 'Sig. str. %_y'], axis=1)

#Rename columns
df_combined = df_combined.rename(columns={
    'Fighter_x' : 'Fighter', 
    'KD' : 'Knockdowns',
    'Sig. str.' : 'Significant Strikes', 
    'Sig. str. %_x' : 'Significant Strike Percentage', 
    'Total str.' : 'Total Strikes', 
    'Td' : 'Takedowns', 
    'Td %' : 'Takedown Percentage',
    'Sub. att' : 'Submission Attempts',
    'Rev.' : 'Reversals',
    'Ctrl' : 'Control',
    'Fight_Card' : 'Fight Card'
    })

#Create list of headers to change
headers_to_change = ['Fighter', 'Knockdowns', 'Significant Strikes', 'Significant Strike Percentage', 
    'Total Strikes', 'Takedowns', 'Takedown Percentage', 'Submission Attempts', 'Reversals', 'Control', 'Head', 'Body', 'Leg', 'Distance', 'Clinch', 'Ground']

#Create list of headers that don't need to be changed
headers_not_to_change = ['Rounds', 'Fight Card']

#List of headers that need to be split into two columns
headers_to_split = ['Significant Strikes', 'Total Strikes', 'Takedowns', 'Head', 'Body', 'Leg', 'Distance', 'Clinch', 'Ground']


#Change column typ to string
for column in df_combined.columns:
    df_combined[column] = df_combined[column].astype(str)

#Loop through columns and eliminate extra lines, splitting the data by comma
for column in headers_to_change:
    df_combined[column] = df_combined[column].str.replace('\n', ',',1)
    df_combined[column] = df_combined[column].str.replace('\n', '')
    df_combined[column] = df_combined[column].str.replace(' ', '')

#Add a space between First and last name of fighter
df_combined['Fighter'] = df_combined['Fighter'].str.replace( r"([A-Z])", r" \1").str.strip()

#split each column in the columns to change list
for column in headers_to_change:
    df_combined[column] = df_combined[column].str.split(',')

#explode columns
df_combined = df_combined.set_index(headers_not_to_change)
df_combined['counter'] = range(len(df_combined))
df_combined=df_combined.explode('Fighter')


#create dataframe with winners, and another for losers to get rid of duplicate entries
df_winners = df_combined.drop_duplicates(subset='counter', keep='first')
df_losers = df_combined.drop_duplicates(subset='counter', keep='last')


#Explode winners dataframe
for column in headers_to_change[1:]:
    df_winners=df_winners.explode(column).drop_duplicates(subset='counter', keep='first')

df_winners = df_winners.reset_index()
#Explode Losers dataframe
for column in headers_to_change[1:]:
    df_losers=df_losers.explode(column).drop_duplicates(subset='counter', keep='last')

df_losers=df_losers.reset_index()

#create dataframe to concatenate the winner and losers dataframe back together
df_final = pd.concat([df_winners,df_losers], keys='counter')
df_final['counter'] =df_final['counter'].astype(int)
df_final = df_final.reset_index(drop=True)
df_final=df_final.sort_values('counter', ascending=True)
df_final = df_final.drop(['counter'], axis=1)

#Split column headers
for column in headers_to_split:
    df_final[column]=df_final[column].str.replace('of', ',')
    df_final[[f'{column} Strikes Landed', f'{column} Strikes Attempted']] = df_final[column].str.split(',', expand=True)
    df_final=df_final.drop(column, axis=1)

#Rename column headers
df_final=df_final.rename(columns={'Takedowns Strikes Landed' : 'Takedowns Landed', 
    'Takedowns Strikes Attempted' : 'Takedowns Attempted', 
    'Significant Strikes Strikes Landed' : 'Significant Strikes Landed',
    'Significant Strikes Strikes Attempted' : 'Significant Strikes Attempted',
    'Total Strikes Strikes Landed' : 'Total Strikes Landed',
    'Total Strikes Strikes Attempted' : 'Total Strikes Attempted'})

#Convert Percentage Strings to float
df_final['Takedown Percentage']=df_final['Takedown Percentage'].str.replace('---','0%')
df_final['Significant Strike Percentage']=df_final['Significant Strike Percentage'].str.replace('---','0%')
df_final['Takedown Percentage'] = df_final['Takedown Percentage'].str.rstrip("%").astype(float)/100
df_final['Significant Strike Percentage'] = df_final['Significant Strike Percentage'].str.rstrip("%").astype(float)/100
df_final = df_final.reset_index(drop=True)

print(df_final.columns)


#Output dataframe to SQL
for index, row in df_final.iterrows():
    if row[1] in db.values:
        print('already in dataset')
    else: 
        try:
            round = row[0]
            fight_card = row[1]
            fighter = row[2]
            knockdowns = row[3]
            significant_strike_percentage = row[4]
            takedown_percentage = row[5]
            submission_attempts = row[6]
            reversals = row[7]
            control = row[8]
            significant_strikes_landed = row[9]
            significant_strikes_attempted = row[10]
            total_strikes_landed = row[11]
            total_strikes_attempted = row[12]
            takedowns_landed = row[13]
            takedowns_attempted = row[14]
            head_strikes_landed = row[15]
            head_strikes_attempted = row[16]
            body_strikes_landed = row[17]
            body_strikes_attempted = row[18]
            leg_strikes_landed = row[19]
            leg_strikes_attempted = row[20]
            distance_strikes_landed = row[21]
            distance_strikes_attempted = row[22]
            clinch_strikes_landed = row[23]
            clinch_strikes_attempted = row[24]
            ground_strikes_landed = row[25]
            ground_strikes_attempted = row[26]

            conn.execute(f'''INSERT INTO fight_stats (
                rounds, 
                fight_card,
                fighter,
                knockdowns,
                significant_strike_percentage,
                takedown_percentage,
                submission_attempts,
                reversals,
                control,
                significant_strikes_landed, 
                significant_strikes_attempted,
                total_strikes_landed,
                total_strikes_attempted,
                takedowns_landed,
                takedowns_attempted,
                head_strikes_landed,
                head_strikes_attempted,
                body_strikes_landed,
                body_strikes_attempted,
                leg_strikes_landed,
                leg_strikes_attempted,
                distance_strikes_landed,
                distance_strikes_attempted,
                clinch_strikes_landed,
                clinch_strikes_attempted,
                ground_strikes_landed,
                ground_strikes_attempted) 
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''', 
                (round, 
                fight_card, 
                fighter,knockdowns,
                significant_strike_percentage, 
                takedown_percentage, 
                submission_attempts, 
                reversals, 
                control,
                significant_strikes_landed, 
                significant_strikes_attempted, 
                total_strikes_landed, 
                total_strikes_attempted, 
                takedowns_landed, 
                takedowns_attempted,
                head_strikes_landed, 
                head_strikes_attempted, 
                body_strikes_landed, 
                body_strikes_attempted, 
                leg_strikes_landed, 
                leg_strikes_attempted,
                distance_strikes_landed, 
                distance_strikes_attempted, 
                clinch_strikes_landed, 
                clinch_strikes_attempted, 
                ground_strikes_landed, 
                ground_strikes_attempted))
        except: pass