from bs4 import BeautifulSoup
import pandas as pd
import requests
from time import sleep
from datetime import date
from sqlalchemy import create_engine
import creds

#Create engine and connection to postgresql database
engine = create_engine(f'postgresql://{creds.DB_USER}:{creds.DB_PASS}@{creds.DB_HOST}:{creds.DB_PORT}/{creds.DB_NAME}',echo=False)
conn = engine.connect()

#create dataframe to extract from database
db = pd.read_sql('SELECT * FROM ufc_fights', conn)

# request ufc card website 
website = 'http://ufcstats.com/statistics/events/completed?page=all'
result = requests.get(website)
content = result.text
soup = BeautifulSoup(content, 'lxml')


#get the table of ufc card and links 
table = soup.find('table', {'class' : 'b-statistics__table-events'})

links = [link['href'] for link in table.find_all('a', href = True)]

#create the 2 dataframes. One for the fights, one for the stats
headers = ['W/L', 'Fighter', 'Kd', 'Str', 'Td', 'Sub', 'Weight class', 'Method', 'Round', 'Time']
df = pd.DataFrame(columns = headers)
df2_headers = ['Fight Card']
df2 = pd.DataFrame(columns = df2_headers)


#loop over the links following them to the fights table
for link in links[1:2]:
    try:
        result = requests.get(link)
        content = result.text
        soup = BeautifulSoup(content, 'lxml')
        fighttable = soup.find('table', {'class' : 'b-fight-details__table_style_margin-top'})
        fight_card = soup.find('span')
        print(fight_card)

        #loop over the fights and extract the table data into the dataframe
        for row in fighttable.find_all('tr')[1:]:
            data = row.find_all('td')
            row_data = [td.text.strip() for td in data]
            length = len(df)
            df.loc[length] = row_data
        #loop over the fights and extract the data into df2
        for row in fighttable.find_all('tr')[1:]:
            fight_card = soup.find('span').text
            df2.loc[len(df2)] = fight_card
        sleep(3)
    except: pass
#combine the two dataframes
combined_dfs = pd.merge(df,df2, left_index=True, right_index=True)


#columns to change
headers_to_change = ['Fighter', 'Kd', 'Str', 'Td', 'Sub', 'Method']

#Replace \n with a comma, and remove whitespace
combined_dfs['Fight Card'] = combined_dfs["Fight Card"].astype(str)
combined_dfs["Fight Card"]=combined_dfs['Fight Card'].str.replace('\n', '')

for column in combined_dfs.columns:
    combined_dfs[column] = combined_dfs[column].astype(str)

#Loop through columns and eliminate extra lines, splitting the data by comma
for column in headers_to_change:
    combined_dfs[column]=combined_dfs[column].str.replace('\n', ',',1)
    combined_dfs[column]=combined_dfs[column].str.replace('\n', '')
    combined_dfs[column]=combined_dfs[column].str.replace(' ', '')
    combined_dfs[column]=combined_dfs[column].str.replace('nc\n\n\nnc', 'nc')

combined_dfs['W/L'] =combined_dfs['W/L'].str.replace('win', 'Win,Loss')

#Create lists of columns to change and columns not to change
columns_to_change = ['W/L', 'Fighter', 'Kd', 'Str', 'Td', 'Sub', 'Method']
columns_not_to_change = ['Weight class', 'Round', 'Time', 'Fight Card']

#Add a space between First and last name of fighter
combined_dfs['Fighter'] = combined_dfs['Fighter'].str.replace( r"([A-Z])", r" \1").str.strip()

#Add a counter column
combined_dfs['counter'] = range(len(combined_dfs))

#split each column in the columns to change list
for column in columns_to_change:
    combined_dfs[column] = combined_dfs[column].str.split(',')





#explode columns
combined_dfs = combined_dfs.set_index(columns_not_to_change)
combined_dfs2 = combined_dfs


for column in columns_to_change:
    combined_dfs=combined_dfs.explode(column).drop_duplicates(subset='counter', keep='first')
    combined_dfs2=combined_dfs2.explode(column).drop_duplicates(subset='counter', keep='last')

df_winners = combined_dfs
df_winners = df_winners.reset_index()
df_losers = combined_dfs2
df_losers = df_losers.reset_index()



df_final = pd.concat([df_winners,df_losers], keys='counter')
df_final = df_final.sort_values(by='counter', ascending=True)
df_final=df_final.reset_index(drop=True)
df_final=df_final.drop(['counter'], axis=1)

print(df_final.columns)
#Output the data to database
for index, row in df_final.iterrows():
    if row['Fight Card'] in db.values:
        print('already in dataset')
    else: 
        try:
            weight_class = row[0]
            round = row[1]
            time = row[2]
            fight_card = row[3]
            wl = row[4]
            fighter = row[5]
            knockdowns = row[6]
            strikes =row[7]
            takedowns = row[8]
            submission = row[9]
            method = row[10]
            conn.execute(f'''INSERT INTO ufc_fights (
                weight_class,
                round,
                time,
                fight_card,
                "W/L",
                fighter,
                knockdowns,
                strikes, 
                takedowns,
                submission_attempts, 
                method) 
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''', (weight_class, round, time, fight_card, wl, fighter, knockdowns, strikes, takedowns,submission, method))
        except: pass        



