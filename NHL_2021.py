# This program gets the most recent NHL scores & updates the dataframe

import matplotlib.pyplot as plt
import seaborn as sns
import datetime
import pandas as pd
import numpy as np

import sqlalchemy
from sqlalchemy import create_engine, func
from sqlalchemy.orm import Session
from sqlalchemy.ext.automap import automap_base

import sqlite3 as sq

import warnings
warnings.filterwarnings("ignore")

# LOAD 2021 DATA FROM THE INTERNET:

season2021 = pd.read_html(
    'https://www.hockey-reference.com/leagues/NHL_2021_games.html')

# Convert to DataFrame:
season2021 = season2021[0].dropna(axis=0, thresh=4)

# Set to filter only through yesterday's date:
today = datetime.date.today()
yesterday = str(today - datetime.timedelta(days=1))

season2021 = season2021[season2021['Date'] <= yesterday]

# TRANSFORM THE DATA:

# Remove 'Att.','LOG', & 'Notes':
season2021.drop(columns=['Att.', 'LOG', 'Notes'], axis=1, inplace=True)

# Change names of goals columns & final score type:
season2021 = season2021.rename(
    columns={'G': 'VG', 'G.1': 'HG', 'Unnamed: 5': 'Type'})

# Fill missing Type values to 'Regulation':
season2021['Type'] = season2021['Type'].fillna('Regulation')

# Change the goal values to integers:
season2021[['HG', 'VG']] = season2021[['HG', 'VG']].astype('int')

# Insert season year column:
season2021.insert(1, 'SeasonYear', '2021')

# Insert season type column:
season2021.insert(2, 'SeasonType', 'Regular')

# APPLY POINTS AND FIX THE COLUMN ORDER:

# Type = Regular Season / Points Awarded, Regulation Home Win:
season2021.loc[(season2021['HG'] > season2021['VG']) & (
    season2021['Type'] == 'Regulation'), 'HP'] = 2
season2021.loc[(season2021['HG'] > season2021['VG']) & (
    season2021['Type'] == 'Regulation'), 'VP'] = 0

# Type = Regular Season / Points Awarded, Regulation Visitor Win:
season2021.loc[(season2021['VG'] > season2021['HG']) & (
    season2021['Type'] == 'Regulation'), 'VP'] = 2
season2021.loc[(season2021['VG'] > season2021['HG']) & (
    season2021['Type'] == 'Regulation'), 'HP'] = 0

# Type = Regular Season / Points Awarded, Overtime Home Win:
# Remove 'Regulation' from list of overtime types (OT, SO, 2OT, 3OT, 4OT, 5OT):
OT_type = list(season2021['Type'].unique())
OT_type.pop(0)
for i in OT_type:
    season2021.loc[(season2021['HG'] > season2021['VG'])
                   & (season2021['Type'] == i), 'HP'] = 2
    season2021.loc[(season2021['HG'] > season2021['VG'])
                   & (season2021['Type'] == i), 'VP'] = 1
    season2021.loc[(season2021['VG'] > season2021['HG'])
                   & (season2021['Type'] == i), 'VP'] = 2
    season2021.loc[(season2021['VG'] > season2021['HG'])
                   & (season2021['Type'] == i), 'HP'] = 1

# Type = OT / HG == VG (Overtime Tie):
season2021.loc[(season2021['HG'] == season2021['VG']) &
               (season2021['Type'] == 'OT'), 'HP'] = 1

# Change data type of points columns from float to int:
season2021[['HP', 'VP']] = season2021[['HP', 'VP']].astype('int')

# Change order of columns:
season2021 = season2021[['Date', 'SeasonYear', 'SeasonType',
                         'Type', 'Home', 'HG', 'HP', 'VP', 'VG', 'Visitor']]


# CREATE SQLITE DATABASE FOR DATA

sql_data = 'NHL_Records.sqlite'

# Create connection & push the data:

conn = sq.connect(sql_data)
cur = conn.cursor()

cur.executescript('''

DROP TABLE IF EXISTS "reg_2021";
CREATE TABLE "reg_2021" (
	"index" INTEGER PRIMARY KEY AUTOINCREMENT,
	"Date" DATE NOT NULL,
	"SeasonYear" CHAR NOT NULL,
	"SeasonType" CHAR NOT NULL,
	"Type" CHAR NOT NULL,
    "Home" CHAR NOT NULL,
    "HG" INTEGER NOT NULL,
    "HP" INTEGER NOT NULL,
    "VP" INTEGER NOT NULL,
    "VG" INTEGER NOT NULL,
    "Visitor" CHAR NOT NULL
);

''')

# COMMIT THE CODE:

# conn.commit()
season2021.to_sql("reg_2021", conn, if_exists='append', index=True)

conn.commit()
conn.close()

# Reflect the Tables into SQLAlchemy ORM
engine = create_engine("sqlite:///NHL_Records.sqlite")
# Reflect an existing database into a new model:
Base = automap_base()
# Reflect the tables:
Base.prepare(engine, reflect=True)
# Save reference to the combined table:
reg_2021 = Base.classes.reg_2021

# QUERY RESULTS FROM SQLITE:

# Read sqlite query results into a pandas DataFrame
con = sq.connect("NHL_Records.sqlite")

# Import NHL Season Predators + Cup Winners:
preds = pd.read_sql_query("\
SELECT * FROM reg_2000 \
WHERE Home == 'Nashville Predators' \
OR Home == 'New Jersey Devils' \
OR Visitor == 'Nashville Predators' \
OR Visitor == 'New Jersey Devils' \
\
UNION \
\
SELECT * FROM reg_2001 \
WHERE Home == 'Nashville Predators' \
OR Home == 'Colorado Avalanche' \
OR Visitor == 'Nashville Predators' \
OR Visitor == 'Colorado Avalanche' \
\
UNION \
\
SELECT * FROM reg_2002 \
WHERE Home == 'Nashville Predators' \
OR Home == 'Detroit Red Wings' \
OR Visitor == 'Nashville Predators' \
OR Visitor == 'Detroit Red Wings' \
\
UNION \
\
SELECT * FROM reg_2003 \
WHERE Home == 'Nashville Predators' \
OR Home == 'New Jersey Devils' \
OR Visitor == 'Nashville Predators' \
OR Visitor == 'New Jersey Devils' \
\
UNION \
\
SELECT * FROM reg_2004 \
WHERE Home == 'Nashville Predators' \
OR Home == 'Tampa Bay Lightning' \
OR Visitor == 'Nashville Predators' \
OR Visitor == 'Tampa Bay Lightning' \
\
UNION \
\
SELECT * FROM reg_2006 \
WHERE Home == 'Nashville Predators' \
OR Home == 'Carolina Hurricanes' \
OR Visitor == 'Nashville Predators' \
OR Visitor == 'Carolina Hurricanes' \
\
UNION \
\
SELECT * FROM reg_2007 \
WHERE Home == 'Nashville Predators' \
OR Home == 'Anaheim Ducks' \
OR Visitor == 'Nashville Predators' \
OR Visitor == 'Anaheim Ducks' \
\
UNION \
\
SELECT * FROM reg_2008 \
WHERE Home == 'Nashville Predators' \
OR Home == 'Detroit Red Wings' \
OR Visitor == 'Nashville Predators' \
OR Visitor == 'Detroit Red Wings' \
\
UNION \
\
SELECT * FROM reg_2009 \
WHERE Home == 'Nashville Predators' \
OR Home == 'Pittsburgh Penguins' \
OR Visitor == 'Nashville Predators' \
OR Visitor == 'Pittsburgh Penguins' \
\
UNION \
\
SELECT * FROM reg_2010 \
WHERE Home == 'Nashville Predators' \
OR Home == 'Chicago Blackhawks' \
OR Visitor == 'Nashville Predators' \
OR Visitor == 'Chicago Blackhawks' \
\
UNION \
\
SELECT * FROM reg_2011 \
WHERE Home == 'Nashville Predators' \
OR Home == 'Boston Bruins' \
OR Visitor == 'Nashville Predators' \
OR Visitor == 'Boston Bruins' \
\
UNION \
\
SELECT * FROM reg_2012 \
WHERE Home == 'Nashville Predators' \
OR Home == 'Los Angeles Kings' \
OR Visitor == 'Nashville Predators' \
OR Visitor == 'Los Angeles Kings' \
\
UNION \
\
SELECT * FROM reg_2013 \
WHERE Home == 'Nashville Predators' \
OR Home == 'Chicago Blackhawks' \
OR Visitor == 'Nashville Predators' \
OR Visitor == 'Chicago Blackhawks' \
\
UNION \
\
SELECT * FROM reg_2014 \
WHERE Home == 'Nashville Predators' \
OR Home == 'Los Angeles Kings' \
OR Visitor == 'Nashville Predators' \
OR Visitor == 'Los Angeles Kings' \
\
UNION \
\
SELECT * FROM reg_2015 \
WHERE Home == 'Nashville Predators' \
OR Home == 'Chicago Blackhawks' \
OR Visitor == 'Nashville Predators' \
OR Visitor == 'Chicago Blackhawks' \
\
UNION \
\
SELECT * FROM reg_2016 \
WHERE Home == 'Nashville Predators' \
OR Home == 'Pittsburgh Penguins' \
OR Visitor == 'Nashville Predators' \
OR Visitor == 'Pittsburgh Penguins' \
\
UNION \
\
SELECT * FROM reg_2017 \
WHERE Home == 'Nashville Predators' \
OR Home == 'Pittsburgh Penguins' \
OR Visitor == 'Nashville Predators' \
OR Visitor == 'Pittsburgh Penguins' \
\
UNION \
\
SELECT * FROM reg_2018 \
WHERE Home == 'Nashville Predators' \
OR Home == 'Washington Capitals' \
OR Visitor == 'Nashville Predators' \
OR Visitor == 'Washington Capitals' \
\
UNION \
\
SELECT * FROM reg_2019 \
WHERE Home == 'Nashville Predators' \
OR Home == 'St. Louis Blues' \
OR Visitor == 'Nashville Predators' \
OR Visitor == 'St. Louis Blues' \
\
UNION \
\
SELECT * FROM reg_2020 \
WHERE Home == 'Nashville Predators' \
OR Home == 'Tampa Bay Lightning' \
OR Visitor == 'Nashville Predators' \
OR Visitor == 'Tampa Bay lightning' \
\
UNION \
\
SELECT * FROM reg_2021 \
WHERE Home == 'Nashville Predators' \
OR Visitor == 'Nashville Predators' \
", con).drop('index', 1)

# Import Cup Winners List:
winners = pd.read_sql_query("\
SELECT * FROM winners \
", con).drop('index', 1)

con.close()

# CALCULATE TOTAL POINTS:

# Total all cup winners home & away points in new column:
for SeasonYear, CupWinner in zip(winners.SeasonYear, winners.CupWinner):
    preds.loc[(preds['SeasonYear'] == SeasonYear) & (preds['Home'] == CupWinner),
              'WinnerTotal'] = preds['HP']
    preds.loc[(preds['SeasonYear'] == SeasonYear) & (preds['Visitor'] == CupWinner),
              'WinnerTotal'] = preds['VP']

# Total all cup winners home & away points in new column:
for SeasonYear, CupWinner in zip(winners.SeasonYear, winners.CupWinner):
    preds.loc[(preds['SeasonYear'] == SeasonYear) & (preds['Home'] == CupWinner),
              'WinnerTotal'] = preds['HP']
    preds.loc[(preds['SeasonYear'] == SeasonYear) & (preds['Visitor'] == CupWinner),
              'WinnerTotal'] = preds['VP']

# Total all Predators home & away points in new column:
for SeasonYear in (winners.SeasonYear):
    preds.loc[(preds['SeasonYear'] == SeasonYear) & (preds['Home'] == 'Nashville Predators'),
              'PredsTotal'] = preds['HP']
    preds.loc[(preds['SeasonYear'] == SeasonYear) & (preds['Visitor'] == 'Nashville Predators'),
              'PredsTotal'] = preds['VP']

# Cumulative Predators points totals by year:
preds['PredsTotal'] = preds.groupby('SeasonYear')['PredsTotal'].cumsum()

# Cumulative cup winners points totals by year:
preds['WinnerTotal'] = preds.groupby('SeasonYear')['WinnerTotal'].cumsum()

preds['PredsGame#'] = 0
preds['WinnerGame#'] = 0

preds['PredsGame#'] = preds[(preds['Home'] == 'Nashville Predators') | (preds['Visitor'] == 'Nashville Predators')]\
    .groupby('SeasonYear')['PredsGame#'].cumcount()+1

preds['WinnerGame#'] = preds[(preds['Home'] != 'Nashville Predators') & (preds['Visitor'] != 'Nashville Predators')]\
    .groupby('SeasonYear')['WinnerGame#'].cumcount()+1

# Plot the data


# CREATE NASHVILLE PREDATORS DATASETS FOR EACH SEASON:

def preds_chart(x):
    return preds[(preds['SeasonYear'] == str(x)) & ((preds['Home'] == 'Nashville Predators') |
                                                        (preds['Visitor'] == 'Nashville Predators'))][['SeasonYear', 'PredsGame#', 'PredsTotal']]

# CREATE CUP WINNERS DATAFRAMES FOR EACH SEASON:


def cup_chart(x):
    return preds[(preds['SeasonYear'] == str(x)) & ((preds['Home'] != 'Nashville Predators') &
                                                        (preds['Visitor'] != 'Nashville Predators'))][['SeasonYear', 'WinnerGame#', 'WinnerTotal']]
    
# PREDATORS POINTS COMPARED TO EVENTUAL CUP WINNERS GRAPH:


sns.set_style('white')
plt.figure(figsize=(13, 10), dpi=75)

plt.title('2021\nTotal Nashville Predators\nPoints Compared To\nEventual Cup Winners\nBy Year',
          size=25, y=.65, x=.25, fontweight='bold')


ax_P21 = sns.lineplot(x='PredsGame#', y='PredsTotal',
                      data=preds_chart(2021), label='Preds2021', linewidth=3)

ax_W00 = sns.scatterplot(x='WinnerGame#', y='WinnerTotal',
                         data=cup_chart(2000), label='Winner2000', alpha=0.4)
ax_W01 = sns.scatterplot(x='WinnerGame#', y='WinnerTotal',
                         data=cup_chart(2001), label='Winner2001', alpha=0.4)
ax_W02 = sns.scatterplot(x='WinnerGame#', y='WinnerTotal',
                         data=cup_chart(2002), label='Winner2002', alpha=0.4)
ax_W03 = sns.scatterplot(x='WinnerGame#', y='WinnerTotal',
                         data=cup_chart(2003), label='Winner2003', alpha=0.4)
ax_W04 = sns.scatterplot(x='WinnerGame#', y='WinnerTotal',
                         data=cup_chart(2004), label='Winner2004', alpha=0.4)
ax_W06 = sns.scatterplot(x='WinnerGame#', y='WinnerTotal',
                         data=cup_chart(2006), label='Winner2006', alpha=0.4)
ax_W07 = sns.scatterplot(x='WinnerGame#', y='WinnerTotal',
                         data=cup_chart(2007), label='Winner2007', alpha=0.4)
ax_W08 = sns.scatterplot(x='WinnerGame#', y='WinnerTotal',
                         data=cup_chart(2008), label='Winner2008', alpha=0.4)
ax_W09 = sns.scatterplot(x='WinnerGame#', y='WinnerTotal',
                         data=cup_chart(2009), label='Winner2009', alpha=0.4)
ax_W10 = sns.scatterplot(x='WinnerGame#', y='WinnerTotal',
                         data=cup_chart(2010), label='Winner2010', alpha=0.4)
ax_W11 = sns.scatterplot(x='WinnerGame#', y='WinnerTotal',
                         data=cup_chart(2011), label='Winner2011', alpha=0.4)
ax_W12 = sns.scatterplot(x='WinnerGame#', y='WinnerTotal',
                         data=cup_chart(2012), label='Winner2012', alpha=0.4)
ax_W13 = sns.scatterplot(x='WinnerGame#', y='WinnerTotal',
                         data=cup_chart(2013), label='Winner2013', alpha=0.4)
ax_W14 = sns.scatterplot(x='WinnerGame#', y='WinnerTotal',
                         data=cup_chart(2014), label='Winner2014', alpha=0.4)
ax_W15 = sns.scatterplot(x='WinnerGame#', y='WinnerTotal',
                         data=cup_chart(2015), label='Winner2015', alpha=0.4)
ax_W16 = sns.scatterplot(x='WinnerGame#', y='WinnerTotal',
                         data=cup_chart(2016), label='Winner2016', alpha=0.4)
ax_W17 = sns.scatterplot(x='WinnerGame#', y='WinnerTotal',
                         data=cup_chart(2017), label='Winner2017', alpha=0.4)
ax_W18 = sns.scatterplot(x='WinnerGame#', y='WinnerTotal',
                         data=cup_chart(2018), label='Winner2018', alpha=0.4)
ax_W19 = sns.scatterplot(x='WinnerGame#', y='WinnerTotal',
                         data=cup_chart(2019), label='Winner2019', alpha=0.4)
ax_W20 = sns.scatterplot(x='WinnerGame#', y='WinnerTotal',
                         data=cup_chart(2020), label='Winner2020', alpha=0.4)


ax_P21.set_xlabel('Total Games Played', size=18, color='red')
ax_P21.set_ylabel('Total Points', size=18, color='red')

plt.xlim([10, (len(preds_chart(2021))+5)])
plt.ylim([0, 90])

plt.tight_layout()
plt.legend(bbox_to_anchor=(1, 1), loc='upper left')
plt.subplots_adjust(right=0.85)

RUNS = list(range(1, (len(preds_chart(2021))+1)))
plt.xticks(RUNS)

plt.show()

# PREDATORS 2021 POINTS COMPARED TO PAST SEASONS GRAPH:

sns.set_style('white')
plt.figure(figsize=(13, 10), dpi=75)

plt.title('Total\nNashville Predators\n2021 Points Compared\nTo The Last 20\nNashville Predators Seasons',
          size=25, y=.65, x=.25, fontweight='bold')

ax_P00 = sns.scatterplot(x='PredsGame#', y='PredsTotal',
                         data=preds_chart(2000), label='Preds2000', alpha=0.4)
ax_P01 = sns.scatterplot(x='PredsGame#', y='PredsTotal',
                         data=preds_chart(2001), label='Preds2001', alpha=0.4)
ax_P02 = sns.scatterplot(x='PredsGame#', y='PredsTotal',
                         data=preds_chart(2002), label='Preds2002', alpha=0.4)
ax_P03 = sns.scatterplot(x='PredsGame#', y='PredsTotal',
                         data=preds_chart(2003), label='Preds2003', alpha=0.4)
ax_P04 = sns.scatterplot(x='PredsGame#', y='PredsTotal',
                         data=preds_chart(2004), label='Preds2004', alpha=0.4)
ax_P06 = sns.scatterplot(x='PredsGame#', y='PredsTotal',
                         data=preds_chart(2006), label='Preds2006', alpha=0.4)
ax_P07 = sns.scatterplot(x='PredsGame#', y='PredsTotal',
                         data=preds_chart(2007), label='Preds2007', alpha=0.4)
ax_P08 = sns.scatterplot(x='PredsGame#', y='PredsTotal',
                         data=preds_chart(2008), label='Preds2008', alpha=0.4)
ax_P09 = sns.scatterplot(x='PredsGame#', y='PredsTotal',
                         data=preds_chart(2009), label='Preds2009', alpha=0.4)
ax_P10 = sns.scatterplot(x='PredsGame#', y='PredsTotal',
                         data=preds_chart(2010), label='Preds2010', alpha=0.4)
ax_P11 = sns.scatterplot(x='PredsGame#', y='PredsTotal',
                         data=preds_chart(2011), label='Preds2011', alpha=0.4)
ax_P12 = sns.scatterplot(x='PredsGame#', y='PredsTotal',
                         data=preds_chart(2012), label='Preds2012', alpha=0.4)
ax_P13 = sns.scatterplot(x='PredsGame#', y='PredsTotal',
                         data=preds_chart(2013), label='Preds2013', alpha=0.4)
ax_P14 = sns.scatterplot(x='PredsGame#', y='PredsTotal',
                         data=preds_chart(2014), label='Preds2014', alpha=0.4)
ax_P15 = sns.scatterplot(x='PredsGame#', y='PredsTotal',
                         data=preds_chart(2015), label='Preds2015', alpha=0.4)
ax_P16 = sns.scatterplot(x='PredsGame#', y='PredsTotal',
                         data=preds_chart(2016), label='Preds2016', alpha=0.4)
ax_P17 = sns.scatterplot(x='PredsGame#', y='PredsTotal',
                         data=preds_chart(2017), label='Preds2017', alpha=0.4)
ax_P18 = sns.scatterplot(x='PredsGame#', y='PredsTotal',
                         data=preds_chart(2018), label='Preds2018', alpha=0.4)
ax_P19 = sns.scatterplot(x='PredsGame#', y='PredsTotal',
                         data=preds_chart(2019), label='Preds2019', alpha=0.4)
ax_P20 = sns.scatterplot(x='PredsGame#', y='PredsTotal',
                         data=preds_chart(2020), label='Preds2020', alpha=0.4)
ax_P21 = sns.lineplot(x='PredsGame#', y='PredsTotal',
                      data=preds_chart(2021), label='Preds2021', linewidth=3)

ax_P21.set_xlabel('Total Games Played', size=18, color='red')
ax_P21.set_ylabel('Total Points', size=18, color='red')

plt.xlim([0, (len(preds_chart(2021))+5)])
plt.ylim([0, 86])

plt.tight_layout()
plt.legend(bbox_to_anchor=(1, 1), loc='upper left', ncol=1)

RUNS = list(range(1, (len(preds_chart(2021))+1)))
plt.xticks(RUNS)
plt.subplots_adjust(right=0.85)

plt.show()
