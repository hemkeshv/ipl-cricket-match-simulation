from bs4 import BeautifulSoup
import csv
import requests
url=requests.get('http://stats.espncricinfo.com/ipl2010/engine/records/averages/batting.html?id=5319;type=tournament')#The Url From Where we will be scraping player data for the year 2010
soup=BeautifulSoup(url.text)
f1= open('tst.txt', 'w')
f1.write(soup.prettify())

table=soup.find_all('table',{'class':'engineTable'})[0]#All the tables which belong to class engineTable
teams=table.find_all('tr',{'class':'note'})#All the rows that belong to class note
team=[]
for line in teams:#teams are extracted here and put in the list named team
   for l in line.findAll('td'):
     team.append(l.extract().text[1:len(l.extract().text)-1])

links=table.find_all('a',{'class':'data-link'})#All the links to player profiles are extracted
players={}
ctr=0
for l in links:
   key=l.extract().text+"_"+team[ctr]
   # key= MS Dhoni_CSK
   ctr+=1
   url=requests.get("http://www.espncricinfo.com/{0}?".format(l["href"]))#Getting to the appropriate player profile link
   soup=BeautifulSoup(url.text)
   tables=soup.find_all('table',{"class" :"engineTable"})[0:2]#finding all the tables for the player

   count=0
   col1=[]
   col2=[]
   cols=soup.find_all('tr',{"class" :"head"})#Extracting the column headings for the tables
   for col in cols:
    cells=col.find_all('th')
    for i in cells:
      count+=1;
      if count<=15:
        col1.append(i.text)#Batting table column headers
      elif count<=29:
        col2.append(i.text)#Bowling table column headers
   print(col1)
   print(col2)

   count=0
   stats1=[[],[],[],[],[],[]]#Batting table rows
   tests = tables[0].find_all('tr')
   for test in tests:
    cells = test.find_all('td')
    for i in cells:
      count=count+1;
      if count<=15:
        stats1[0].append(i.text)#tests
      elif count<=30:
        stats1[1].append(i.text)#ODIS
      elif count<=45:
        stats1[2].append(i.text)#T20Is
      elif count<=60:
        stats1[3].append(i.text)#First-Class
      elif count<=75:
        stats1[4].append(i.text)#List A
      elif count<=90:
        stats1[5].append(i.text)#Twenty20

   count=0
   stats2=[[],[],[],[],[],[]]#Bowling table rows
   tests = tables[1].find_all('tr')
   for test in tests:
    cells = test.find_all('td')
    for i in cells:
      count=count+1;
      if count<=14:
        stats2[0].append(i.text)#Tests
      elif count<=28:
        stats2[1].append(i.text)#ODIs
      elif count<=42:
        stats2[2].append(i.text)#T20Is
      elif count<=56:
        stats2[3].append(i.text)#First-Class
      elif count<=70:
        stats2[4].append(i.text)#List A
      elif count<=84:
        stats2[5].append(i.text)#Twenty20
#print(stats2)
   name=key+'.csv'#example: MSDhoni_ChennaiSuperKings.csv
   print(name)
   with open (name,'w') as file:
    writer=csv.writer(file)
    writer.writerow(col1)#write the headers for batting table
    for row in stats1:#write the rows for batting table
      writer.writerow(row)
    writer.writerow(col2)#write the headers for bowling table
    for row in stats2:
      writer.writerow(row)#write the rows for bowling table
