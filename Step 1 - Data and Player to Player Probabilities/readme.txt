IPL Player Data used for Clustering from Cricinfo in the Step 2 Folder.

Folder Details -
1. Ball by Ball Data - Contains ball by ball details about each IPL match from 2008-2015.
2. Player to Player Probabilities - Contains player to player probabilities which gives us the probability of a batsman scoring x runs vs that bowler and also the wicket probability.

	DataExtractProb.java - The MapReduce code used to generate these probabilities from the ball by ball data.
	PlayerToPlayerProbabilities.txt - Output file containing player to player probabilities.
	Format - Batsman Name, Bowler Name	P(0),P(1),P(2),P(3),P(4),P(6),P(Not Out)

3. Player Profiles (Scraped Data) - Consists of detailed statistics about each player in their respective IPL teams scraped from ESPNCricinfo. Contains ScrapePlayers.py which was used to scrape the data.