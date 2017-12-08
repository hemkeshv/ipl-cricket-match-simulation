Data Used -
Player Profiles IPL Data from 2008-2015

Procedure -

The clustering was done using Spark MLLib using K-Means clustering. The code is with Pooja.

Folder Details -
1. Input Data -
	batting.csv - Input for dividing the batsmen into clusters.
	Format - Batsman Name, Average Runs Scored, Average Strike Rate

	bowling.csv - Input for dividing the bowlers into clusters.
	Format - Bowler Name, Average Economy Rate, Average Strike Rate

2. Output Data -
	BattingClustered.csv - Output for Batsmen along with their respective clusters.
	BowlingClustered.csv - Output for Bowlers along with their respective clusters.

3. Clustering using Spark MLLib -
	Clustering.txt - Code to obtain the clusters for the players using Spark MLLib and K-Means Clustering.

4. Silhouette Method -
	Silhouette_code.R - Graphical analysis to justify the usage of 10 clusters for the purpose of clustering using R.
	Silhouette_Method.png - Graph