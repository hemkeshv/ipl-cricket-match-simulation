#Silhouette analysis can be used to study the separation distance 
#between the resulting clusters. The silhouette plot displays a measure of how
#close each point in one cluster is to points in the neighboring clusters and 
#thus provides a way to assess parameters like number of clusters visually.
#Silhouette coefficients, near +1 indicate that the sample 
#is far away from the neighboring clusters. A value of 0 indicates that the sample is on or very close
#to the decision boundary between two neighboring clusters and negative values indicate that those samples 
#might have been assigned to the wrong cluster.
#Also from the thickness of the silhouette plot the cluster size can be visualized. 
#We get a graph in which we can see purtabations which indicated osciallations. When the amount of purtabations
#reduce and start forming a straight line it indicates that thats the appropriate number of clusters.


library(cluster)
k.max <- 50 // max number of clusters 50
data <- read.csv('/Users/Poojabalusani/Desktop/bat.csv')//input data
sil <- rep(0, k.max)//iterating from 0 to max number of clusters to be created
for(i in 2:k.max){
  km.res <- kmeans(data, centers = i, nstart = 25)
  ss <- silhouette(km.res$cluster, dist(data))
  sil[i] <- mean(ss[, 3])
}
# Plot the  average silhouette width
plot(1:k.max, sil, type = "b", pch = 19, 
     frame = FALSE, xlab = "Number of clusters k")
abline(v = which.max(sil), lty = 2)

