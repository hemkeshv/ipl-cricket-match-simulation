Steps to setup the environment before running (One Time Process)-

1. Create two folders in HDFS (/user/hadoop/input/TempStep1 and /user/hadoop/input/TempStep3). Eg: hadoop fs -mkdir /user/hadoop/input/TempStep1.

Copy the PlayerToPlayerProbabilites.txt into /user/hadoop/input/TempStep1 and Step3Output.txt into /user/hadoop/input/TempStep3. Eg: hadoop fs -copyFromLocal ~/Desktop/Step3Output.txt /user/hadoop/input/TempStep3/ 

2. Place the code IPLSimulation.java into the local Desktop (NOT HDFS).
For Mac Users: Place the file in /Users/hadoop/Desktop
For Ubuntu Users: Place the file in /home/<user_name>/Desktop.

3. Place the file clusterno.txt in the local Desktop (NOT HDFS).
Assuming the file is in ~/Desktop, change the contents of line 340 (FileReader part) in the code to reflect this path.

Steps to Run the Code (To be done for every match) -

1. A Hive query is to be made to get the cluster numbers for each player. Populate the file SampleInput.txt to reflect the batting and bowling orders of the desired match and load the file into HDFS.
hadoop fs -copyFromLocal SampleInput.txt /user/hadoop/

Follow the steps as mentioned in QueriesForClusterNums.txt and obtain the cluster numbers for each player. Copy the output from the terminal into the clusterno.txt file (Ctrl+C).

2. Change to the Desktop where the IPLSimulation.java is placed.
cd ~/Desktop

3. Compile the code -
javac -classpath $HADOOP_HOME/share/hadoop/common/hadoop-common-2.7.2.jar:$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.7.2.jar:$HADOOP_HOME/share/hadoop/common/lib/commons-cli-1.2.jar -d /Users/hadoop/Desktop IPLSimulation.java

(Change paths as required for Ubuntu users)

4. Create a folder called IPLSimulationFolder.

mkdir IPLSimulationFolder

5. Move all the generated class files into this folder.

mv *.class ./IPLSimulationFolder

6. Create the JAR file.

jar -cvf IPLSimulationFolder.jar -C /Users/hadoop/Desktop/IPLSimulationFolder .

(Note the dot at the end^)

7. Run the JAR File on MapReduce.

hadoop jar /Users/hadoop/Desktop/IPLSimulationFolder.jar IPLSimulationFolder

8. To view the output, view the contents of the part-r-00000 in the output folder created automatically on HDFS (called IPLSimulationOutput)

hadoop fs -cat /user/hadoop/IPLSimulationOutput/part-r-00000

9. Once output is generated, note down the observations in the shared spreadsheet.

10. Remove the output directory from HDFS in order to run the next iteration.

hadoop fs -rmr /user/hadoop/IPLSimulationOutput.

11. To run for a new match, go back to step 1. Only Step 1 will need to be repeated as the inputs change for every match. The rest of the steps should be the same.
