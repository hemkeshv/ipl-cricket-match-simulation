import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
public class IPLSimulation {
  public static class SimulationMapper1//MAPPER TO HANDLE PLAYER TO PLAYER PROBABILITIES
       extends Mapper<Object, Text, Text, Text>{
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			String bat1[]=new String[11];
  			String bowl1[]=new String[20];
  			String bat2[]=new String[11];
  			String bowl2[]=new String[20];
			for(int i=0;i<11;i++)//populating the arrays based on the config vars
			{
				bat1[i]=conf.get("bat1,"+i).substring(0,conf.get("bat1,"+i).indexOf(";"));
				bat2[i]=conf.get("bat2,"+i).substring(0,conf.get("bat2,"+i).indexOf(";"));
			}
			for(int i=0;i<20;i++)
			{
				bowl1[i]=conf.get("bowl1,"+i).substring(0,conf.get("bowl1,"+i).indexOf(";"));
				bowl2[i]=conf.get("bowl2,"+i).substring(0,conf.get("bowl2,"+i).indexOf(";"));
			}
			String val=value.toString().trim();
      		String first=val.substring(0,val.indexOf("\t")).trim();//extracting the fields fromt the value
			String second=val.substring(val.lastIndexOf("\t")+1,val.length());
			String batsman=first.substring(0,first.indexOf(",")).trim();
			String bowler=first.substring(first.indexOf(",")+1,first.length()).trim();
			for(int i=0;i<11;i++)
			{
				for(int j=0;j<20;j++)//passing the k-v pair to the reducer if input k-v pair matches the corresponding conf vars
				{
					if(batsman.equals(bat1[i])&&bowler.equals(bowl1[j]))
						context.write(new Text("1,"+Integer.toString(i)+","+Integer.toString(j)),new Text(second+"P"));
					if(batsman.equals(bat2[i])&&bowler.equals(bowl2[j]))
						context.write(new Text("2,"+Integer.toString(i)+","+Integer.toString(j)),new Text(second+"P"));
				}
			}
      }
    }
  public static class SimulationMapper2//MAPPER TO HANDLE CLUSTER TO CLUSTER PROBABILITIES
       extends Mapper<Object, Text, Text, Text>{
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			String temp;
			String bat1[]=new String[11];
  			String bowl1[]=new String[20];
  			String bat2[]=new String[11];
  			String bowl2[]=new String[20];
			int batclust1[]=new int[11];//stores the cluster nos. of batsman of innings 1
  			int bowlclust1[]=new int[20];//stores the cluster nos. of bowlers of innings 1
  			int batclust2[]=new int[11];//stores the cluster nos. of batsman of innings 2
  			int bowlclust2[]=new int[20];//stores the cluster nos. of bowlers of innings 2
			for(int i=0;i<11;i++)//populating the array based on the config vars
			{
				temp=conf.get("bat1,"+i);
				bat1[i]=temp.substring(0,temp.indexOf(";"));//as input is name ; cluster no we extract the fields and store it in the arrays
				batclust1[i]=Integer.parseInt(temp.substring(temp.indexOf(";")+1,temp.length()));
				temp=conf.get("bat2,"+i);
				bat2[i]=temp.substring(0,temp.indexOf(";"));
				batclust2[i]=Integer.parseInt(temp.substring(temp.indexOf(";")+1,temp.length()));
			}
			for(int i=0;i<20;i++)
			{
				temp=conf.get("bowl1,"+i);
				bowl1[i]=temp.substring(0,temp.indexOf(";"));
				bowlclust1[i]=Integer.parseInt(temp.substring(temp.indexOf(";")+1,temp.length()));
				temp=conf.get("bowl2,"+i);
				bowl2[i]=temp.substring(0,temp.indexOf(";"));
				bowlclust2[i]=Integer.parseInt(temp.substring(temp.indexOf(";")+1,temp.length()));
			}
			String splits[]=value.toString().trim().split("\t");
			int batclust=Integer.parseInt(splits[0]);//stores the input cluster no based on the input k-v pair
			int bowlclust=Integer.parseInt(splits[1]);
			for(int i=0;i<11;i++)
			{
				for(int j=0;j<20;j++)//passing the k-v pair to the reducer if input k-v pair matches the corresponding conf vars
				{
					if(batclust1[i]==batclust&&bowlclust1[i]==bowlclust)
						context.write(new Text("1,"+	Integer.toString(i)+","+Integer.toString(j)),new Text(splits[2]+","+splits[3]+","+splits[4]+","+splits[5]+","+splits[6]+","+splits[7]+","+splits[8]));
					if(batclust2[i]==batclust&&bowlclust2[i]==bowlclust)
						context.write(new Text("2,"+	Integer.toString(i)+","+Integer.toString(j)),new Text(splits[2]+","+splits[3]+","+splits[4]+","+splits[5]+","+splits[6]+","+splits[7]+","+splits[8]));
				}
			}
      }
    }
  public static class SimulationReducer//POPULATING THE PROBABILITIES FOR EVERY PLAYER TO PLAYER COMBINATION
       extends Reducer<Text,Text,Text,Text> {
    double prob1[][][]=new double[11][20][7];//stores the probabilities of all players combinations of innings 1
	double prob2[][][]=new double[11][20][7];//stores the probabilities of all players combinations of innings 2
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
		String probstring[]=new String[7];
		int innings,batsman,bowler;
		String keysplit[]=key.toString().split(",");
		for(Text val:values)
		{
			String tempstr=val.toString();
			if(tempstr.charAt(tempstr.length()-1)=='P')//if we have p-p probabilities then it'll select that only
			{
				probstring=tempstr.substring(0,tempstr.length()-1).trim().split(",");
				break;
			}
			else
			{
				probstring=tempstr.trim().split(",");//if no p-p prob then select c-c prob
			}
		}
		// probstring format : innings, batsman , bowler , prob(p-p/c-c)
		innings=Integer.parseInt(keysplit[0]);
		batsman=Integer.parseInt(keysplit[1]);
		bowler=Integer.parseInt(keysplit[2]);
		for(int i=0;i<7;i++)//populating the arrays based on input prob
		{
			if(innings==1)
				prob1[batsman][bowler][i]=Double.parseDouble(probstring[i]);
			else
				prob2[batsman][bowler][i]=Double.parseDouble(probstring[i]);
		}
    }
	@Override//SIMULATING THE MATCH
	protected void cleanup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		String bat1[]=new String[11];
		String bowl1[]=new String[20];
		String bat2[]=new String[11];
		String bowl2[]=new String[20];
		double workprob1[][]=new double[11][20];
		double workprob2[][]=new double[11][20];
		for(int i=0;i<11;i++)//populating the arrays 
		{
			bat1[i]=conf.get("bat1,"+i).substring(0,conf.get("bat1,"+i).indexOf(";"));
			bat2[i]=conf.get("bat2,"+i).substring(0,conf.get("bat2,"+i).indexOf(";"));
		}
		for(int i=0;i<20;i++)
		{
			bowl1[i]=conf.get("bowl1,"+i).substring(0,conf.get("bowl1,"+i).indexOf(";"));
			bowl2[i]=conf.get("bowl2,"+i).substring(0,conf.get("bowl2,"+i).indexOf(";"));;
		}
		for(int i=0;i<11;i++)//storing the prob of a batsman not getting out by the bowler in a temp array to take care of the fall of the wickets
		{
			for(int j=0;j<20;j++)
			{
				workprob1[i][j]=prob1[i][j][6];
				workprob2[i][j]=prob2[i][j][6];
			}
		}
		int batsman1,bowler,batsman2,score1,wickets1,balls1,score2,wickets2,balls2;
		batsman1=0;
		batsman2=1;
		bowler=0;
		score1=0;
		wickets1=0;
		balls1=0;
		score2=0;
		wickets2=0;
		balls2=0;
		context.write(new Text("\t\t\t\t\t    IPL MATCH SIMULATION"),new Text(""));
		context.write(new Text("--------------------------------------------------------------------------------------------------------------------------"),new Text(""));
		context.write(new Text("\t\t\t\t\tBIG DATA (UE14CS314) PROJECT\t"),new Text(""));
		context.write(new Text("--------------------------------------------------------------------------------------------------------------------------"),new Text("\n"));
		context.write(new Text("\t\t\t\tPRATIK CHATTERJEE"),new Text("01FB14ECS161"));		
		context.write(new Text("\t\t\t\tPOOJA BALUSANI\t"),new Text("01FB14ECS145"));
		context.write(new Text("\t\t\t\tPRIYANSHA PATHAK"),new Text("01FB14ECS168"));
		context.write(new Text("\t\t\t\tHEMKESH V KUMAR\t"),new Text("01FB14ECS083\n"));
		context.write(new Text("--------------------------------------------------------------------------------------------------------------------------"),new Text("\n"));
		//FIRST INNINGS
		outer1: for(int i=0;i<20;i++)//simulating the 1st innings
		{
			for(int j=0;j<6;j++)
			{
				int repindex=i;
				for(int k=0;k<=i;k++)//find the 1st over of the bowler who is currently bowling
				{
					if(bowl1[i].equals(bowl1[k]))
					{
						repindex=k;//stores the over no. of the current bowlers 1st over
						break;
					}
				}
				
				workprob1[batsman1][repindex]=workprob1[batsman1][repindex]*prob1[batsman1][bowler][6];//decreasing the prob for a batsman-bowler combination
				if(workprob1[batsman1][repindex]<0.5)//if prob < 0.5 : wicket
				{
					wickets1++;
					if(wickets1==10)//checking if all the wickets have fallen
					{
						context.write(new Text(Integer.toString(balls1/6)+"."+Integer.toString(balls1%6)+"\tBatsman: "+bat1[batsman1]+"\t\tBowler: "+bowl1[i]),new Text("\tBall Outcome:\tW"));//print output
						context.write(new Text("Score:"),new Text(Integer.toString(score1)+"/"+Integer.toString(wickets1)));
						balls1++;
						break outer1;//terminate innings	
					}
					else//incase a wicket falls and total wickets <10 
					{
						context.write(new Text(Integer.toString(balls1/6)+"."+Integer.toString(balls1%6)+"\tBatsman: "+bat1[batsman1]+"\t\tBowler: "+bowl1[i]),new Text("\tBall Outcome:\tW"));
						context.write(new Text("Score:"),new Text(Integer.toString(score1)+"/"+Integer.toString(wickets1)));
						batsman1=wickets1+1;//changing the batsman
						balls1++;
						continue;
					}
				}
				double rand=Math.random();//generating a random number
				double s=0;
				int runs[]={0,1,2,3,4,6};
				for(int k=0;k<6;k++)
				{
					double r1=s;
					double r2=s+prob1[batsman1][i][k];
					if(k==5)
						r2=1;
					if(rand>=r1&&rand<r2)//if the random no. generated is in the range btwn r1 and r2
					{
						score1+=runs[k];
						context.write(new Text(Integer.toString(balls1/6)+"."+Integer.toString(balls1%6)+"\tBatsman: "+bat1[batsman1]+"\t\tBowler: "+bowl1[i]),new Text("\tBall Outcome:\t"+Integer.toString(runs[k])));
						if(runs[k]%2!=0)//if the runs scored is odd we change the striker
						{
							int temp=batsman1;
							batsman1=batsman2;
							batsman2=temp;
						}
						break;
					}
					s=r2;
				}
				balls1++;
				context.write(new Text("Score:"),new Text(Integer.toString(score1)+"/"+Integer.toString(wickets1)));
			}
			bowler++;
			int temp=batsman1;//changing the strike at the end of the over 
			batsman1=batsman2;
			batsman2=temp;
		}
		context.write(new Text("\n--------------------------------------------------------------------------------------------------------------------------"),new Text(""));
		context.write(new Text("\t\t\t\tFIRST INNINGS SCORE: "+Integer.toString(score1)+"/"+Integer.toString(wickets1)+" in "+Integer.toString(balls1/6)+"."+Integer.toString(balls1%6)+" overs"),new Text(""));
		context.write(new Text("--------------------------------------------------------------------------------------------------------------------------"),new Text("\n"));
		batsman1=0;
		batsman2=1;
		bowler=0;
		//SECOND INNINGS
		outer2: for(int i=0;i<20;i++)
		{
			for(int j=0;j<6;j++)
			{
				int repindex=i;
				for(int k=0;k<=i;k++)
				{
					if(bowl2[i].equals(bowl2[k]))
					{
						repindex=k;
						break;
					}
				}
				workprob2[batsman1][repindex]*=prob2[batsman1][bowler][6];
				if(workprob2[batsman1][repindex]<0.5)
				{
					wickets2++;
					if(wickets2==10)
					{
						context.write(new Text(Integer.toString(balls2/6)+"."+Integer.toString(balls2%6)+"\tBatsman: "+bat2[batsman1]+"\t\tBowler: "+bowl2[i]),new Text("\tBall Outcome:\tW"));
						context.write(new Text("Score:"),new Text(Integer.toString(score2)+"/"+Integer.toString(wickets2)));
						balls2++;
						break outer2;	
					}
					else
					{
						context.write(new Text(Integer.toString(balls2/6)+"."+Integer.toString(balls2%6)+"\tBatsman: "+bat2[batsman1]+"\t\tBowler: "+bowl2[i]),new Text("\tBall Outcome:\tW"));
						context.write(new Text("Score:"),new Text(Integer.toString(score2)+"/"+Integer.toString(wickets2)));
						batsman1=wickets2+1;
						balls2++;
						continue;
					}
				}
				double rand=Math.random();
				double s=0;
				int runs[]={0,1,2,3,4,6};
				for(int k=0;k<6;k++)
				{
					double r1=s;
					double r2=s+prob2[batsman1][i][k];
					if(k==5)
						r2=1;
					if(rand>=r1&&rand<r2)
					{
						score2+=runs[k];
						context.write(new Text(Integer.toString(balls2/6)+"."+Integer.toString(balls2%6)+"\tBatsman: "+bat2[batsman1]+"\t\tBowler: "+bowl2[i]),new Text("\tBall Outcome:\t"+Integer.toString(runs[k])));
						if(runs[k]%2!=0)
						{
							int temp=batsman1;
							batsman1=batsman2;
							batsman2=temp;
						}
						break;
					}
					s=r2;
				}
				balls2++;
				context.write(new Text("Score:"),new Text(Integer.toString(score2)+"/"+Integer.toString(wickets2)));
				if(score2>score1)
					break outer2;
			}
			bowler++;
			int temp=batsman1;
			batsman1=batsman2;
			batsman2=temp;
		}
		context.write(new Text("\n--------------------------------------------------------------------------------------------------------------------------"),new Text(""));
		context.write(new Text("\t\t\t\t\tSECOND INNINGS SCORE: "+Integer.toString(score2)+"/"+Integer.toString(wickets2)+" in "+Integer.toString(balls2/6)+"."+Integer.toString(balls2%6)+" overs"),new Text(""));
		context.write(new Text("--------------------------------------------------------------------------------------------------------------------------"),new Text("\n"));
		if(score1>score2)
			context.write(new Text("MATCH RESULT: "),new Text("TEAM 1 WON BY "+(score1-score2)+" RUNS\n"));
		else if((score2>score1)&&(wickets2!=9))
			context.write(new Text("MATCH RESULT: "),new Text("TEAM 2 WON BY "+(10-wickets2)+" WICKETS\n"));
		else if((score2>score1)&&(wickets2==9))
			context.write(new Text("MATCH RESULT: "),new Text("TEAM 2 WON BY "+(10-wickets2)+" WICKET\n"));
		else
			context.write(new Text("MATCH RESULT: "),new Text("TIE\n"));
	}
  }

  public static void main(String[] args) throws Exception {
	
			Configuration conf = new Configuration();
			String bat1[]=new String[11];//names of batsmen of 1st innings
  			String bowl1[]=new String[20];//names of bowler of 1st innings
  			String bat2[]=new String[11];//names of batsmen of 2nd innings
  			String bowl2[]=new String[20];//names of bowler of 2nd innings
			FileReader fr = new FileReader("/Users/hadoop/Public/clusterno.txt");//cluster nos. of each player
			BufferedReader br = new BufferedReader(fr);
			for(int i=0;i<62;i++)
			{
				String line[]=br.readLine().trim().split("\t");//extracting fields from the data
				int innings=Integer.parseInt(line[0]);
				int role=Integer.parseInt(line[1]);
				int pos=Integer.parseInt(line[2]);
				String name=line[3];
				String clust=line[4].trim();
				if(innings==1)//populating the arrays
				{
					if(role==1)//role = 1 : batsman ; role = 2 : bowler
					{
						conf.set("bat1,"+pos,name+";"+clust);//setting config vars - used to communicate across mappers and reducers
						bat1[pos]=name;
					}
					else if(role==2)
					{
						conf.set("bowl1,"+pos,name+";"+clust);
						bowl1[pos]=name;
					}
				}
				else if(innings==2)
				{
					if(role==1)
					{
						conf.set("bat2,"+pos,name+";"+clust);
						bat2[pos]=name;
					}
					else if(role==2)
					{
						conf.set("bowl2,"+pos,name+";"+clust);
						bowl2[pos]=name;
					}
				}
			}
			
			Job job = Job.getInstance(conf, "Match Simulation");
			job.setJarByClass(IPLSimulation.class);
			//job.setMapperClass(SimulationMapper.class);
			job.setReducerClass(SimulationReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			MultipleInputs.addInputPath(job, new Path("/user/hadoop/input/TempStep1"),TextInputFormat.class,SimulationMapper1.class);//player to player probabilities
			MultipleInputs.addInputPath(job, new Path("/user/hadoop/input/TempStep3"),TextInputFormat.class,SimulationMapper2.class);//cluster to cluster probabilities
			FileOutputFormat.setOutputPath(job, new Path("IPLSimulationOutput"));
			job.waitForCompletion(true);
		
  }
}
