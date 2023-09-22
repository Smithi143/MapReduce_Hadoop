import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MutualFrnds {

	public static class MyMapper 
    extends Mapper<Object, Text, Text, Text>{
 
		
	 private Text friendsKey = new Text();
	 private Text allFrnds = new Text();
	   
	 public void map(Object key, Text value, Context context
	                 ) throws IOException, InterruptedException {
	   
		 String[] divide = value.toString().split("\\s+");
	   
		 if(divide.length == 2 ) {
			 int person = Integer.parseInt(divide[0]);
			 int[] friends = Arrays.stream(divide[1].trim().split(","))
		                .mapToInt(Integer::parseInt)
		                .toArray();
			 
			 for(int i = 0; i < friends.length; i++) {				 
				 String personPairKey = (person < friends[i]) ? person + " " + friends[i] + "-"
						 : friends[i] + " " + person + "-";
				 friendsKey.set(personPairKey);
				 allFrnds.set(divide[1]);
				 context.write(friendsKey, allFrnds);				 
			 }
		 }
	 }
	}
	
	public static class MyReducer 
    extends Reducer<Text,Text,Text,Text> {
	  
	 private Text resultKey = new Text();
	 private Text result = new Text();
	 public int max = 0;
     public HashSet<Integer> hs = new HashSet<>();
     public HashMap<String,Integer> hm = new HashMap<>();
     public int maxx = 0;
	
	 public void reduce(Text key, Iterable<Text> values, 
	                    Context context
	                    ) throws IOException, InterruptedException {
		 
		 ArrayList<Integer> allMutualFriends = new ArrayList<Integer>(); 

		 StringBuilder res = new StringBuilder();
      
	      for(Text val : values) {   	      	  
	    	  String[] individualValues = val.toString().split(",");
	    	  allMutualFriends.addAll(Arrays.stream(individualValues)
	    	            .map(Integer::parseInt)
	    	            .collect(Collectors.toList()));
	      }
	      	      
		 Collections.sort(allMutualFriends);
		 int temp = 0;
		 int i = 0;
		 StringBuilder maxKey = new StringBuilder();
		 while (i < allMutualFriends.size() - 1) {
			 if(allMutualFriends.get(i).equals(allMutualFriends.get(i+1))) {
				 temp = temp + 1;
				 res.append(allMutualFriends.get(i)).append(",");
				 i = i + 2;
			 }
			 else 
				 i = i + 1;
		 }
		 if (res.length() > 0) {
			   res.setLength(res.length() - 1);
			}
		 
		 hs.add(temp);
		 hm.put(key.toString(),temp);
		 	 
	      Iterator<Integer> ite = hs.iterator();
	      while (ite.hasNext()) {
	          int current = ite.next();
	          if (current > maxx) {
	              maxx = current;
	          }
	      }
		   result.set(res.toString());
		   context.write(key, result);
	 }
	 
	 @Override
	    protected void cleanup(Context context) throws IOException, InterruptedException {
		 
		 context.write(new Text("Keys with Highest Mutual Friends Count of "), new Text(String.valueOf(maxx)));
	        for (Map.Entry<String, Integer> entry : hm.entrySet()) {
	            if (entry.getValue() == maxx) {       	
	            	context.write(new Text(entry.getKey()), new Text(String.valueOf(maxx)));
	            }	
	        }
	        
	        String text = "Keys that start with either 1 or 5";
	        
	        for (Map.Entry<String, Integer> entry : hm.entrySet()) {
	          String [] divideKey = new String[2];
	  	      divideKey = entry.getKey().toString().split(" ");
	  	      if((!divideKey[0].trim().isEmpty()) || !divideKey[1].trim().isEmpty()) {
	  	    	if ((divideKey[0].charAt(0) == '1' &&  divideKey[1].charAt(0) == '1') || 
	  	            	(divideKey[0].charAt(0) == '5' &&  divideKey[1].charAt(0) == '5') ||
	  	            	(divideKey[0].charAt(0) == '1' &&  divideKey[1].charAt(0) == '5') ||
	  	            	(divideKey[0].charAt(0) == '5' &&  divideKey[1].charAt(0) == '1')	 ) {
	  	    		StringBuilder cleanResult = new StringBuilder(entry.getKey().substring(0, entry.getKey().length() - 1));
	  	            	context.write(new Text("Keys start with 1 or 5"), new Text(cleanResult.toString()));
	  	    	}
	  	      }
            }
            
	        
	    }
	 
	}
    
	
	  public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    
		    Job job = new Job(conf, "mutual friends");
		    job.setJarByClass(MutualFrnds.class);
		    job.setMapperClass(MyMapper.class);
		    job.setReducerClass(MyReducer.class);
		    //job.setNumReduceTasks(1);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
}
