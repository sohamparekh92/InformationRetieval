import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

//--------------------------Positional Index----------------------------------------------------------------

public  class PosIdx{ 

		public static class MapperA extends MapReduceBase implements Mapper<LongWritable, Text, Text,Text> {
		//private final static Text temp =new Text();
		private static String fix;
		private final static Text var1 = new Text();
		private final static Text var2 =new Text();
		private static int cvar=0;
		private static int pt=0;
		
		public void map(LongWritable key, Text val, OutputCollector<Text,Text> output, Reporter rep)
			throws IOException
		{
			FileSplit splitVar = (FileSplit)rep.getInputSplit();
			String strfile = splitVar.getPath().getName();
			String newline = val.toString();
			StringTokenizer tokenx = new StringTokenizer(newline.toLowerCase());			
			if(cvar==0)
           	fix=strfile;
            else if(strfile.equals(fix)){}
            else
            {       fix=strfile;
                	pt=0;  	
                }
            	while (tokenx.hasMoreTokens()) {
				String cx=tokenx.nextToken().toString();
				cx =cx.replaceAll("([^\\w+])","").replaceAll("([\\d+.*])","");;
				//cc
				pt+=1;
				if( !cx.isEmpty() )
			 {
				var2.set("["+strfile+","+pt+"]");
				var1.set(cx);
				System.out.println("for key"+var1+"posting is"+var2);
				output.collect(var1, var2);
				cvar+=1;
			}
			
		}
	}
	}

	public static class ReducerB extends MapReduceBase implements Reducer<Text,Text, Text, Text> {
		
		private	static int[] doccount=new int[20];

		private	static int px=0;
		private	static int count1=0;
	
		
		
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter z)
						throws IOException {
			//create arraylist
			ArrayList<Integer> arr = new ArrayList<>();
			//create hashmap_var
			HashMap<Integer,String> postings = new HashMap<>();
			StringBuilder rettool = new StringBuilder();
			StringBuilder strb2 = new StringBuilder();
				 int count=0, nc=0;
				 while (values.hasNext()) 
		            {
		            	String post=values.next().toString();
		            	String[] partmain=post.split(",",2);
		            	String partx1=partmain[0];
		            	String partx3=partmain[1];
		            	String[] parts4=partx1.split("-",2);
		            	String[] parts5=partx3.split("-",2);
		              int fil=Integer.parseInt(parts4[1]);
	               int postn=Integer.parseInt(parts5[0]);
		                if(count1==0)
		                {
		                px=fil;
		                nc+=1;
		                doccount[px]=nc;
		                }
		                else
		                if(fil==px)
		                    {
		                    	nc+=1;
		                    	doccount[px]=nc;
		                    }
		                    else
		                    	{  
			                    	nc=1;
			                    	px=fil;
			                    	doccount[px]=nc;
			                    	//poss="";
			                    	arr.clear();}
		                   
		               	Collections.sort(arr);
		            	System.out.println("for key "+key+" fil :"+fil+"postn :"+postn);
		            	postings.put(fil,arr.toString());	
		  
		            	 count+=1;
		            	 count1+=1;
		            }
		            
		            
		            System.out.println("the count : "+count+"count1 equals "+count1);
		            System.out.println("posting has :"+postings);
		            System.out.println("document count[1]="+doccount[1]+"document count[2]="+doccount[2]);
		            Map<Integer,String> newMap = new TreeMap<Integer,String>(postings);

		           for(int y : postings.keySet())
		           rettool.append("["+y+","+doccount[y]+":"+postings.get(y)+"]");
			   
		           IntWritable freq = new IntWritable(count);
		           strb2.append(key.toString());
            strb2.append("|").append(freq);
            output.collect(new Text(strb2.toString()),new Text(rettool.toString()));
		}
	}



	public static void run(String in, String out)
		{
				JobClient jc = new JobClient();
				JobConf gtx = new JobConf(Pos.class);//class name acc
		
				gtx.setJobName("PositionalIndex");
				//gtx.setOutputKeyClass(Text.class);
				//gtx.setOutputValueClass(Text.class);
				gtx.setMapOutputKeyClass(Text.class);
				gtx.setMapOutputValueClass(Text.class);
				gtx.setOutputKeyClass(Text.class);
				gtx.setOutputValueClass(Text.class);
				
				FileInputFormat.addInputPath(gtx, new Path(in));
				FileOutputFormat.setOutputPath(gtx, new Path(out));
				gtx.setMapperClass(MapperA.class);
				gtx.setReducerClass(ReducerB.class);
		
				jc.setConf(gtx);
				//JobClient.runJob(gtx);
		try {
			JobClient.runJob(gtx);
		} 
		catch (Exception e) {
			e.printStackTrace();
		}

	}
	
	
	public static void main(String args[]) 
	{
		if( args.length!=2 )System.err.println("Positional-Indexer <input_dir> <output_dir>");
		else	run(args[0], args[1]);
	}

}
