import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

//----------------Uniword Index------------------------------------------------------------------------

public class UniIdx{ 

	public static class MapperA extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		private final static Text var1 = new Text();
		//private final static Text var2 = new Text();
		//override map
		public void map(LongWritable key, Text val, OutputCollector<Text, IntWritable> output, Reporter z)
						throws IOException {
			
			FileSplit splitvar = (FileSplit)z.getInputSplit();
			IntWritable fe = new IntWritable(Integer.parseInt(splitvar.getPath().getName()));
			//new line from file
			String newline = val.toString();
			StringTokenizer loop = new StringTokenizer(newline.toLowerCase());
			//till iterator has tokens
			while (loop.hasMoreTokens()) {
			String str=loop.nextToken().toString();
			str =str.replaceAll("([^\\w+])","").replaceAll("([\\d+.*])","");
			//check if empty
			if(!str.isEmpty())
					{
						var1.set(new Text(str));
						output.collect(var1, fe);
					}
			}
		}
	}

	public static class ReducerB extends MapReduceBase implements Reducer<Text, IntWritable, Text, ArrayList<IntWritable>> {
		
		//override reduce
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, ArrayList<IntWritable>> output, Reporter reporter)
						throws IOException {
			ArrayList<IntWritable> files = new ArrayList<IntWritable>();
			files.clear();
			System.out.println("Files_clr");
			int count=0;
			StringBuilder frk = new StringBuilder();
			StringBuilder rettool = new StringBuilder();
			
			while (values.hasNext()){
			System.out.println("files: " + files );
		    IntWritable filename = new IntWritable(values.next().get());
		        if(!(files.contains(filename)))
		        { 
		        System.out.println(" Key "+key+" File " + filename );
		        //add name of fille to files
			    files.add(filename);
		              
		            if (count!=0)rettool.append("-> ");
		            count+=1;
		            rettool.append(filename); //attach fn
		        }
			}
			Collections.sort(files);
			IntWritable freq = new IntWritable(count);
            frk.append(key.toString());
            frk.append("|");
            frk.append(freq);
			output.collect(new Text(frk.toString()),files );
		}
	}

	public static void run(String in, String out){
		JobClient cl = new JobClient();
		JobConf cf = new JobConf(Inverted.class);//class name acc
		
		cf.setJobName("InvertedIndex"); //Output acc
		
		cf.setMapOutputKeyClass(Text.class);
		cf.setMapOutputValueClass(IntWritable.class);
		
		cf.setOutputKeyClass(Text.class);
		cf.setOutputValueClass(ArrayList.class);

		FileInputFormat.addInputPath(cf, new Path(in));
		FileOutputFormat.setOutputPath(cf, new Path(out));

		cl.setConf(cf);
		cf.setMapperClass(MapperA.class);
		cf.setReducerClass(ReducerB.class);


		try 
		{
		JobClient.runJob(cf);
		} 
		catch (Exception e) {
			//System.out.printl("Exception");
			e.printStackTrace();
		}

	}public static void main(String arg[])
	{
		if( arg.length!=2 )	System.err.println("Inverted-Index <input_dir> <output_dir>");
		else run(arg[0], arg[1]);
	}
}
