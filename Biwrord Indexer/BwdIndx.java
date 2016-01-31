import java.io.*;
import java.util.*;
//import java.lang.*;
//import java.
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

//-------------------BiwordIndex-------------------------------------------------------------------
public class BwdIndx
{ 
//Mapper Class
	public static class MapperClassA extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		private static Text theWord;
		
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter z)
						throws IOException {
			
			//Take in the files
			FileSplit Splitvar = (FileSplit)z.getInputSplit();
			IntWritable fNumber = new IntWritable(Integer.parseInt(Splitvar.getPath().getName()));
			//System.out.println(fnumber);

			String newline = value.toString();
			StringTokenizer tokenx = new StringTokenizer(newline.toLowerCase());
			//iterator
			while (tokenx.hasMoreTokens()) 
			{
			String tstring=tokenx.nextToken().toString();
			tstring =tstring.replaceAll("([^\\w+])","").replaceAll("([\\d+.*])","");
	
				
			if(!tstring.isEmpty())
			{
		    String a;
			String b;
			//Local var
			a=tstring;
			if(tokenx.hasMoreTokens())
			{
			String tx=tokenx.nextToken().toString();
			//tx =tx.replaceAll("([^\\w+])","");
			tx =tx.replaceAll("([^\\w+])","").replaceAll("([\\d+.*])","");
			if(!tx.isEmpty())
		   {
			b=tx;
			theWord=new Text(a+" "+b);
            output.collect(theWord, fNumber);
			}
			}
			}
		}
	}
	}

	public static class ReducerClassB extends MapReduceBase implements Reducer<Text, IntWritable, Text, ArrayList<IntWritable>> {
		
		
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, ArrayList<IntWritable>> output, Reporter gz)
			throws IOException {
			ArrayList<IntWritable> files = new ArrayList<IntWritable>();
			files.clear();
			System.out.println("Files clear job");
			int count=0;
			StringBuilder rettool = new StringBuilder();
			StringBuilder xkey = new StringBuilder();
			
			while (values.hasNext()){
				System.out.println(" File -  " + files );
		        IntWritable filename = new IntWritable(values.next().get());
		        if(!(files.contains(filename)))
		        { 
		        	
		        	System.out.println("Key "+key+" File " + filename );
		        	files.add(filename);
		            if (count!=0) rettool.append("-> ");
		            count++;
		            rettool.append(filename);
		        }
			}
			
			Collections.sort(files);
			IntWritable freq = new IntWritable(count);
            xkey.append(key.toString());
            xkey.append("|").append(freq);
			output.collect(new Text(xkey.toString()),files );
		}
		//catch(IOException e)
		//{}
	}

	public static void run(String in, String out)
	{
		JobClient jc = new JobClient();
		//System.out.println("TEST run");
		JobConf jobx = new JobConf(InvertedIndex.class);
		jobx.setJobName("InvertedIndex");
		jobx.setOutputKeyClass(Text.class);
		jobx.setOutputValueClass(ArrayList.class);
		jobx.setMapOutputKeyClass(Text.class);
		jobx.setMapOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(jobx, new Path(in));
		FileOutputFormat.setOutputPath(jobx, new Path(out));
		jobx.setMapperClass(MapperClassA.class);
		jobx.setReducerClass(ReducerClassB.class);
		jc.setConf(jobx);
		try 
		{
			JobClient.runJob(jobx);
		} 
		catch(IOException e)
		{
			e.printStackTrace();
		}
		
		catch (Exception e) 
		{
			e.printStackTrace();
		}
	}
	
	public static void main(String arg[]) 
	{
		if( arg.length!=2 )
		{
			System.err.println("Inverted-Index <input_dir> <output_dir>");
		}
		else
		{
			run(arg[0], arg[1]);
		}
	}
}