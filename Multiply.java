
import java.io.*;
import java.util.*;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.util.*;
import java.io.IOException;
import java.util.Scanner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.conf.*;




class Element implements Writable
 {
    public short tag;  // 0 for M, 1 for N
    public int index;  // one of the indexes (the other is used as a key)
    public double value;
    Element () {}

    Element ( short t, int in, double val ) {
        tag=t; index = in; value = val;
    }

    public void write ( DataOutput out ) throws IOException {
        out.writeShort(tag);
        out.writeInt(index);
        out.writeDouble(value);
    }

    public void readFields ( DataInput in ) throws IOException {
        tag = in.readShort();
        index = in.readInt();
        value = in.readDouble();
    }
	public int getIndex(){ return this.index;}
	public double getValue(){ return this.value;}
	
}
class Pair implements WritableComparable<Pair>
{
	int i;
	int j;
  
	Pair () {}
  
	Pair (int indexi , int indexj)
	{
		i=indexi;
		j=indexj;
	  
	}
	public void write ( DataOutput out ) throws IOException {
        out.writeInt(i);
        out.writeInt(j);
        
    }

    public void readFields ( DataInput in ) throws IOException {
        i = in.readInt();
        j = in.readInt();
        
    }
	public int compareTo(Pair p)
	{	
		int n = this.i - p.i;
		
		if(n==0)
			return (this.j - p.j);
		return n;
	}
}


public class Multiply
{
	public static class MatrixMMapper extends Mapper<Object,Text,IntWritable,Element > {
		
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
			int j=0;
			short t =0;
			System.out.println("MpperM");
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
			Element e = new Element();
			e.tag=t;
			e.index=s.nextInt();	//i
			j= s.nextInt();			//j
			e.value=s.nextDouble();
			System.out.println(j+","+e.tag+","+e.index+","+e.value);
            context.write(new IntWritable(j),e);	//output of first map reduce job : {j, (M, i, mij))
            s.close();
        }
    }

	public static class MatrixNMapper extends Mapper<Object,Text,IntWritable,Element > {
		
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
			System.out.println("MApperN");
			int j=0;
			short t =1;
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
			Element e = new Element();
			e.tag=t;
			j= s.nextInt();			//j
			e.index=s.nextInt();	//k
			e.value=s.nextDouble(); //value
			System.out.println(j+","+e.tag+","+e.index+","+e.value);
            context.write(new IntWritable(j),e);
			s.close();
        }
    }
	public static class reduce extends Reducer<IntWritable,Element,Text,Text>
	{
		static Vector<Element> A = new Vector<Element>();
        static Vector<Element> B = new Vector<Element>();
		int i=0,j=0;
		
        @Override
        public void reduce ( IntWritable key, Iterable<Element> values, Context context )
                           throws IOException, InterruptedException {
            A.clear();
			B.clear();
            System.out.println("Reducer key="+key);
            for (Element v: values)
			{
                if (v.tag == 0)
				{
					Element e = new Element(v.tag,v.index,v.value);
					A.add(e);
					System.out.println(e.tag+","+e.index+","+e.value);
				}
                    
                else 
				{	
					Element d = new Element(v.tag,v.index,v.value);
					B.add(d);
					System.out.println(d.tag+","+d.index+","+d.value);
				}
			}
			
			for ( Element a: A )
			{
                for ( Element b: B )
				{
					Text k = new Text(a.index+","+b.index);
					Pair p = new Pair();
					p.i=a.index;
					p.j=b.index;
					Text value= new Text(Double.toString(a.value*b.value));
                    context.write(k,value);
				}
			}
		}
	}//reduce
	
	
	public static class IdentityMapper extends Mapper<Text,Text,Text,Text > {
	    @Override
        public void map ( Text key, Text value, Context context )
                        throws IOException, InterruptedException
		{
            System.out.println("Phase 2 mapper");
			context.write(key,value);	//output of second map reduce job (j, (N, k, njk))
            
        }
    }
	
	public static class SumReducer extends Reducer<Text,Text,Text,Text> {
		@Override
        public void reduce ( Text key, Iterable<Text> values, Context context )
                           throws IOException, InterruptedException {
            double sum = 0.0;
			for (Text v: values)
			{
                sum +=  Double.valueOf(v.toString());
				System.out.println("sum:"+sum);	
            }
			Text t = new Text(String.valueOf(sum));
			context.write(key,t);
        }
    }
	
	
	public static void main ( String[] args ) throws Exception 
	{
		
		Configuration conf = new Configuration();
		
		//driver code contains configuration details
		Job job1 = Job.getInstance(conf,"Job1");
		job1.setJobName("Sparse Matrix mult-aggregation");
		job1.setJarByClass(Multiply.class);
		
		//mapper output format
		job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(Element.class);
		
		//reducer output format
		job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
		
		MultipleInputs.addInputPath(job1,new Path(args[0]),TextInputFormat.class,MatrixMMapper.class);
        MultipleInputs.addInputPath(job1,new Path(args[1]),TextInputFormat.class,MatrixNMapper.class);
       	job1.setReducerClass(reduce.class);
		
		 
		//intermediate output
	    FileOutputFormat.setOutputPath(job1,new Path(args[2])); // directory where the output will be written onto
        job1.waitForCompletion(true);
		
		//job2
		Job job2 = Job.getInstance(conf,"job 2");
		job2.setJobName("Sparse Matrix mult-summation");
		job2.setJarByClass(Multiply.class);
		
		job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
		
		job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
		
		job2.setMapperClass(IdentityMapper.class);
		job2.setReducerClass(SumReducer.class);
		
		job2.setInputFormatClass(KeyValueTextInputFormat.class);
			
		//directory from where it will fetch input file	
		FileInputFormat.setInputPaths(job2,new Path(args[2])); //directory from where it will fetch input file
        FileOutputFormat.setOutputPath(job2,new Path(args[3]));// directory where the output will be written onto
        
		job2.waitForCompletion(true);
	}//main
	

}//multiply