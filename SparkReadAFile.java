package spkg;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import com.google.common.base.Optional;

import scala.Tuple2;

public class SparkReadAFile {
	static SparkConf conf = null;
	static JavaSparkContext sc = null;

	public static void init()
	{
		 conf = new SparkConf().setAppName("Read A File").setMaster("local");
		 sc = new JavaSparkContext(conf);

	}
	
	public static void main(String[] args) 
	{
		Date bdate = new Date();
		System.out.println("Begin date " + new Date());
		init();
		String afile = "D:/compare/aa.txt";
		String bfile = "D:/compare/bb.txt";
		JavaPairRDD<String, MyData> agrp = getgroupRDD2(afile);
		JavaPairRDD<String, MyData> bgrp = getgroupRDD2(bfile);
		
		System.out.println("Before Outer join " + new Date());
		JavaPairRDD<String, Tuple2<Optional<MyData>, Optional<MyData>>> joinedResult = agrp.fullOuterJoin(bgrp);
		System.out.println("Before filter " + new Date());

		JavaPairRDD<String, Tuple2<Optional<MyData>, Optional<MyData>>> filteredResult = joinedResult.filter(new Function<Tuple2<String,Tuple2<Optional<MyData>,Optional<MyData>>>,Boolean>()
		{
					@Override
					public Boolean call(Tuple2<String, Tuple2<Optional<MyData>, Optional<MyData>>> arg0)
							throws Exception 
					{
						String custid_ofrcode = arg0._1;
						Tuple2<Optional<MyData>, Optional<MyData>> valtuple = arg0._2; 
						Optional<MyData> lval = valtuple._1;
						Optional<MyData> rval = valtuple._2;
						//System.out.println("lval is " + lval + ",rval is " + rval);
						if ( !lval.isPresent() || !rval.isPresent() || lval.get().isMyDataSame(rval.get()) == false )
						{
							return true;
						}
						// TODO Auto-generated method stub
						return false;
					}
		}
		);

		System.out.println("Before mapTopair " + new Date());

		JavaPairRDD<String, MyData> deltaResult = filteredResult.mapToPair(new PairFunction <Tuple2<String, Tuple2<Optional<MyData>, Optional<MyData>>>,  String, MyData>() 
		{
			@Override
			public Tuple2<String, MyData> call(Tuple2<String, Tuple2<Optional<MyData>, Optional<MyData>>> arg0)
					throws Exception 
			{
				// TODO Auto-generated method stub
				String custid_ofrcode = arg0._1;
				Tuple2<Optional<MyData>, Optional<MyData>> valtuple = arg0._2; 
				Optional<MyData> lval = valtuple._1;
				Optional<MyData> rval = valtuple._2;
				System.out.println("lval is " + lval + ",rval is " + rval);
				if ( !lval.isPresent() )
				{
					//System.out.println("Left is null " + custid_ofrcode);
					return new Tuple2<String, MyData>(custid_ofrcode+"|ADD",rval.get());
				}
				else if (!rval.isPresent() )
				{
					//System.out.println("Right is null " + custid_ofrcode);
					return new Tuple2<String, MyData>(custid_ofrcode+"|DEL",lval.get());
				}
				else 
				{
					return new Tuple2<String, MyData>(custid_ofrcode+"|MOD",rval.get());
				}
				// TODO Auto-generated method stub
			}
			
		});
		System.out.println("Before collect " + new Date());

		try
		{
			File file = new File("D:/compare/deltaout.txt");
			 
			// if file doesnt exists, then create it
			if (!file.exists()) {
				file.createNewFile();
			}
	
			FileWriter fw = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);
			
			List<Tuple2<String, MyData>> oresult = deltaResult.collect();
			System.out.println("Size of result set is " + oresult.size());
			for ( Tuple2<String, MyData> t: oresult )
			{
				String custid_ofrcode = t._1;
				bw.write(custid_ofrcode+"-"+t.toString());
				bw.newLine();
				//System.out.println("Filtered Tuple : "+t.toString());
			}
			bw.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		
		/*
		JavaPairRDD<String, Tuple2<Optional<MyData>, Optional<MyData>>> outgrp = fgrp.filter(new Function<Tuple2<String,Tuple2<Optional<MyData>,Optional<MyData>>>,Boolean>()
		{
					@Override
					public Boolean call(Tuple2<String, Tuple2<Optional<MyData>, Optional<MyData>>> arg0)
							throws Exception 
					{
						String custid_ofrcode = arg0._1;
						Tuple2<Optional<MyData>, Optional<MyData>> valtuple = arg0._2; 
						Optional<MyData> lval = valtuple._1;
						Optional<MyData> rval = valtuple._2;
						System.out.println("lval is " + lval + ",rval is " + rval);
						if ( !lval.isPresent() )
						{
							System.out.println("Left is null " + custid_ofrcode);
							return true;
						}
						else if (!rval.isPresent() )
						{
							System.out.println("Right is null " + custid_ofrcode);
							return true;
						}
						// TODO Auto-generated method stub
						return false;
					}
		}
		);
		/*
		List<Tuple2<String, Tuple2<Optional<MyData>, Optional<MyData>>>> fresult = fgrp.collect();
		for ( Tuple2<String, Tuple2<Optional<MyData>, Optional<MyData>>> t: fresult )
		{
			System.out.println("Full outer Joined Tuple : "+t);
		}

		try
		{
			File file = new File("D:/compare/deltaout_small.txt");
			 
			// if file doesnt exists, then create it
			if (!file.exists()) {
				file.createNewFile();
			}
	
			FileWriter fw = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);
	
			List<Tuple2<String, Tuple2<Optional<MyData>, Optional<MyData>>>> oresult = outgrp.collect();
			for ( Tuple2<String, Tuple2<Optional<MyData>, Optional<MyData>>> t: oresult )
			{
				t.toString();
				String custid_ofrcode = t._1;
				bw.write(custid_ofrcode+"-"+t.toString());
				bw.newLine();
				System.out.println("Filtered Tuple : "+t.toString());
			}
			bw.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		*/
		System.out.println("Program begin date " + bdate + ", End date " + new Date());

	}
	
	public static JavaPairRDD<String, MyData> getgroupRDD2(String file)
	{
		
		JavaRDD<String> afile = sc.textFile(file);
		JavaPairRDD<String, MyData> pairtokens = afile.mapToPair(new PairFunction <String, String,  MyData>() 
		{
			@Override
			public Tuple2<String, MyData> call(String s)	throws Exception 
			{
				// TODO Auto-generated method stub
				ArrayList<String> valList = new ArrayList<String>(Arrays.asList(s.split("\\|")));
				MyData ofr = new MyData();
				ofr.setCustId(valList.get(0));
				ofr.setName(valList.get(1));
				ofr.setOfrcd(valList.get(2));
				ofr.setEndDate(valList.get(3));
				ofr.setBonusPoints(valList.get(4));

				return new Tuple2<String, MyData>(valList.get(0)+"|"+valList.get(2), ofr);
				
			}
	    });
		
		return pairtokens;

	}

	/*

	public static void main1(String[] args) 
	{
		String afile = "D:/compare/a.txt";
		String bfile = "D:/compare/b.txt";
		JavaPairRDD<String, Iterable<List<String>>> agrp = getgroupRDD(afile);
		JavaPairRDD<String, Iterable<List<String>>> bgrp = getgroupRDD(bfile);
		JavaPairRDD<String, Tuple2<Iterable<List<String>>,Iterable<List<String>>>> cgrp = agrp.join(bgrp);
		List<Tuple2<String, Tuple2<Iterable<List<String>>,Iterable<List<String>>>>> cresult = cgrp.collect();
		for ( Tuple2<String, Tuple2<Iterable<List<String>>,Iterable<List<String>>>> t: cresult )
		{
			System.out.println("Joined Tuple : "+t);
		}
		JavaPairRDD<String, Tuple2<Optional<Iterable<List<String>>>, Iterable<List<String>>>> dgrp = agrp.rightOuterJoin(bgrp);
		List<Tuple2<String, Tuple2<Optional<Iterable<List<String>>>, Iterable<List<String>>>>> dresult = dgrp.collect();
		for ( Tuple2<String, Tuple2<Optional<Iterable<List<String>>>, Iterable<List<String>>>> t: dresult )
		{
			System.out.println("Right outer Joined Tuple : "+t);
		}
		
		
		JavaPairRDD<String, Tuple2<Iterable<List<String>>, Optional<Iterable<List<String>>>>> egrp = agrp.leftOuterJoin(bgrp);
		List<Tuple2<String, Tuple2<Iterable<List<String>>, Optional<Iterable<List<String>>>>>> eresult = egrp.collect();
		for ( Tuple2<String, Tuple2<Iterable<List<String>>, Optional<Iterable<List<String>>>>> t: eresult )
		{
			System.out.println("Left outer Joined Tuple : "+t);
		}
		
		JavaPairRDD<String, Tuple2<Optional<Iterable<List<String>>>, Optional<Iterable<List<String>>>>> fgrp = agrp.fullOuterJoin(bgrp);
		List<Tuple2<String, Tuple2<Optional<Iterable<List<String>>>, Optional<Iterable<List<String>>>>>> fresult = fgrp.collect();
		for ( Tuple2<String, Tuple2<Optional<Iterable<List<String>>>, Optional<Iterable<List<String>>>>> t: fresult )
		{
			System.out.println("Full outer Joined Tuple : "+t);
		}
	
	}

	
	public static JavaPairRDD<String, Iterable<List<String>>> getgroupRDD(String file)
	{
		JavaRDD<String> afile = sc.textFile(file);
		JavaPairRDD<String, List<String>> pairtokens = afile.mapToPair(new PairFunction <String, String,  List<String>>() 
		{
			@Override
			public Tuple2<String, List<String>> call(String s)	throws Exception 
			{
				// TODO Auto-generated method stub
				ArrayList<String> valList = new ArrayList<String>(Arrays.asList(s.split("\\|")));
				return new Tuple2<String, List<String>>(valList.get(0)+"|"+valList.get(2), valList);
			}
	    });
		
		List<Tuple2<String, List<String>>> arrlist = pairtokens.collect();
		System.out.println("After collect");
		for ( Tuple2<String, List<String>> t: arrlist )
		{
			System.out.println("Tuple : "+t);
		}
	
		JavaPairRDD<String, Iterable<List<String>>> myfinalmp = pairtokens.groupByKey();
		
		List<Tuple2<String, Iterable<List<String>>>> brrlist = myfinalmp.collect();
		for ( Tuple2<String, Iterable<List<String>>> t: brrlist )
		{
			System.out.println("Second Tuple : "+t);
		}
		
		return myfinalmp;

	}
	public static void main2(String[] args) 
	{
		SparkConf conf = new SparkConf().setAppName("Read A File").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> afile = sc.textFile("D:/compare/a.txt");
		JavaRDD<String> bfile = sc.textFile("D:/compare/b.txt");
	
		System.out.println("Before map");
	
		JavaRDD<List<String>> alinetokens = afile.map(new Function <String, List<String>>() 
		{
	        @Override
	        public List<String> call(String s) {
	        	System.out.println("Call function");
	            return new ArrayList<String>(Arrays.asList(s.split("\\|")));
	        }
	    });
		
		JavaRDD<List<String>> blinetokens = bfile.map(new Function <String, List<String>>() 
		{
	        @Override
	        public List<String> call(String s) {
	        	System.out.println("Call function");
	            return new ArrayList<String>(Arrays.asList(s.split("\\|")));
	        }
	    });
	
		System.out.println("Before collect");
	
		List<List<String>> arrlist = alinetokens.collect();
		System.out.println("After collect");
	
		for ( List<String> line: arrlist )
		System.out.println("Line: "+line);		
	}
	*/
	
	public static void main3(String[] args) 
	{
		try {
			 
			String content = "This is the content to write into file";
 
			File file = new File("D:/compare/aaa.txt");
 
			// if file doesnt exists, then create it
			if (!file.exists()) {
				file.createNewFile();
			}
 
			FileWriter fw = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);
			
			bw.write(String.valueOf(0));bw.write("|");
			bw.write("Name"+String.valueOf(0));bw.write("|");
			bw.write("OFR"+String.valueOf(0));bw.write("|");
			bw.write("Date"+String.valueOf(0));bw.write("|");
			bw.write("BONUS"+String.valueOf(0%100));

			for ( int i=1;i<=10000;i++)
			{
				//if (i>9998 && i%9999==0) continue;
				for (int j=0;j<5;j++)
				{
					bw.newLine();
					bw.write(String.valueOf(i));bw.write("|");
					bw.write("Name"+String.valueOf(i));bw.write("|");
					bw.write("OFR"+String.valueOf(j));bw.write("|");
					bw.write("Date"+String.valueOf(i));bw.write("|");
					//if ( i>99 && i%100 == 0 && j == 3 ) bw.write("BONUS"+String.valueOf(3));
					//else bw.write("BONUS"+String.valueOf(i%100));
					bw.write("BONUS"+String.valueOf(i%100));
				}
			}
			/*
			for ( int i=10001;i<=10500;i++)
			{
				for (int j=0;j<5;j++)
				{
					bw.newLine();
					bw.write(String.valueOf(i));bw.write("|");
					bw.write("Name"+String.valueOf(i));bw.write("|");
					bw.write("OFR"+String.valueOf(j));bw.write("|");
					bw.write("Date"+String.valueOf(i));bw.write("|");
					bw.write("BONUS"+String.valueOf(i%100));
				}
			}
			*/
			bw.close();
 
			System.out.println("Done");
 
		} catch (IOException e) {
			e.printStackTrace();
		}

		
	}

}
