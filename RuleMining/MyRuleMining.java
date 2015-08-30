package rpkg;


import java.io.File;
import java.io.FileOutputStream;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;


public class MyRuleMining
{
            private static FileOutputStream fpout;
            private static FileOutputStream arout;
            private static String profilePath;
            private Long transactionCount;
            private double minSupport = 0.02f;
            private double minConfidence = 0.5f;
            private HashMap < String, ArrayList< FrequentItem  > > freqPattern;
            private HashMap < HashSet<String> , Long > patCnt;
            MyRuleMining()
            {
            	
            }

            @SuppressWarnings("deprecation")
            public void readFrequentPatterns(RuleMiningRequest req) throws Exception 
            {
                freqPattern = new HashMap < String, ArrayList< FrequentItem> >();
                patCnt = new HashMap < HashSet<String> , Long > ();
                /*
                while(false)
                {
                    String Keystr = "";
                    ArrayList< HashMap < HashSet<String> , Long> > arrList = new ArrayList< HashMap < HashSet<String> , Long> >();
                    //todo
                    freqPattern.put(Keystr, arrList);
                }
                */
            }

            public void buildModel (RuleMiningRequest req) 
            {
            	try
            	{           		  	
            		profilePath = req.getOutdir() +  "/" ;
	                fpout = new FileOutputStream( profilePath + "fpout.txt" );
	                arout = new FileOutputStream( profilePath + "arout.txt" );
            		readFrequentPatterns(req);
            		buildAssociationRules(req);
            		fpout.close();
            		arout.close();
            	}
            	catch (Exception e)
            	{
            		e.printStackTrace();
            	}
            }
            
            public void buildAssociationRules (RuleMiningRequest req) 
            {
            	transactionCount = req.getTransactionCnt();
            	try
            	{
                    String pi = "|";
                    String enofl = "\n";
                    String co = "#";
                    minSupport = Double.parseDouble(req.getMinSup())/100;
                    minConfidence = Double.parseDouble(req.getMinConf())/100;
                    System.out.println("Minumum support used is " + minSupport);
                    System.out.println("Minumum confidence used is " + minConfidence);
            		for (Map.Entry<String, ArrayList< FrequentItem >> mitem : freqPattern.entrySet())
            		{
            			  String key = mitem.getKey();
            			  System.out.println("Processing pattern for the key " + key );
            			  ArrayList< FrequentItem > value = mitem.getValue();
            			  Long rFullCount = value.get(0).getCnt();
            			  HashSet<String> ss = value.get(0).getItem();
            			  String sss = "";
            			  for (String s : ss )
    					  {
    						  if ( sss.isEmpty() )
    						  {
    							  sss = s;
    						  }
    						  
    						  else
    						  {
    							  sss = sss + "," + s ;
    						  }
    					  }
            			  for (int i=0; i<value.size(); i++)
            			  {
            				  if ( value.get(i).getItem().size() > 1 )
            				  {
            					  Long occurence = value.get(i).getCnt();
            					  HashSet<String> lSidePatternSet = new HashSet<String>(value.get(i).getItem());
            					  lSidePatternSet.remove(key);
            					  String pattern = "";
            					  String lPattern = "";
            					  int c=0;
            					  for (String s : lSidePatternSet)
            					  {
            						  if ( c!=0)
            						  {
            							  pattern = pattern + "," + s ;
            							  lPattern = lPattern + "," + s ;
            						  }
            						  
            						  else
            						  {
            							  pattern = pattern + s ;
            							  lPattern = lPattern + s ;
            						  }
            						  c++;
            					  }
            					  pattern = pattern + "->" +  key;
            					  //dummyout.write(pattern.getBytes());
            					  //dummyout.write(enofl.getBytes());
            					  double support = (double)occurence.doubleValue() / transactionCount; 
            					  if ( support > minSupport )
            					  {
            						  Long noOfLSideOccurence = getLSideOccurences(lSidePatternSet,pattern);
            						  if ( noOfLSideOccurence != null )
            						  {
            							  double confidence = (double)occurence.doubleValue()/noOfLSideOccurence.doubleValue();
            							  if ( confidence > minConfidence )
            							  {
            								  double lift = (double)confidence/((double)rFullCount.doubleValue()/(double)transactionCount.doubleValue());
            								  double conviction = (double)(1- ((double) (rFullCount/transactionCount)))/(double)(1-confidence);
            								  arout.write(req.getTimestamp().getBytes());
            								  arout.write(pi.getBytes());
            								  arout.write(lPattern.getBytes());
            								  arout.write(pi.getBytes());
            								  arout.write(key.getBytes());
            								  arout.write(pi.getBytes());
            								  arout.write(String.valueOf(support).getBytes());
            								  arout.write(pi.getBytes());
            								  arout.write(String.valueOf(confidence).getBytes());
            								  arout.write(pi.getBytes());
            								  arout.write(String.valueOf(lift).getBytes());
            								  arout.write(pi.getBytes());
            								  arout.write(String.valueOf(conviction).getBytes());
            								  arout.write(enofl.getBytes());
            								  
            								  /*
            								  rulevalue.write(lPattern.getBytes());
            								  rulevalue.write(pi.getBytes());
            								  rulevalue.write(key.getBytes());
            								  rulevalue.write(pi.getBytes());
            								  rulevalue.write(String.valueOf(support).getBytes());
            								  rulevalue.write(pi.getBytes());
            								  rulevalue.write(String.valueOf(confidence).getBytes());
            								  rulevalue.write(pi.getBytes());
            								  rulevalue.write(String.valueOf(lift).getBytes());
            								  rulevalue.write(pi.getBytes());
            								  rulevalue.write(sss.getBytes());
            								  rulevalue.write(pi.getBytes());
            								  rulevalue.write(String.valueOf(rFullCount).getBytes());
            								  rulevalue.write(pi.getBytes());
            								  rulevalue.write(String.valueOf(occurence).getBytes());
            								  rulevalue.write(pi.getBytes());
            								  rulevalue.write(String.valueOf(noOfLSideOccurence).getBytes());
            								  rulevalue.write(pi.getBytes());
            								  rulevalue.write(String.valueOf(transactionCount).getBytes());
            								  rulevalue.write(enofl.getBytes());
            								  */

            							  }
            							  else
                    					  {
            								  //thresholdnotMeeting.write(pattern.getBytes());
            								  //thresholdnotMeeting.write(enofl.getBytes());            								  
                    					  }
            						  }
            						  else
            						  {
            							  //notfoundout.write(pattern.getBytes());
            							  //notfoundout.write(enofl.getBytes());
            						  }
            					  }
            					  else
            					  {
    								  //thresholdnotMeeting.write(pattern.getBytes());
    								  //thresholdnotMeeting.write(enofl.getBytes());
            					  }
            				  }
            			  }
            		}
            	}
            	catch (Exception e)
            	{
            		e.printStackTrace();
            	}
            }

			private Long getLSideOccurences(HashSet<String> lSidePatternSet, String pattern) 
			{
				Long cnt = patCnt.get(lSidePatternSet);
				try
				{
					// TODO Auto-generated method stub
					String enofl = "\n";
					if ( cnt == null )
					{
						for (String s : lSidePatternSet) 
						{
							ArrayList< FrequentItem > refList = freqPattern.get(s);
							for ( int i =0;i<refList.size();i++)
							{
								 HashSet<String> refSet = refList.get(i).getItem();
								 boolean found = true;
								 for (String ss : lSidePatternSet)
								 {
									 if ( !refSet.contains(ss) )
									 {
										 found = false;
										 break;
									 }
								 }
								 if ( found )
								 {
									 patCnt.put(lSidePatternSet, refList.get(i).getCnt());
									 //foundinsubsetmapout.write(pattern.getBytes());
									 //foundinsubsetmapout.write(enofl.getBytes());
									 return refList.get(i).getCnt();
								 }
							}
						}
					}
					else
					{
						//foundinmapout.write(pattern.getBytes());
						//foundinmapout.write(enofl.getBytes());
	
						//System.out.println("Found Matching pattern in the local cache Map");
					}
				}
				catch (Exception e)
				{
					e.printStackTrace();
				}
				return cnt;

			}
}


