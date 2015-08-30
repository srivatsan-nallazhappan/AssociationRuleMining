package rpkg;

import java.util.Date;

public class RuleMiningTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) 
	{
		// TODO Auto-generated method stub
		RuleMiningRequest req = new RuleMiningRequest();
		req.setMinSup(args[0]);
		req.setMinConf(args[1]);
		req.setPatternFile(args[2]);
		req.setOutdir(args[3]);
		req.setTransactionCnt(Long.parseLong(args[4]));
		Date d = new Date();
		req.setTimestamp(d.toString());
		RuleMining r = new RuleMining();
		r.buildModel(req);
	}

}
