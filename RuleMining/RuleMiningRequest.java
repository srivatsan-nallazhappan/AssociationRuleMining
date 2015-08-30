package rpkg;

import java.util.ArrayList;

public class RuleMiningRequest 
{
	private String timestamp;
	private String outdir;
	private String patternFile;
	private String xmlFile;
	private String method;
	private String kVal;
	private String minSup;
	private String minConf;
	private Long transactionCnt;
	
	public Long getTransactionCnt() {
		return transactionCnt;
	}
	public void setTransactionCnt(Long transactionCnt) {
		this.transactionCnt = transactionCnt;
	}

	public String getkVal() {
		return kVal;
	}
	public void setkVal(String kVal) {
		this.kVal = kVal;
	}
	public String getMinSup() {
		return minSup;
	}
	public void setMinSup(String minSup) {
		this.minSup = minSup;
	}
	public String getMethod() {
		return method;
	}
	public void setMethod(String method) {
		this.method = method;
	}
	public String getXmlFile() {
		return xmlFile;
	}
	public void setXmlFile(String xmlFile) {
		this.xmlFile = xmlFile;
	}
	
	
	public String getPatternFile() {
		return patternFile;
	}
	public void setPatternFile(String inputFile) {
		this.patternFile = inputFile;
	}
	
	private boolean outputFormat;
	private boolean inputFormat;
	
	public String getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}
	public boolean isOutputFormat() {
		return outputFormat;
	}
	public String getOutdir() {
		return outdir;
	}
	public void setOutdir(String outdir) {
		this.outdir = outdir;
	}
	public void setOutputFormat(boolean outputFormat) {
		this.outputFormat = outputFormat;
	}
	public boolean isInputFormat() {
		return inputFormat;
	}
	public void setInputFormat(boolean inputFormat) {
		this.inputFormat = inputFormat;
	}
	public String getMinConf() {
		return minConf;
	}
	public void setMinConf(String minConf) {
		this.minConf = minConf;
	}
}
