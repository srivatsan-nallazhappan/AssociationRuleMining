package rpkg;


import java.io.File;
import java.io.FileOutputStream;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;


public class FrequentItem
{
	private  HashSet<String> item =  null;
	 private  Long cnt = null;
     FrequentItem( HashSet<String> iitem, Long icnt)
     {
    	 item = iitem;
    	 cnt = icnt;
     }
	 public HashSet<String> getItem() {
		return item;
	}
	public void setItem(HashSet<String> item) {
		this.item = item;
	}
	public Long getCnt() {
		return cnt;
	}
	public void setCnt(Long cnt) {
		this.cnt = cnt;
	}

}


