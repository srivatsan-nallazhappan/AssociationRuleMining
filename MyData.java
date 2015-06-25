package spkg;

import java.io.Serializable;

public class MyData implements Serializable  {
	private String custId;
	private String ofrcd;
	private String name;
	private String endDate;
	private String bonusPoints;
	public String getCustId() {
		return custId;
	}
	public void setCustId(String custId) {
		this.custId = custId;
	}
	public String getOfrcd() {
		return ofrcd;
	}
	public void setOfrcd(String ofrcd) {
		this.ofrcd = ofrcd;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getEndDate() {
		return endDate;
	}
	public void setEndDate(String endDate) {
		this.endDate = endDate;
	}
	public String getBonusPoints() {
		return bonusPoints;
	}
	public void setBonusPoints(String bonusPoints) {
		this.bonusPoints = bonusPoints;
	}
	public Boolean isMyDataSame(MyData newOfr)
	{
		if ( name.equals(newOfr.getName()) && endDate.equals(newOfr.getEndDate()) && bonusPoints.equals(newOfr.getBonusPoints()))
		{
			return true;
		}
		return false;
	}
}
