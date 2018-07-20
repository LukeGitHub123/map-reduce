package uk.co.luke.advancedcomputing.MapReduce.problemspecific.datastructures;

public class Float {
	private int beforeDecimalPoint;
	private int afterFecimalPoint;
	
	public Float(int beforeDecimalPoint, int afterFecimalPoint){
		this.beforeDecimalPoint = beforeDecimalPoint;
		this.afterFecimalPoint = afterFecimalPoint;
	}
	
	public int getBeforeDecimalPoint() {
		return beforeDecimalPoint;
	}
	
	public int getAfterFecimalPoint() {
		return afterFecimalPoint;
	}
}
