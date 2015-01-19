package net.opentsdb.core;

interface IValuesInterval extends Aggregator.Doubles {

	public abstract boolean hasNextValue();

	public abstract double nextDoubleValue();
	
	public abstract void seekInterval(long timestamp);
	
	public abstract void moveToNextInterval();
	
	public long getIntervalTimestamp();
	
	public long currentTimeSatamp() ;

}