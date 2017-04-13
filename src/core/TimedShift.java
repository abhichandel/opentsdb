package net.opentsdb.core;

import java.util.InputMismatchException;

public class TimedShift {
	/**
	 * Is represented as minutes elapsed from 00:00:00
	 */
	private final int startTime;
	/**
	 * Is represented as minutes elapsed from 00:00:00
	 */
	private final int endTime;
	private final String name;
	private TimedShift nextShift;
	private final int minutesInDay = 24*60;
	private final int secsInDay = 24*60*60;
	private final long millisInDay = 24*60*60*1000l;
	private final long shiftSpan;

	/**
	 * @param startTime in HH:mm
	 * @param endTime in HH:mm
	 * @throws InputFormatException 
	 */
	public TimedShift(final String name, final String startTime, final String endTime) throws InputMismatchException {
		int indexOfColon = startTime.indexOf(":");
		if(indexOfColon != -1) {
			this.startTime = Integer.parseInt(startTime.substring(0, indexOfColon)) * 60 + Integer.parseInt(startTime.substring(indexOfColon + 1));
		} else {
			throw new InputMismatchException("wrong format for start time: " + startTime + ", expected HH:mm");
		}
		
		indexOfColon = endTime.indexOf(":");
		
		if(indexOfColon != -1) {
			this.endTime = Integer.parseInt(endTime.substring(0, indexOfColon)) * 60 + Integer.parseInt(endTime.substring(indexOfColon + 1));
		} else {
			throw new InputMismatchException("wrong format for start time: " + startTime + ", expected HH:mm");
		}
		this.name = name;
		if(this.startTime < this.endTime){
			this.shiftSpan = (this.endTime - this.startTime)*60*1000l;
		} else {
			this.shiftSpan = (minutesInDay - this.startTime + this.endTime)*60*1000l;
		}
	}
	
	/**
	 * 
	 * @param startTime in seconds
	 * @param endTime in seconds
	 */
	public TimedShift(String  name, int startTime, int endTime){
		this.startTime = startTime;
		this.endTime = endTime;
		this.name = name;
		if(this.startTime < this.endTime){
			this.shiftSpan = (this.endTime - this.startTime)*60*1000l;
		} else {
			this.shiftSpan = (minutesInDay - this.startTime + this.endTime)*60*1000l;
		}
	}
	
	
	public long getShiftSpan() {
		return shiftSpan;
	}

	public int getStartTime() {
		return startTime;
	}

	public int getEndTime() {
		return endTime;
	}

	public void setNextShift(TimedShift shift){
		nextShift = shift;
	}
	
	public TimedShift getNextShift(){
		return nextShift;
	}
	
	public String getName() {
		return name;
	}

	/**
	 * Start time is inclusive and end time is exclusive.
	 * @param timeInSec
	 * @return true if time lies within start and end date
	 */
	public boolean contains(int timeInSec){
		int startTimeInSec = startTime*60;
		int endTimeInSec = endTime*60;
		if(startTimeInSec < endTimeInSec)
			return startTimeInSec <= timeInSec && timeInSec < endTimeInSec;
		else
			return (startTimeInSec <= timeInSec && timeInSec <= secsInDay) || (0 <= timeInSec && timeInSec < endTimeInSec); 
	}
	
	public long getShiftStartFor(long time, int timeInSecForDay) {
		return time - timeInSecForDay*1000l + startTime*60*1000l; 
	}
	
	public long getShiftEndFor(long time, int timeInSecForDay) {
		if(startTime < endTime){
			return time - timeInSecForDay*1000l + endTime*60*1000l;
		} else {
			return time - timeInSecForDay*1000l + millisInDay + endTime*60*1000l;
		}
			
		
	}

	@Override
	public String toString() {
		return "TimedShift [name=" + name +", startTime=" + startTime/60 + ":" + startTime%60 + ", endTime=" + endTime/60 + ":" + endTime%60
				+ "]";
	}
}
