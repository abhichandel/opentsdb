// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.core;


/**
 * Provides additional options that will be used when calculating rates. These
 * options are useful when working with metrics that are raw counter values, 
 * where a counter is defined by a value that always increases until it hits
 * a maximum value and then it "rolls over" to start back at 0.
 * <p/>
 * These options will only be utilized if the query is for a rate calculation
 * and if the "counter" options is set to true.
 * @since 2.0
 */
public class RateOptions {
  public static final long DEFAULT_RESET_VALUE = 0;

  public static final int MONOTONIC_INCREMEMNT_COUNTER = 0;
  public static final int BIDIRECTIONAL_INCREMEMNT_COUNTER = 1;
  public static final int BIDIRECTIONAL_DECREMENT_COUNTER = 2;
  /**
   * If true, then when calculating a rate of change assume that the metric
   * values are counters and thus non-zero, always increasing and wrap around at
   * some maximum
   */
  private boolean counter;
  
  
  private int counterType;

/**
   * If calculating a rate of change over a metric that is a counter, then this
   * value specifies the maximum value the counter will obtain before it rolls
   * over. This value will default to Long.MAX_VALUE.
   */
  private long counter_max;

  /**
   * Specifies the the rate change value which, if exceeded, will be considered
   * a data anomaly, such as a system reset of the counter, and the rate will be
   * returned as a zero value for a given data point.
   */
  private long reset_value;

  /**
   * Ctor
   */
  public RateOptions() {
    this.counter = false;
    this.counter_max = Long.MAX_VALUE;
    this.reset_value = DEFAULT_RESET_VALUE;
  }
  
  /**
   * Ctor
   * @param counter If true, indicates that the rate calculation should assume
   * that the underlying data is from a counter
   * @param counter_max Specifies the maximum value for the counter before it
   * will roll over and restart at 0
   * @param reset_value Specifies the largest rate change that is considered
   * acceptable, if a rate change is seen larger than this value then the
   * counter is assumed to have been reset
   */
  public RateOptions(final boolean counter, final long counter_max,
      final long reset_value) {
    this.counter = counter;
    this.counter_max = counter_max;
    this.reset_value = reset_value;
  }
  
  public RateOptions(final boolean counter, int counterType, final long counter_max,
	      final long reset_value) {
	  this(counter, counter_max, reset_value);
	  this.counterType = counterType;
 }
  
  /** @return Whether or not the counter flag is set */
  public boolean isCounter() {
    return counter;
  }

  /** @return The counter max value */
  public long getCounterMax() {
    return counter_max;
  }

  /** @return The optional reset value for anomaly suppression */
  public long getResetValue() {
    return reset_value;
  }

  /** @param counter Whether or not the time series should be considered counters */
  public void setIsCounter(boolean counter) {
    this.counter = counter;
  }
  
  /** @param counter_max The value at which counters roll over */
  public void setCounterMax(long counter_max) {
    this.counter_max = counter_max;
  }
  
  /** @param reset_value A difference that may be an anomaly so suppress it */
  public void setResetValue(long reset_value) {
    this.reset_value = reset_value;
  }
  
  public int getCounterType() {
	return counterType;
  }
  
  public void setCounterType(int counterType) {
	this.counterType = counterType;
  }
  
  /**
   * Generates a String version of the rate option instance in a format that 
   * can be utilized in a query.
   * @return string version of the rate option instance.
   */
  public String toString() {
    StringBuilder buf = new StringBuilder();
    buf.append('{');
    buf.append(counter);
    buf.append(',').append(counter_max);
    buf.append(',').append(reset_value);
    buf.append('}');
    return buf.toString();
  }
  
  public static RateOptions parseRateOptions(String rateSpec) {
	  String[] parts = Tags
		         .splitString(rateSpec, ',');
		     if (parts.length < 1 || parts.length > 4) {
		       throw new IllegalArgumentException(
		           "Incorrect number of values in rate options specification, must be " +
		           "counter[,counter max value,reset value, interval], recieved: "
		               + parts.length + " parts");
		     }
		     boolean counter = false;
		     int counterType = MONOTONIC_INCREMEMNT_COUNTER;
		     if("mono-inc-counter".equals(parts[0]) || "counter".equals(parts[0])){
		    	 counter = true;
		    	 counterType = MONOTONIC_INCREMEMNT_COUNTER;
		     } else if("bi-inc-counter".equals(parts[0])){
		    	 counter = true;
		    	 counterType = BIDIRECTIONAL_INCREMEMNT_COUNTER;
		     } else if("bi-dec-counter".equals(parts[0])){
		    	 counter = true;
		    	 counterType = BIDIRECTIONAL_DECREMENT_COUNTER;
		     }

		     if(counterType == MONOTONIC_INCREMEMNT_COUNTER) {
		    	 try {
				       final long max = (parts.length >= 2 && parts[1].length() > 0 ? Long
				           .parseLong(parts[1]) : Long.MAX_VALUE);
				       try {
				         final long reset = (parts.length >= 3 && parts[2].length() > 0 ? Long
				             .parseLong(parts[2]) : RateOptions.DEFAULT_RESET_VALUE);
				         return new RateOptions(counter, counterType, max, reset);
				       } catch (NumberFormatException e) {
				         throw new IllegalArgumentException(
				             "Reset value of counter was not a number, received '" + parts[2]
				                 + "'");
				       }
				     } catch (NumberFormatException e) {
				       throw new IllegalArgumentException(
				           "Max value of counter was not a number, received '" + parts[1] + "'");
				     }
		     } else {
		    	 return new RateOptions(counter, counterType, 0, 0);
		     }
  	}
}
