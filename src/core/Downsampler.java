// This file is part of OpenTSDB.
// Copyright (C) 2014  The OpenTSDB Authors.
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

import java.util.Calendar;
import java.util.NoSuchElementException;
import java.util.TimeZone;


/**
 * Iterator that downsamples data points using an {@link Aggregator}.
 */
public class Downsampler implements SeekableView, DataPoint {

  /** Function to use for downsampling. */
  private final Aggregator downsampler;
  /** Iterator to iterate the values of the current interval. */
  private final IValuesInterval values_in_interval;
  /** Last normalized timestamp */ 
  private long timestamp;
  /** Last value as a double */
  private double value;
  
  /**
   * Ctor.
   * @param source The iterator to access the underlying data.
   * @param interval_ms The interval in milli seconds wanted between each data
   * point.
   * @param downsampler The downsampling function to use.
   */
  Downsampler(final SeekableView source,
              final long interval_ms,
              final Aggregator downsampler) {
	  this.values_in_interval = new ValuesInInterval(source, interval_ms, 0);
	    this.downsampler = downsampler;
  }
  
  /**
   * Ctor.
   * @param source The iterator to access the underlying data.
   * @param interval_ms The interval in milli seconds wanted between each data
   * point.
   * @param downsampler The downsampling function to use.
   */
  Downsampler(final SeekableView source,
              final long interval_ms,
              final Aggregator downsampler,
              final String dimension,
              final TimeZone tz,
              final RateOptions options) {
	  
//              case 'm': multiplier = 60; break;               // minutes
//              case 'h': multiplier = 3600; break;             // hours
//              case 'w': multiplier = 3600 * 24 * 7; break;    // weeks
//	int offset = 60*60*1000 - tz.getOffset(Calendar.ZONE_OFFSET)%(60*60*1000);;
	long offset = tz.getOffset(Calendar.ZONE_OFFSET);
	final String timeDim = dimension.substring(dimension.length() - 1);
	IValuesInterval valuesInterval;
	if(!options.isCounter()){
		valuesInterval = new ValuesInInterval(source, interval_ms, offset);
	} else if(options.getCounterType() == RateOptions.BIDIRECTIONAL_DECREMENT_COUNTER) {
		valuesInterval = new DecCounterValuesInInterval(source, interval_ms, offset);
	} else if(options.getCounterType() == RateOptions.BIDIRECTIONAL_INCREMEMNT_COUNTER) {
		valuesInterval = new IncCounterValuesInInterval(source, interval_ms, offset);
	} else {
		if("n".equals(timeDim)) {
			valuesInterval = new MonthlyCounterValuesInInterval(source, interval_ms, offset, options.getCounterMax(), options.getResetValue(), tz);
		} else {
			valuesInterval = new CounterValuesInInterval(source, interval_ms, offset, options.getCounterMax(), options.getResetValue());
		}
	}
	
	switch (timeDim) {
	case "n":
		this.values_in_interval = new MonthlyInterval(valuesInterval,tz ,downsampler);
		break;
	case "y":
		this.values_in_interval = new YearlyInterval(valuesInterval, tz, downsampler);
		break;
	case "d":
		this.values_in_interval = valuesInterval;
		break;
	case "o":
		this.values_in_interval = new OtherValuesInInterval(source, interval_ms, offset, options.getCounterMax(), options.getResetValue(), tz);
		break;
	default:
		if(options.isCounter()){
			if(options.getCounterType() == RateOptions.BIDIRECTIONAL_DECREMENT_COUNTER) {
				this.values_in_interval = new DecCounterValuesInInterval(source, interval_ms, offset);
			} else if(options.getCounterType() == RateOptions.BIDIRECTIONAL_INCREMEMNT_COUNTER) {
				this.values_in_interval = new IncCounterValuesInInterval(source, interval_ms, offset);
			} else {
				this.values_in_interval = new CounterValuesInInterval(source, interval_ms, offset, options.getCounterMax(), options.getResetValue());
			}
		} else
			this.values_in_interval = new ValuesInInterval(source, interval_ms, offset);
		break;
	}
	
	this.downsampler = downsampler;
	
//	if(options.isCounter()){
		//offset_for_tz != 30*60*1000 && offset_for_tz != 15*60*1000 && offset_for_tz != 0) { //this offset can be either (0|30|45)*60*1000
		//60*60*1000 - DateTime.timezones.get(timezone).getOffset(Calendar.ZONE_OFFSET)%(60*60*1000);
//		this.values_in_interval = new CounterValuesInInterval(source, interval_ms, offset, options.getCounterMax(), options.getResetValue());
//		this.values_in_interval = new YearlyInterval(new CounterValuesInInterval(source, 24*60*60*1000, 0, options.getCounterMax(), options.getResetValue()), downsampler);
//		this.values_in_interval = new MonthlyInterval(new ValuesInInterval(source, interval_ms, 24*60*60*1000), downsampler);
//	    this.downsampler = downsampler;
//	}
  }

  // ------------------ //
  // Iterator interface //
  // ------------------ //

  public boolean hasNext() {
    return values_in_interval.hasNextValue();
  }

  public DataPoint next() {
    if (hasNext()) {
      value = downsampler.runDouble(values_in_interval);
      timestamp = values_in_interval.getIntervalTimestamp();
      values_in_interval.moveToNextInterval();
      return this;
    }
    throw new NoSuchElementException("no more data points in " + this);
  }

  public void remove() {
    throw new UnsupportedOperationException();
  }

  // ---------------------- //
  // SeekableView interface //
  // ---------------------- //

  public void seek(final long timestamp) {
    values_in_interval.seekInterval(timestamp);
  }

  @Override
  public String toString() {
    final StringBuilder buf = new StringBuilder();
    buf.append("Downsampler: ")
    	//TODO: how to set interval
//       .append("interval_ms=").append(values_in_interval.interval_ms)
       .append(", downsampler=").append(downsampler)
       .append(", current data=(timestamp=").append(timestamp)
       .append(", value=").append(value)
       .append("), values_in_interval=").append(values_in_interval);
   return buf.toString();
  }

  public long timestamp() {
    return timestamp;
  }

  public boolean isInteger() {
    return false;
  }

  public long longValue() {
    throw new ClassCastException("Downsampled values are doubles");
  }

  public double doubleValue() {
    return value;
  }

  public double toDouble() {
    return value;
  }
  
  private static class YearlyInterval extends MonthlyInterval {
		
		public YearlyInterval(final IValuesInterval values_in_interval, final TimeZone tz, final Aggregator downsampler) {
			super(values_in_interval, tz, downsampler);
		}

	    /**
	     * Resets the current interval with the interval of the timestamp of
	     * the next value read from source. It is the first value of the next
	     * interval. */
	    protected void resetEndOfInterval() {
	      if (values_in_interval.hasNextValue()) {
	    	  //TODO: set timezone
	    	Calendar c = Calendar.getInstance(tz);
	    	c.setTimeInMillis(values_in_interval.currentTimeSatamp());
	    	c.set(c.get(Calendar.YEAR) + 1, Calendar.JANUARY, 1);
	    	timestamp_end_interval = c.getTimeInMillis();
	      }
	    }

	    /** Returns the representative timestamp of the current interval. */
	    public long getIntervalTimestamp() {
	      // NOTE: It is well-known practice taking the start time of
	      // a downsample interval as a representative timestamp of it. It also
	      // provides the correct context for seek.
	    	Calendar c = Calendar.getInstance(tz);
	    	c.setTimeInMillis(timestamp_end_interval);
	    	c.set(c.get(Calendar.YEAR) - 1,Calendar.JANUARY, 1);
	      return c.getTimeInMillis();
	    }
	    
	    /** Advances the interval iterator to the given timestamp. */
	    public void seekInterval(long timestamp) {
	      // To make sure that the interval of the given timestamp is fully filled,
	      // rounds up the seeking timestamp to the smallest timestamp that is
	      // a multiple of the interval and is greater than or equal to the given
	      // timestamp..
	      Calendar c = Calendar.getInstance(tz);
	      c.setTimeInMillis(timestamp);
	      if(c.get(Calendar.DAY_OF_MONTH) != 1 && c.get(Calendar.MONTH) != Calendar.JANUARY) {
	    	  c.set(c.get(Calendar.YEAR) + 1, Calendar.JANUARY, 1, 0, 0, 0);
	      }
	      values_in_interval.seekInterval(c.getTimeInMillis());
	      initialized = false;
	    }

	  }
  
  private static class MonthlyInterval implements IValuesInterval {
	
	protected final IValuesInterval values_in_interval;
	protected long timestamp_end_interval = Long.MIN_VALUE;
	protected final Aggregator downsampler;
	/** True if it is initialized for iterating intervals. */
	protected boolean initialized = false;
	protected final TimeZone tz;
	
	public MonthlyInterval(final IValuesInterval values_in_interval, final TimeZone tz, final Aggregator downsampler) {
		this.values_in_interval = values_in_interval; 
		this.downsampler = downsampler;
		this.tz = tz;
	}

	/** Initializes to iterate intervals. */
    private void initializeIfNotDone() {
      // NOTE: Delay initialization is required to not access any data point
      // from the source until a user requests it explicitly to avoid the severe
      // performance penalty by accessing the unnecessary first data of a span.
      if (!initialized) {
        initialized = true;
//        moveToNextValue();
        resetEndOfInterval();
      }
    }
    
    /**
     * Resets the current interval with the interval of the timestamp of
     * the next value read from source. It is the first value of the next
     * interval. */
    protected void resetEndOfInterval() {
      if (values_in_interval.hasNextValue()) {
    	  //TODO: set timezone
    	Calendar c = Calendar.getInstance(tz);
    	c.setTimeInMillis(values_in_interval.currentTimeSatamp());
    	int currMonth = c.get(Calendar.MONTH);
    	if(currMonth < 11) {
    		c.set(Calendar.MONTH, currMonth + 1);
    		c.set(Calendar.DAY_OF_MONTH, 1);
    	} else {
    		c.set(c.get(Calendar.YEAR) + 1, Calendar.JANUARY, 1);
    	}
    	 c.set(Calendar.HOUR_OF_DAY, 0);
         c.set(Calendar.MINUTE, 0);
         c.set(Calendar.SECOND, 0);
         c.set(Calendar.MILLISECOND, 0);
    	timestamp_end_interval = c.getTimeInMillis();
      }
    }
    
    /**
     * Resets the current interval with the interval of the timestamp of
     * the next value read from source. It is the first value of the next
     * interval. */
    protected void resetIntervalInMS() {
      if (values_in_interval.hasNextValue()) {
    	  //TODO: set timezone
    	Calendar c = Calendar.getInstance(tz);
    	c.setTimeInMillis(values_in_interval.currentTimeSatamp());
    	values_in_interval.setIntervalInMs(c.getActualMaximum(Calendar.DAY_OF_MONTH)*24*60*60*1000l);
      }
    }

    /** Moves to the next available interval. */
    public void moveToNextInterval() {
      initializeIfNotDone();
      resetEndOfInterval();
      resetIntervalInMS();
    }

	@Override
	public boolean hasNextValue() {
		initializeIfNotDone();
		return values_in_interval.hasNextValue() && values_in_interval.currentTimeSatamp() < timestamp_end_interval;
	}
	
    /** Returns the representative timestamp of the current interval. */
    public long getIntervalTimestamp() {
      // NOTE: It is well-known practice taking the start time of
      // a downsample interval as a representative timestamp of it. It also
      // provides the correct context for seek.
    	Calendar c = Calendar.getInstance(tz);
    	c.setTimeInMillis(timestamp_end_interval);
    	int currMonth = c.get(Calendar.MONTH);
    	if(currMonth > Calendar.JANUARY) {
    		c.set(Calendar.MONTH, currMonth - 1);
    		c.set(Calendar.DAY_OF_MONTH, 1);
    	} else {
    		c.set(c.get(Calendar.YEAR) - 1, Calendar.DECEMBER, 1);
    	}
      return c.getTimeInMillis();
    }
    
    /** Advances the interval iterator to the given timestamp. */
    public void seekInterval(long timestamp) {
      // To make sure that the interval of the given timestamp is fully filled,
      // rounds up the seeking timestamp to the smallest timestamp that is
      // a multiple of the interval and is greater than or equal to the given
      // timestamp..
      Calendar c = Calendar.getInstance(tz);
      c.setTimeInMillis(timestamp);
      if(c.get(Calendar.DAY_OF_MONTH) != 1) {
    	  c.set(Calendar.MONTH, c.get(Calendar.MONTH) + 1);
    	  c.set(Calendar.DAY_OF_MONTH, 1);
      }
      c.set(Calendar.HOUR_OF_DAY, 0);
      c.set(Calendar.MINUTE, 0);
      c.set(Calendar.SECOND, 0);
      c.set(Calendar.MILLISECOND, 0);
      values_in_interval.setIntervalInMs(c.getActualMaximum(Calendar.DAY_OF_MONTH)*24*60*60*1000l);
      values_in_interval.seekInterval(c.getTimeInMillis());
      initialized = false;
    }

	@Override
	public double nextDoubleValue() {
		if (hasNextValue()) {
			double value = downsampler.runDouble(values_in_interval);
			values_in_interval.moveToNextInterval();
	        return value;
	      }
	      throw new NoSuchElementException("no more values in interval of "
	          + timestamp_end_interval);
	}

	@Override
	public long currentTimeSatamp() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void setIntervalInMs(long interval) {
		// TODO Auto-generated method stub
		
	}
  }
  
  
  /** Iterates source values for an interval. */
  private static class ValuesInInterval implements Aggregator.Doubles, IValuesInterval {

    /** The iterator of original source values. */
    protected final SeekableView source;
    /** The sampling interval in milliseconds. */
    protected long interval_ms;
    
    protected final long offset;
    /** The end of the current interval. */
    protected long timestamp_end_interval = Long.MIN_VALUE;
    /** True if the last value was successfully extracted from the source. */
    protected boolean has_next_value_from_source = false;
    /** The last data point extracted from the source. */
    protected DataPoint next_dp = null;

    /** True if it is initialized for iterating intervals. */
    private boolean initialized = false;

    /**
     * Constructor.
     * @param source The iterator to access the underlying data.
     * @param interval_ms Downsampling interval.
     */
    ValuesInInterval(final SeekableView source, final long interval_ms, final long offset) {
      this.source = source;
      this.interval_ms = interval_ms;
      this.timestamp_end_interval = interval_ms;
      this.offset = offset;
    }

    /** Initializes to iterate intervals. */
    private void initializeIfNotDone() {
      // NOTE: Delay initialization is required to not access any data point
      // from the source until a user requests it explicitly to avoid the severe
      // performance penalty by accessing the unnecessary first data of a span.
      if (!initialized) {
        initialized = true;
        moveToNextValue();
        resetEndOfInterval();
      }
    }

    /** Extracts the next value from the source. */
    private void moveToNextValue() {
      if (source.hasNext()) {
        has_next_value_from_source = true;
        next_dp = source.next();
      } else {
        has_next_value_from_source = false;
      }
    }

    public long currentTimeSatamp() {
    	return next_dp.timestamp();
    }
    
    /**
     * Resets the current interval with the interval of the timestamp of
     * the next value read from source. It is the first value of the next
     * interval. */
    private void resetEndOfInterval() {
      if (has_next_value_from_source) {
        // Sets the end of the interval of the timestamp.
        timestamp_end_interval = alignTimestamp(next_dp.timestamp()) + 
            interval_ms;
      }
    }

    /** Moves to the next available interval. */
    public void moveToNextInterval() {
      initializeIfNotDone();
      resetEndOfInterval();
    }

    /** Advances the interval iterator to the given timestamp. */
    public void seekInterval(long timestamp) {
      // To make sure that the interval of the given timestamp is fully filled,
      // rounds up the seeking timestamp to the smallest timestamp that is
      // a multiple of the interval and is greater than or equal to the given
      // timestamp..
      source.seek(alignTimestamp(timestamp + interval_ms - 1));
      initialized = false;
    }

    /** Returns the representative timestamp of the current interval. */
    public long getIntervalTimestamp() {
      // NOTE: It is well-known practice taking the start time of
      // a downsample interval as a representative timestamp of it. It also
      // provides the correct context for seek.
      return alignTimestamp(timestamp_end_interval - interval_ms);
    }

    /** Returns timestamp aligned by interval. */
    private long alignTimestamp(long timestamp) {
      return timestamp - ((timestamp - offset*-1l) % interval_ms);
    }

    // ---------------------- //
    // Doubles interface //
    // ---------------------- //

    /* (non-Javadoc)
	 * @see net.opentsdb.core.IValuesInterval#hasNextValue()
	 */
	@Override
    public boolean hasNextValue() {
      initializeIfNotDone();
      return has_next_value_from_source &&
          next_dp.timestamp() < timestamp_end_interval;
    }

    /* (non-Javadoc)
	 * @see net.opentsdb.core.IValuesInterval#nextDoubleValue()
	 */
	@Override
    public double nextDoubleValue() {
      if (hasNextValue()) {
        double value = next_dp.toDouble();
        moveToNextValue();
        return value;
      }
      throw new NoSuchElementException("no more values in interval of "
          + timestamp_end_interval);
    }

    @Override
    public String toString() {
      final StringBuilder buf = new StringBuilder();
      buf.append("ValuesInInterval: ")
         .append("interval_ms=").append(interval_ms)
         .append(", timestamp_end_interval=").append(timestamp_end_interval)
         .append(", has_next_value_from_source=")
         .append(has_next_value_from_source);
      if (has_next_value_from_source) {
        buf.append(", nextValue=(").append(next_dp).append(')');
      }
      buf.append(", source=").append(source);
      return buf.toString();
    }

	@Override
	public void setIntervalInMs(long interval) {
		interval_ms =interval;
	}
  }
  
  /** Iterates source values for a decreasing interval and ignores the increment*/
  private static class IncCounterValuesInInterval extends ValuesInInterval {

    /**
     * Constructor.
     * @param source The iterator to access the underlying data.
     * @param options Rate options for this interval
     */
	IncCounterValuesInInterval(final SeekableView source, final long interval_ms, final long offset) {
      super(source, interval_ms, offset);
    }

    /** Extracts the next value from the source. */
    private void moveToNextValue() {
    	super.moveToNextValue();
    	//next to next value is also required to get the difference.
        if(!source.hasNext()){
        	has_next_value_from_source = false;
        }
    }

    // ---------------------- //
    // Doubles interface //
    // ---------------------- //

    @Override
    public double nextDoubleValue() {    	
      if (hasNextValue()) {
    	MutableDataPoint prev_data = new MutableDataPoint();  
        prev_data.reset(next_dp);
        moveToNextValue();
        final long t0 = prev_data.timestamp();
        final long t1 = next_dp.timestamp();
        if (t1 <= t0) {
          throw new IllegalStateException(
              "Next timestamp (" + t1 + ") is supposed to be "
              + " strictly greater than the previous one (" + t0 + "), but it's"
              + " not.  this=" + this);
        }
        double difference;
        if (prev_data.isInteger() && next_dp.isInteger()) {
          // NOTE: Calculates in the long type to avoid precision loss
          // while converting long values to double values if both values are long.
          // NOTE: Ignores the integer overflow.
          difference = next_dp.longValue() - prev_data.longValue();
        } else {
          difference = next_dp.toDouble() - prev_data.toDouble();
        }
        
        if (difference < 0) {
            difference = 0;
        }
        
        return difference;
      }
      throw new NoSuchElementException("no more values in interval of "
          + timestamp_end_interval);
    }
  }
  
  /** Iterates source values for a decreasing interval and ignores the increment*/
  private static class DecCounterValuesInInterval extends ValuesInInterval {

    /**
     * Constructor.
     * @param source The iterator to access the underlying data.
     * @param options Rate options for this interval
     */
    DecCounterValuesInInterval(final SeekableView source, final long interval_ms, final long offset) {
      super(source, interval_ms, offset);
    }

    /** Extracts the next value from the source. */
    private void moveToNextValue() {
    	super.moveToNextValue();
    	//next to next value is also required to get the difference.
        if(!source.hasNext()){
        	has_next_value_from_source = false;
        }
    }

    // ---------------------- //
    // Doubles interface //
    // ---------------------- //

    @Override
    public double nextDoubleValue() {    	
      if (hasNextValue()) {
    	MutableDataPoint prev_data = new MutableDataPoint();  
        prev_data.reset(next_dp);
        moveToNextValue();
        final long t0 = prev_data.timestamp();
        final long t1 = next_dp.timestamp();
        if (t1 <= t0) {
          throw new IllegalStateException(
              "Next timestamp (" + t1 + ") is supposed to be "
              + " strictly greater than the previous one (" + t0 + "), but it's"
              + " not.  this=" + this);
        }
        double difference;
        if (prev_data.isInteger() && next_dp.isInteger()) {
          // NOTE: Calculates in the long type to avoid precision loss
          // while converting long values to double values if both values are long.
          // NOTE: Ignores the integer overflow.
          difference = next_dp.longValue() - prev_data.longValue();
        } else {
          difference = next_dp.toDouble() - prev_data.toDouble();
        }
        
        if (difference > 0) {
            difference = 0;
        }
        
        return difference*-1;
      }
      throw new NoSuchElementException("no more values in interval of "
          + timestamp_end_interval);
    }
  }
  
  /** Iterates source values for an interval. */
  private static class CounterValuesInInterval extends ValuesInInterval {

    /** The last data point extracted from the source. */
    private final long maxValue;
    private final long resetRate;

    /**
     * Constructor.
     * @param source The iterator to access the underlying data.
     * @param options Rate options for this interval
     */
    CounterValuesInInterval(final SeekableView source, final long interval_ms, final long offset, final long max, final long resetRate) {
      super(source, interval_ms, offset);
      this.maxValue = max;
      this.resetRate = resetRate;
    }

    /** Extracts the next value from the source. */
    private void moveToNextValue() {
    	super.moveToNextValue();
    	//next to next value is also required to get the difference.
        if(!source.hasNext()){
        	has_next_value_from_source = false;
        }
    }

    // ---------------------- //
    // Doubles interface //
    // ---------------------- //

    @Override
    public double nextDoubleValue() {    	
      if (hasNextValue()) {
    	MutableDataPoint prev_data = new MutableDataPoint();  
        prev_data.reset(next_dp);
        moveToNextValue();
        final long t0 = prev_data.timestamp();
        final long t1 = next_dp.timestamp();
        if (t1 <= t0) {
          throw new IllegalStateException(
              "Next timestamp (" + t1 + ") is supposed to be "
              + " strictly greater than the previous one (" + t0 + "), but it's"
              + " not.  this=" + this);
        }
        double difference;
        if (prev_data.isInteger() && next_dp.isInteger()) {
          // NOTE: Calculates in the long type to avoid precision loss
          // while converting long values to double values if both values are long.
          // NOTE: Ignores the integer overflow.
          difference = next_dp.longValue() - prev_data.longValue();
        } else {
          difference = next_dp.toDouble() - prev_data.toDouble();
        }
        
        if (difference < 0) {
            if (prev_data.isInteger() && next_dp.isInteger()) {
              // NOTE: Calculates in the long type to avoid precision loss
              // while converting long values to double values if both values are long.
              difference = maxValue - prev_data.longValue() +
                  next_dp.longValue();
            } else {
              difference = maxValue - prev_data.toDouble() +
                  next_dp.toDouble();
            }
            if(resetRate != 0 && difference*1000/(double)(t1 - t0) > resetRate){
            	difference = 0;
//            	if (prev_data.isInteger() && next_dp.isInteger()) {
//                    // NOTE: Calculates in the long type to avoid precision loss
//                    // while converting long values to double values if both values are long.
//                    difference = next_dp.longValue();
//                  } else {
//                    difference = next_dp.toDouble();
//                  }
            }
        }
        return difference;
      }
      throw new NoSuchElementException("no more values in interval of "
          + timestamp_end_interval);
    }
  }
  
  
  /** Iterates source values for an interval. */
  private static class MonthlyValuesInInterval implements Aggregator.Doubles, IValuesInterval {

    /** The iterator of original source values. */
    protected final SeekableView source;
    /** The sampling interval in milliseconds. */
    protected long interval_ms;
    
    protected final long offset;
    /** The end of the current interval. */
    protected long timestamp_end_interval = Long.MIN_VALUE;
    /** True if the last value was successfully extracted from the source. */
    protected boolean has_next_value_from_source = false;
    /** The last data point extracted from the source. */
    protected DataPoint next_dp = null;
    private final TimeZone tz;
    Calendar c;

    /** True if it is initialized for iterating intervals. */
    private boolean initialized = false;

    /**
     * Constructor.
     * @param source The iterator to access the underlying data.
     * @param interval_ms Downsampling interval.
     */
    MonthlyValuesInInterval(final SeekableView source, final long interval_ms, final long offset, final TimeZone tz) {
      this.source = source;
      this.interval_ms = interval_ms;
      this.timestamp_end_interval = interval_ms;
      this.offset = offset;
      this.tz = tz;
      c = Calendar.getInstance(tz);
    }

    /** Initializes to iterate intervals. */
    private void initializeIfNotDone() {
      // NOTE: Delay initialization is required to not access any data point
      // from the source until a user requests it explicitly to avoid the severe
      // performance penalty by accessing the unnecessary first data of a span.
      if (!initialized) {
        initialized = true;
        moveToNextValue();
        resetEndOfInterval();
      }
    }

    /** Extracts the next value from the source. */
    private void moveToNextValue() {
      if (source.hasNext()) {
        has_next_value_from_source = true;
        next_dp = source.next();
      } else {
        has_next_value_from_source = false;
      }
    }

    public long currentTimeSatamp() {
    	return next_dp.timestamp();
    }
    
    /**
     * Resets the current interval with the interval of the timestamp of
     * the next value read from source. It is the first value of the next
     * interval. */
    private void resetEndOfInterval() {
      if (has_next_value_from_source) {
        // Sets the end of the interval of the timestamp.
      	c.setTimeInMillis(currentTimeSatamp());
      	int currMonth = c.get(Calendar.MONTH);
      	if(currMonth < 11) {
      		c.set(Calendar.MONTH, currMonth + 1);
      		c.set(Calendar.DAY_OF_MONTH, 1);
      	} else {
      		c.set(c.get(Calendar.YEAR) + 1, Calendar.JANUARY, 1);
      	}
      	
      	c.set(Calendar.HOUR_OF_DAY, 0);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MILLISECOND, 0);
        
        timestamp_end_interval = c.getTimeInMillis();
      }
    }

    /** Moves to the next available interval. */
    public void moveToNextInterval() {
      initializeIfNotDone();
      resetEndOfInterval();
    }

    /** Advances the interval iterator to the given timestamp. */
    public void seekInterval(long timestamp) {
      // To make sure that the interval of the given timestamp is fully filled,
      // rounds up the seeking timestamp to the smallest timestamp that is
      // a multiple of the interval and is greater than or equal to the given
      // timestamp..
      source.seek(timestamp);
      initialized = false;
    }

    /** Returns the representative timestamp of the current interval. */
    public long getIntervalTimestamp() {
      // NOTE: It is well-known practice taking the start time of
      // a downsample interval as a representative timestamp of it. It also
      // provides the correct context for seek.
//      return alignTimestamp(timestamp_end_interval - interval_ms);
    	c.setTimeInMillis(timestamp_end_interval);
    	int currMonth = c.get(Calendar.MONTH);
    	if(currMonth > Calendar.JANUARY) {
    		c.set(Calendar.MONTH, currMonth - 1);
    		c.set(Calendar.DAY_OF_MONTH, 1);
    	} else {
    		c.set(c.get(Calendar.YEAR) - 1, Calendar.DECEMBER, 1);
    	}
      return c.getTimeInMillis();
    }

//    /** Returns timestamp aligned by interval. */
//    private long alignTimestamp(long timestamp) {
//      return timestamp - ((timestamp - offset*-1l) % interval_ms);
//    }

    // ---------------------- //
    // Doubles interface //
    // ---------------------- //

    /* (non-Javadoc)
	 * @see net.opentsdb.core.IValuesInterval#hasNextValue()
	 */
	@Override
    public boolean hasNextValue() {
      initializeIfNotDone();
      return has_next_value_from_source &&
          next_dp.timestamp() < timestamp_end_interval;
    }

    /* (non-Javadoc)
	 * @see net.opentsdb.core.IValuesInterval#nextDoubleValue()
	 */
	@Override
    public double nextDoubleValue() {
      if (hasNextValue()) {
        double value = next_dp.toDouble();
        moveToNextValue();
        return value;
      }
      throw new NoSuchElementException("no more values in interval of "
          + timestamp_end_interval);
    }

    @Override
    public String toString() {
      final StringBuilder buf = new StringBuilder();
      buf.append("ValuesInInterval: ")
         .append("interval_ms=").append(interval_ms)
         .append(", timestamp_end_interval=").append(timestamp_end_interval)
         .append(", has_next_value_from_source=")
         .append(has_next_value_from_source);
      if (has_next_value_from_source) {
        buf.append(", nextValue=(").append(next_dp).append(')');
      }
      buf.append(", source=").append(source);
      return buf.toString();
    }

	@Override
	public void setIntervalInMs(long interval) {
		interval_ms =interval;
	}
  }
  
  /** Iterates source values for an interval. */
  private static class MonthlyCounterValuesInInterval extends MonthlyValuesInInterval {

    /** The last data point extracted from the source. */
    private final long maxValue;
    private final long resetRate;

    /**
     * Constructor.
     * @param source The iterator to access the underlying data.
     * @param options Rate options for this interval
     */
    MonthlyCounterValuesInInterval(final SeekableView source, final long interval_ms, final long offset, final long max, final long resetRate, final TimeZone tz) {
      super(source, interval_ms, offset, tz);
      this.maxValue = max;
      this.resetRate = resetRate;
    }

    /** Extracts the next value from the source. */
    private void moveToNextValue() {
    	super.moveToNextValue();
    	//next to next value is also required to get the difference.
        if(!source.hasNext()){
        	has_next_value_from_source = false;
        }
    }

    // ---------------------- //
    // Doubles interface //
    // ---------------------- //

    @Override
    public double nextDoubleValue() {    	
      if (hasNextValue()) {
    	MutableDataPoint prev_data = new MutableDataPoint();  
        prev_data.reset(next_dp);
        moveToNextValue();
        final long t0 = prev_data.timestamp();
        final long t1 = next_dp.timestamp();
        if (t1 <= t0) {
          throw new IllegalStateException(
              "Next timestamp (" + t1 + ") is supposed to be "
              + " strictly greater than the previous one (" + t0 + "), but it's"
              + " not.  this=" + this);
        }
        double difference;
        if (prev_data.isInteger() && next_dp.isInteger()) {
          // NOTE: Calculates in the long type to avoid precision loss
          // while converting long values to double values if both values are long.
          // NOTE: Ignores the integer overflow.
          difference = next_dp.longValue() - prev_data.longValue();
        } else {
          difference = next_dp.toDouble() - prev_data.toDouble();
        }
        
        if (difference < 0) {
            if (prev_data.isInteger() && next_dp.isInteger()) {
              // NOTE: Calculates in the long type to avoid precision loss
              // while converting long values to double values if both values are long.
              difference = maxValue - prev_data.longValue() +
                  next_dp.longValue();
            } else {
              difference = maxValue - prev_data.toDouble() +
                  next_dp.toDouble();
            }
            if(resetRate != 0 && difference*1000/(t1 - t0) > resetRate){
            	difference = 0;
//            	if (prev_data.isInteger() && next_dp.isInteger()) {
//                    // NOTE: Calculates in the long type to avoid precision loss
//                    // while converting long values to double values if both values are long.
//                    difference = next_dp.longValue();
//                  } else {
//                    difference = next_dp.toDouble();
//                  }
            }
        }
        return difference;
      }
      throw new NoSuchElementException("no more values in interval of "
          + timestamp_end_interval);
    }
  }
  
  /** Iterates source values for an interval. */
  private static class OtherValuesInInterval implements Aggregator.Doubles, IValuesInterval {

    /** The iterator of original source values. */
    protected final SeekableView source;
    
    protected final long offset;
    /** The end of the current interval. */
    protected long timestamp_start = Long.MIN_VALUE;
    /** True if the last value was successfully extracted from the source. */
    protected boolean has_next_value_from_source = false;
    /** The last data point extracted from the source. */
    protected DataPoint next_dp = null;
    MutableDataPoint prev_dp = new MutableDataPoint(); 
    private final TimeZone tz;
    Calendar c;
    private final long maxValue;
    private final long resetRate;

    /** True if it is initialized for iterating intervals. */
    private boolean initialized = false;

    /**
     * Constructor.
     * @param source The iterator to access the underlying data.
     * @param interval_ms Downsampling interval.
     */
    OtherValuesInInterval(final SeekableView source, final long interval_ms, final long offset, final long max, final long resetRate, final TimeZone tz) {
      this.source = source;
      this.offset = offset;
      this.tz = tz;
      this.maxValue = max;
      this.resetRate = resetRate;
      c = Calendar.getInstance(tz);
    }

    /** Initializes to iterate intervals. */
    private void initializeIfNotDone() {
      // NOTE: Delay initialization is required to not access any data point
      // from the source until a user requests it explicitly to avoid the severe
      // performance penalty by accessing the unnecessary first data of a span.
      if (!initialized) {
        initialized = true;
        moveToNextValue();
        moveToNextValue();
        if(!hasNextValue()) {
        	nextInterval();
        }
        resetStartOfInterval();
      }
    }

    /** Extracts the next value from the source. */
    private void moveToNextValue() {
      if (source.hasNext()) {
        has_next_value_from_source = true;
        if(next_dp != null)
        prev_dp.reset(next_dp);
        next_dp = source.next();
      } else {
        has_next_value_from_source = false;
      }
      if(!source.hasNext()){
      	has_next_value_from_source = false;
      }
    }

    public long currentTimeSatamp() {
    	return next_dp.timestamp();
    }
    
    private void resetStartOfInterval(){
    	if(prev_dp != null)
    		timestamp_start = prev_dp.timestamp();
    }
    
    /** Moves to the next available interval. */
    public void moveToNextInterval() {
      initializeIfNotDone();
      nextInterval();
      resetStartOfInterval();
    }
    
    private void nextInterval() {
    	while(source.hasNext()){
    		moveToNextValue();
    		if(hasNextValue()) {
    			break;
    		}
    	}
    }

    /** Advances the interval iterator to the given timestamp. */
    public void seekInterval(long timestamp) {
      // To make sure that the interval of the given timestamp is fully filled,
      // rounds up the seeking timestamp to the smallest timestamp that is
      // a multiple of the interval and is greater than or equal to the given
      // timestamp..
      source.seek(timestamp);
      initialized = false;
    }

    /** Returns the representative timestamp of the current interval. */
    public long getIntervalTimestamp() {
      return timestamp_start;
    }

//    /** Returns timestamp aligned by interval. */
//    private long alignTimestamp(long timestamp) {
//      return timestamp - ((timestamp - offset*-1l) % interval_ms);
//    }

    // ---------------------- //
    // Doubles interface //
    // ---------------------- //

    /* (non-Javadoc)
	 * @see net.opentsdb.core.IValuesInterval#hasNextValue()
	 */
	@Override
    public boolean hasNextValue() {
      initializeIfNotDone();
      double difference;
      if(has_next_value_from_source) {
    	  if (prev_dp.isInteger() && next_dp.isInteger()) {
    	        // NOTE: Calculates in the long type to avoid precision loss
    	        // while converting long values to double values if both values are long.
    	        // NOTE: Ignores the integer overflow.
    	        difference = next_dp.longValue() - prev_dp.longValue();
    	      } else {
    	        difference = next_dp.toDouble() - prev_dp.toDouble();
    	      }
    	      return difference != 0; 
      } else {
    	  return false;
      }
      
    }

	// ---------------------- //
    // Doubles interface //
    // ---------------------- //

    @Override
    public double nextDoubleValue() {    	
      if (hasNextValue()) {
    	MutableDataPoint prev_prev_data = new MutableDataPoint();  
    	prev_prev_data.reset(prev_dp);
          moveToNextValue();
        final long t0 = prev_prev_data.timestamp();
        final long t1 = prev_dp.timestamp();
        if (t1 <= t0) {
          throw new IllegalStateException(
              "Next timestamp (" + t1 + ") is supposed to be "
              + " strictly greater than the previous one (" + t0 + "), but it's"
              + " not.  this=" + this);
        }
        double difference;
        if (prev_dp.isInteger() && prev_prev_data.isInteger()) {
          // NOTE: Calculates in the long type to avoid precision loss
          // while converting long values to double values if both values are long.
          // NOTE: Ignores the integer overflow.
          difference = prev_dp.longValue() - prev_prev_data.longValue();
        } else {
          difference = prev_dp.toDouble() - prev_prev_data.toDouble();
        }
        
        if (difference < 0) {
            if (prev_prev_data.isInteger() && prev_dp.isInteger()) {
              // NOTE: Calculates in the long type to avoid precision loss
              // while converting long values to double values if both values are long.
              difference = maxValue - prev_prev_data.longValue() +
            		  prev_dp.longValue();
            } else {
              difference = maxValue - prev_prev_data.toDouble() +
            		  prev_dp.toDouble();
            }
            if(resetRate != 0 && difference*1000/(t1 - t0) > resetRate){
            	difference = 0;
//            	if (prev_data.isInteger() && next_dp.isInteger()) {
//                    // NOTE: Calculates in the long type to avoid precision loss
//                    // while converting long values to double values if both values are long.
//                    difference = next_dp.longValue();
//                  } else {
//                    difference = next_dp.toDouble();
//                  }
            }
        }
        return difference;
      }
      throw new NoSuchElementException("no more values in interval of "
          + timestamp_start);
    }

    @Override
    public String toString() {
      final StringBuilder buf = new StringBuilder();
      buf.append("ValuesInInterval: ")
         .append(", timestamp_start_interval=").append(timestamp_start)
         .append(", has_next_value_from_source=")
         .append(has_next_value_from_source);
      if (has_next_value_from_source) {
        buf.append(", nextValue=(").append(next_dp).append(')');
      }
      buf.append(", source=").append(source);
      return buf.toString();
    }

	@Override
	public void setIntervalInMs(long interval) {
	}
  }
  
}
