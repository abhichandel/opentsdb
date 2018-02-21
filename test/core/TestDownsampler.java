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


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.util.List;
import java.util.TimeZone;

import com.google.common.collect.Lists;

import net.opentsdb.utils.DateTime;

import org.junit.Before;
import org.junit.Test;


/** Tests {@link Downsampler}. */
public class TestDownsampler {

  private static final long BASE_TIME = 1356998400000L;
  private static final DataPoint[] DATA_POINTS = new DataPoint[] {
    // timestamp = 1,356,998,400,000 ms
    MutableDataPoint.ofLongValue(BASE_TIME, 40),
    // timestamp = 1,357,000,400,000 ms
    MutableDataPoint.ofLongValue(BASE_TIME + 2000000, 50),
    // timestamp = 1,357,002,000,000 ms
    MutableDataPoint.ofLongValue(BASE_TIME + 3600000, 40),
    // timestamp = 1,357,002,005,000 ms
    MutableDataPoint.ofLongValue(BASE_TIME + 3605000, 50),
    // timestamp = 1,357,005,600,000 ms
    MutableDataPoint.ofLongValue(BASE_TIME + 7200000, 40),
    // timestamp = 1,357,007,600,000 ms
    MutableDataPoint.ofLongValue(BASE_TIME + 9200000, 50)
  };
  private static final int THOUSAND_SEC_INTERVAL =
      (int)DateTime.parseDuration("1000s");
  private static final int TEN_SEC_INTERVAL =
      (int)DateTime.parseDuration("10s");
  private static final Aggregator AVG = Aggregators.get("avg");
  private static final Aggregator SUM = Aggregators.get("sum");
  private static final Aggregator FIRST = Aggregators.get("first");
  private static final Aggregator LAST = Aggregators.get("last");
  private static final Aggregator AVGABS = Aggregators.get("avgabs");

  private SeekableView source;
  private Downsampler downsampler;

  @Before
  public void before() {
    source = spy(SeekableViewsForTest.fromArray(DATA_POINTS));
  }

  @Test
  public void testDownsampler() {
    downsampler = new Downsampler(source, THOUSAND_SEC_INTERVAL, AVG);
    verify(source, never()).next();
    List<Double> values = Lists.newArrayList();
    List<Long> timestamps_in_millis = Lists.newArrayList();
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      values.add(dp.doubleValue());
      timestamps_in_millis.add(dp.timestamp());
    }

    assertEquals(5, values.size());
    assertEquals(40, values.get(0), 0.0000001);
    assertEquals(BASE_TIME - 400000L, timestamps_in_millis.get(0).longValue());
    assertEquals(50, values.get(1), 0.0000001);
    assertEquals(BASE_TIME + 1600000, timestamps_in_millis.get(1).longValue());
    assertEquals(45, values.get(2), 0.0000001);
    assertEquals(BASE_TIME + 3600000L, timestamps_in_millis.get(2).longValue());
    assertEquals(40, values.get(3), 0.0000001);
    assertEquals(BASE_TIME + 6600000L, timestamps_in_millis.get(3).longValue());
    assertEquals(50, values.get(4), 0.0000001);
    assertEquals(BASE_TIME + 8600000L, timestamps_in_millis.get(4).longValue());
  }

  @Test
  public void testDownsampler_10seconds() {
    source = spy(SeekableViewsForTest.fromArray(new DataPoint[] {
        MutableDataPoint.ofDoubleValue(BASE_TIME + 5000L * 0, 1),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 5000L * 1, 2),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 5000L * 2, 4),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 5000L * 3, 8),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 5000L * 4, 16),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 5000L * 5, 32),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 5000L * 6, 64),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 5000L * 7, 128),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 5000L * 8, 256),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 5000L * 9, 512),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 5000L * 10, 1024)
    }));
    downsampler = new Downsampler(source, 10000, SUM);
    verify(source, never()).next();
    List<Double> values = Lists.newArrayList();
    List<Long> timestamps_in_millis = Lists.newArrayList();
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      values.add(dp.doubleValue());
      timestamps_in_millis.add(dp.timestamp());
    }

    assertEquals(6, values.size());
    assertEquals(3, values.get(0), 0.0000001);
    assertEquals(BASE_TIME + 00000L, timestamps_in_millis.get(0).longValue());
    assertEquals(12, values.get(1), 0.0000001);
    assertEquals(BASE_TIME + 10000L, timestamps_in_millis.get(1).longValue());
    assertEquals(48, values.get(2), 0.0000001);
    assertEquals(BASE_TIME + 20000L, timestamps_in_millis.get(2).longValue());
    assertEquals(192, values.get(3), 0.0000001);
    assertEquals(BASE_TIME + 30000L, timestamps_in_millis.get(3).longValue());
    assertEquals(768, values.get(4), 0.0000001);
    assertEquals(BASE_TIME + 40000L, timestamps_in_millis.get(4).longValue());
    assertEquals(1024, values.get(5), 0.0000001);
    assertEquals(BASE_TIME + 50000L, timestamps_in_millis.get(5).longValue());
  }
  
  @Test
  public void testDownsampler_10seconds_avgabs_1() {
    source = spy(SeekableViewsForTest.fromArray(new DataPoint[] {
        MutableDataPoint.ofDoubleValue(BASE_TIME + 5000L * 0, 1),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 5000L * 1, -2),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 5000L * 2, 4),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 5000L * 3, -8),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 5000L * 4, 16),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 5000L * 5, -32),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 5000L * 6, 64),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 5000L * 7, -128),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 5000L * 8, 256),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 5000L * 9, -512),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 5000L * 10, 1024)
    }));
    downsampler = new Downsampler(source, 10000, AVGABS);
    verify(source, never()).next();
    List<Double> values = Lists.newArrayList();
    List<Long> timestamps_in_millis = Lists.newArrayList();
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      values.add(dp.doubleValue());
      timestamps_in_millis.add(dp.timestamp());
    }

    assertEquals(6, values.size());
    assertEquals(1.5, values.get(0), 0.0000001);
    assertEquals(BASE_TIME + 00000L, timestamps_in_millis.get(0).longValue());
    assertEquals(6, values.get(1), 0.0000001);
    assertEquals(BASE_TIME + 10000L, timestamps_in_millis.get(1).longValue());
    assertEquals(24, values.get(2), 0.0000001);
    assertEquals(BASE_TIME + 20000L, timestamps_in_millis.get(2).longValue());
    assertEquals(96, values.get(3), 0.0000001);
    assertEquals(BASE_TIME + 30000L, timestamps_in_millis.get(3).longValue());
    assertEquals(384, values.get(4), 0.0000001);
    assertEquals(BASE_TIME + 40000L, timestamps_in_millis.get(4).longValue());
    assertEquals(1024, values.get(5), 0.0000001);
    assertEquals(BASE_TIME + 50000L, timestamps_in_millis.get(5).longValue());
  }
  
  @Test
  public void testDownsampler_month() {
    source = spy(SeekableViewsForTest.fromArray(new DataPoint[] {
        MutableDataPoint.ofDoubleValue(BASE_TIME + 30*60*1000L * 0, 100), //5:30
        MutableDataPoint.ofDoubleValue(BASE_TIME + 30*60*1000L * 1, 110), //6:00
        MutableDataPoint.ofDoubleValue(BASE_TIME + 30*60*1000L * 2, 120), //6:30
        MutableDataPoint.ofDoubleValue(BASE_TIME + 30*60*1000L * 3, 130), //7:00
        MutableDataPoint.ofDoubleValue(BASE_TIME + 30*60*1000L * 4, 140), //7:30
        MutableDataPoint.ofDoubleValue(BASE_TIME + 30*60*1000L * 5, 150), //8:00
        MutableDataPoint.ofDoubleValue(BASE_TIME + 30*60*1000L * 6, 160), //8:30
        MutableDataPoint.ofDoubleValue(BASE_TIME + 30*60*1000L * 7, 170), //9:00
        MutableDataPoint.ofDoubleValue(BASE_TIME + 30*60*1000L * 8, 180), //9:30
        MutableDataPoint.ofDoubleValue(BASE_TIME + 30*60*1000L * 9, 190), //10:00
        MutableDataPoint.ofDoubleValue(BASE_TIME + 30*60*1000L * 10, 200), //10:30
        MutableDataPoint.ofDoubleValue(BASE_TIME + 30*60*1000L * 11, 210), //11:00
        MutableDataPoint.ofDoubleValue(BASE_TIME + 30*60*1000L * 12, 220), //11:30
        MutableDataPoint.ofDoubleValue(BASE_TIME + 30*60*1000L * 13, 230) //12:00
    }));
    downsampler = new Downsampler(source, 60*60*1000, SUM, "n", TimeZone.getTimeZone("IST"), new RateOptions(false, RateOptions.MONOTONIC_INCREMEMNT_COUNTER, 32767,1.2));
    verify(source, never()).next();
    List<Double> values = Lists.newArrayList();
    List<Long> timestamps_in_millis = Lists.newArrayList();
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      values.add(dp.doubleValue());
      timestamps_in_millis.add(dp.timestamp());
    }

    
    assertEquals(2, values.size());
    assertEquals(910, values.get(0), 0.0000001);
    assertEquals(1400, values.get(1), 0.0000001);
    assertEquals(1356985800000l, timestamps_in_millis.get(0).longValue());
    assertEquals(1357011000000l, timestamps_in_millis.get(1).longValue());
//    assertEquals(1357000200000l, timestamps_in_millis.get(1).longValue());
//    assertEquals(1357003800000l, timestamps_in_millis.get(2).longValue());
  }

  @Test
  public void testDownsampler_shifts() {
    source = spy(SeekableViewsForTest.fromArray(new DataPoint[] {
        MutableDataPoint.ofDoubleValue(BASE_TIME + 30*60*1000L * 0, 100), //5:30
        MutableDataPoint.ofDoubleValue(BASE_TIME + 30*60*1000L * 1, 110), //6:00
        MutableDataPoint.ofDoubleValue(BASE_TIME + 30*60*1000L * 2, 120), //6:30
        MutableDataPoint.ofDoubleValue(BASE_TIME + 30*60*1000L * 3, 130), //7:00
        MutableDataPoint.ofDoubleValue(BASE_TIME + 30*60*1000L * 4, 140), //7:30
        MutableDataPoint.ofDoubleValue(BASE_TIME + 30*60*1000L * 5, 150), //8:00
        MutableDataPoint.ofDoubleValue(BASE_TIME + 30*60*1000L * 6, 160), //8:30
        MutableDataPoint.ofDoubleValue(BASE_TIME + 30*60*1000L * 7, 170), //9:00
        MutableDataPoint.ofDoubleValue(BASE_TIME + 30*60*1000L * 8, 180), //9:30
        MutableDataPoint.ofDoubleValue(BASE_TIME + 30*60*1000L * 9, 190), //10:00
        MutableDataPoint.ofDoubleValue(BASE_TIME + 30*60*1000L * 10, 200), //10:30
        MutableDataPoint.ofDoubleValue(BASE_TIME + 30*60*1000L * 11, 210), //11:00
        MutableDataPoint.ofDoubleValue(BASE_TIME + 30*60*1000L * 12, 220), //11:30
        MutableDataPoint.ofDoubleValue(BASE_TIME + 30*60*1000L * 13, 230) //12:00
    }));
    downsampler = new Downsampler(source, 60*60*1000, SUM, "s[02:00-09:00,09:00-02:00]", TimeZone.getTimeZone("IST"), new RateOptions(false, RateOptions.MONOTONIC_INCREMEMNT_COUNTER, 32767,1.2));
    verify(source, never()).next();
    List<Double> values = Lists.newArrayList();
    List<Long> timestamps_in_millis = Lists.newArrayList();
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      values.add(dp.doubleValue());
      timestamps_in_millis.add(dp.timestamp());
    }

    
    assertEquals(2, values.size());
    assertEquals(910, values.get(0), 0.0000001);
    assertEquals(1400, values.get(1), 0.0000001);
    assertEquals(1356985800000l, timestamps_in_millis.get(0).longValue());
    assertEquals(1357011000000l, timestamps_in_millis.get(1).longValue());
//    assertEquals(1357000200000l, timestamps_in_millis.get(1).longValue());
//    assertEquals(1357003800000l, timestamps_in_millis.get(2).longValue());
  }
  
  @Test
  public void testDownsampler_counter() {
    source = spy(SeekableViewsForTest.fromArray(new DataPoint[] {
        MutableDataPoint.ofDoubleValue(BASE_TIME + 30*60*1000L * 0, 100), //5:30
        MutableDataPoint.ofDoubleValue(BASE_TIME + 30*60*1000L * 1, 110), //6:00
        MutableDataPoint.ofDoubleValue(BASE_TIME + 30*60*1000L * 2, 120), //6:30
        MutableDataPoint.ofDoubleValue(BASE_TIME + 30*60*1000L * 3, 130), //7:00
        MutableDataPoint.ofDoubleValue(BASE_TIME + 30*60*1000L * 4, 140), //7:30
//        MutableDataPoint.ofDoubleValue(BASE_TIME + 30*60*1000L * 5, 150), //8:00
//        MutableDataPoint.ofDoubleValue(BASE_TIME + 30*60*1000L * 6, 160), //8:30
//        MutableDataPoint.ofDoubleValue(BASE_TIME + 30*60*1000L * 7, 170), //9:00
//        MutableDataPoint.ofDoubleValue(BASE_TIME + 30*60*1000L * 8, 180), //9:30
//        MutableDataPoint.ofDoubleValue(BASE_TIME + 30*60*1000L * 9, 190), //10:00
//        MutableDataPoint.ofDoubleValue(BASE_TIME + 30*60*1000L * 10, 200), //10:30
//        MutableDataPoint.ofDoubleValue(BASE_TIME + 30*60*1000L * 11, 210), //11:00
//        MutableDataPoint.ofDoubleValue(BASE_TIME + 30*60*1000L * 12, 220), //11:30
//        MutableDataPoint.ofDoubleValue(BASE_TIME + 30*60*1000L * 13, 230) //12:00
    }));
    downsampler = new Downsampler(source, 60*60*1000, SUM, "h", TimeZone.getTimeZone("IST"), new RateOptions(true, RateOptions.MONOTONIC_INCREMEMNT_COUNTER, 999999,5));
    verify(source, never()).next();
    List<Double> values = Lists.newArrayList();
    List<Long> timestamps_in_millis = Lists.newArrayList();
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      values.add(dp.doubleValue());
      timestamps_in_millis.add(dp.timestamp());
    }

    
    assertEquals(3, values.size());
    assertEquals(10, values.get(0), 0.0000001);
    assertEquals(20, values.get(1), 0.0000001);
    assertEquals(10, values.get(2), 0.0000001);
    assertEquals(1356996600000l, timestamps_in_millis.get(0).longValue());
    assertEquals(1357000200000l, timestamps_in_millis.get(1).longValue());
    assertEquals(1357003800000l, timestamps_in_millis.get(2).longValue());
  }
  
  @Test
  public void testDownsampler_counter_reset() {
    source = spy(SeekableViewsForTest.fromArray(new DataPoint[] {
//    		MutableDataPoint.ofDoubleValue(BASE_TIME + 30*60*1000L * -1, 9000), //5:30
        MutableDataPoint.ofDoubleValue(BASE_TIME + 30*60*1000L * 0, 0), //5:30
        MutableDataPoint.ofDoubleValue(BASE_TIME + 30*60*1000L * 1, 15000), //6:00
        MutableDataPoint.ofDoubleValue(BASE_TIME + 30*60*1000L * 2, 0), //6:30
        MutableDataPoint.ofDoubleValue(BASE_TIME + 30*60*1000L * 3, 15000), //7:00
        MutableDataPoint.ofDoubleValue(BASE_TIME + 30*60*1000L * 4, 0), //7:30
//        MutableDataPoint.ofDoubleValue(BASE_TIME + 30*60*1000L * 5, 150), //8:00
//        MutableDataPoint.ofDoubleValue(BASE_TIME + 30*60*1000L * 6, 160), //8:30
//        MutableDataPoint.ofDoubleValue(BASE_TIME + 30*60*1000L * 7, 170), //9:00
//        MutableDataPoint.ofDoubleValue(BASE_TIME + 30*60*1000L * 8, 180), //9:30
//        MutableDataPoint.ofDoubleValue(BASE_TIME + 30*60*1000L * 9, 190), //10:00
//        MutableDataPoint.ofDoubleValue(BASE_TIME + 30*60*1000L * 10, 200), //10:30
//        MutableDataPoint.ofDoubleValue(BASE_TIME + 30*60*1000L * 11, 210), //11:00
//        MutableDataPoint.ofDoubleValue(BASE_TIME + 30*60*1000L * 12, 220), //11:30
//        MutableDataPoint.ofDoubleValue(BASE_TIME + 30*60*1000L * 13, 230) //12:00
    }));
    downsampler = new Downsampler(source, 60*60*1000, SUM, "h", TimeZone.getTimeZone("IST"), new RateOptions(true, RateOptions.MONOTONIC_INCREMEMNT_COUNTER, 999999,5));
    verify(source, never()).next();
    List<Double> values = Lists.newArrayList();
    List<Long> timestamps_in_millis = Lists.newArrayList();
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      values.add(dp.doubleValue());
      timestamps_in_millis.add(dp.timestamp());
    }

    System.out.println(values);
    System.out.println(timestamps_in_millis);
    assertEquals(3, values.size());
    assertEquals(0, values.get(0), 0.0000001);
    assertEquals(0, values.get(1), 0.0000001);
    assertEquals(0, values.get(2), 0.0000001);
    assertEquals(1356996600000l, timestamps_in_millis.get(0).longValue());
    assertEquals(1357000200000l, timestamps_in_millis.get(1).longValue());
    assertEquals(1357003800000l, timestamps_in_millis.get(2).longValue());
  }
  
  @Test
  public void testDownsampler_StartStopRunning() {
    source = spy(SeekableViewsForTest.fromArray(new DataPoint[] {
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 0, 0),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 1, 10),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 2, 10),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 3, 10),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 4, 10),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 5, 10),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 6, 10),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 7, 10),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 8, 10),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 9, 10),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 10, 10)
    }));
    downsampler = new Downsampler(source, 10000, SUM, "o", TimeZone.getTimeZone("IST"), new RateOptions(false, RateOptions.MONOTONIC_INCREMEMNT_COUNTER, 32767,1.2));
    verify(source, never()).next();
    List<Double> values = Lists.newArrayList();
    List<Long> timestamps_in_millis = Lists.newArrayList();
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      values.add(dp.doubleValue());
      timestamps_in_millis.add(dp.timestamp());
    }

    
    assertEquals(1, values.size());
    assertEquals(100, values.get(0), 0.0000001);
    assertEquals(BASE_TIME, timestamps_in_millis.get(0).longValue());
  }
  
  @Test
  public void testDownsampler_FirstHourWise() {
    source = spy(SeekableViewsForTest.fromArray(new DataPoint[] {
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 0, 100), //5:30
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 1, 110), //5:40
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 2, 120), //5:50
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 3, 130), //6:00
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 4, 140), //6:10
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 5, 150), //6:20
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 6, 160), //6:30
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 7, 170), //6:40
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 8, 180), //6:50
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 9, 190), //7:00
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 10, 200), //7:10
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 11, 210), //7:20
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 12, 220), //7:30
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 13, 230) //7:40
    }));
    downsampler = new Downsampler(source, 60*60*1000, FIRST, "h", TimeZone.getTimeZone("IST"), new RateOptions(false, RateOptions.MONOTONIC_INCREMEMNT_COUNTER, 32767,1.2));
    verify(source, never()).next();
    List<Double> values = Lists.newArrayList();
    List<Long> timestamps_in_millis = Lists.newArrayList();
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      values.add(dp.doubleValue());
      timestamps_in_millis.add(dp.timestamp());
    }

    
    assertEquals(3, values.size());
    assertEquals(100, values.get(0), 0.0000001);
    assertEquals(130, values.get(1), 0.0000001);
    assertEquals(190, values.get(2), 0.0000001);
    assertEquals(1356996600000l, timestamps_in_millis.get(0).longValue());
    assertEquals(1357000200000l, timestamps_in_millis.get(1).longValue());
    assertEquals(1357003800000l, timestamps_in_millis.get(2).longValue());
  }
  
  
  @Test
  public void testDownsampler_LastHourWise() {
    source = spy(SeekableViewsForTest.fromArray(new DataPoint[] {
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 0, 100), //5:30
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 1, 110), //5:40
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 2, 120), //5:50
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 3, 130), //6:00
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 4, 140), //6:10
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 5, 150), //6:20
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 6, 160), //6:30
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 7, 170), //6:40
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 8, 180), //6:50
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 9, 190), //7:00
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 10, 200), //7:10
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 11, 210), //7:20
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 12, 220), //7:30
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 13, 230) //7:40
    }));
    downsampler = new Downsampler(source, 60*60*1000, LAST, "h", TimeZone.getTimeZone("IST"), new RateOptions(false, RateOptions.MONOTONIC_INCREMEMNT_COUNTER, 32767,1.2));
    verify(source, never()).next();
    List<Double> values = Lists.newArrayList();
    List<Long> timestamps_in_millis = Lists.newArrayList();
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      values.add(dp.doubleValue());
      timestamps_in_millis.add(dp.timestamp());
    }

    
    assertEquals(3, values.size());
    assertEquals(120, values.get(0), 0.0000001);
    assertEquals(180, values.get(1), 0.0000001);
    assertEquals(230, values.get(2), 0.0000001);
    assertEquals(1356996600000l, timestamps_in_millis.get(0).longValue());
    assertEquals(1357000200000l, timestamps_in_millis.get(1).longValue());
    assertEquals(1357003800000l, timestamps_in_millis.get(2).longValue());
  }
  
  @Test
  public void testDownsampler_StartStop() {
    source = spy(SeekableViewsForTest.fromArray(new DataPoint[] {
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 0, 0),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 1, 10),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 2, 10),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 3, 10),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 4, 10),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 5, 10),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 6, 10),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 7, 10),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 8, 10),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 9, 0),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 10, 0)
    }));
    downsampler = new Downsampler(source, 10000, SUM, "o", TimeZone.getTimeZone("IST"), new RateOptions(false, RateOptions.MONOTONIC_INCREMEMNT_COUNTER, 32767,1.2));
    verify(source, never()).next();
    List<Double> values = Lists.newArrayList();
    List<Long> timestamps_in_millis = Lists.newArrayList();
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      values.add(dp.doubleValue());
      timestamps_in_millis.add(dp.timestamp());
    }

    
    assertEquals(1, values.size());
    assertEquals(80, values.get(0), 0.0000001);
    assertEquals(BASE_TIME, timestamps_in_millis.get(0).longValue());
  }
  
  /**
 * Multiple start stop of a value. zero value is a stop.
 */
@Test
  public void testDownsampler_MultipleStartStop() {
    source = spy(SeekableViewsForTest.fromArray(new DataPoint[] {
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 0, 0),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 1, 0),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 2, 10),        
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 3, 10),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 4, 10),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 5, 10),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 6, 10),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 7, 10),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 8, 10),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 9, 10),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 10, 10),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 11, 0),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 12, 0),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 13, 0),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 14, 0),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 15, 10),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 16, 10),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 17, 10),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 18, 10),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 19, 0),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 20, 0)
        
    }));
    downsampler = new Downsampler(source, 10000, SUM, "o", TimeZone.getTimeZone("IST"), new RateOptions(false, RateOptions.MONOTONIC_INCREMEMNT_COUNTER, 32767,1.2));
    verify(source, never()).next();
    List<Double> values = Lists.newArrayList();
    List<Long> timestamps_in_millis = Lists.newArrayList();
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      values.add(dp.doubleValue());
      timestamps_in_millis.add(dp.timestamp());
    }

    
    assertEquals(2, values.size());
    assertEquals(90, values.get(0), 0.0000001);
    assertEquals(40, values.get(1), 0.0000001);
    assertEquals(BASE_TIME + 10*60*1000L * 1, timestamps_in_millis.get(0).longValue());
    assertEquals(BASE_TIME + 10*60*1000L * 14, timestamps_in_millis.get(1).longValue());
  }
  
  @Test
  public void testDownsampler_CounterStartStopRunning() {
    source = spy(SeekableViewsForTest.fromArray(new DataPoint[] {
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 0, 268),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 1, 377),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 2, 712),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 3, 997),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 4, 1298),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 5, 1681),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 6, 1968),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 7, 2268),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 8, 2568),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 9, 2868),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 10, 3168)
    }));
    downsampler = new Downsampler(source, 10000, SUM, "o", TimeZone.getTimeZone("IST"), new RateOptions(true, RateOptions.MONOTONIC_INCREMEMNT_COUNTER, 32767,1.2));
    verify(source, never()).next();
    List<Double> values = Lists.newArrayList();
    List<Long> timestamps_in_millis = Lists.newArrayList();
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      values.add(dp.doubleValue());
      timestamps_in_millis.add(dp.timestamp());
    }

    
    assertEquals(1, values.size());
    assertEquals(2900, values.get(0), 0.0000001);
    assertEquals(BASE_TIME, timestamps_in_millis.get(0).longValue());
  }
  
  @Test
  public void testDownsampler_CounterStartStop() {
    source = spy(SeekableViewsForTest.fromArray(new DataPoint[] {
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 0, 268),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 1, 377),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 2, 712),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 3, 997),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 4, 1298),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 5, 1681),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 6, 1968),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 7, 2268),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 8, 2568),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 9, 2868),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 10, 2868)
    }));
    downsampler = new Downsampler(source, 10000, SUM, "o", TimeZone.getTimeZone("IST"), new RateOptions(true, RateOptions.MONOTONIC_INCREMEMNT_COUNTER, 32767,1.2));
    verify(source, never()).next();
    List<Double> values = Lists.newArrayList();
    List<Long> timestamps_in_millis = Lists.newArrayList();
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      values.add(dp.doubleValue());
      timestamps_in_millis.add(dp.timestamp());
    }

    
    assertEquals(1, values.size());
    assertEquals(2600, values.get(0), 0.0000001);
    assertEquals(BASE_TIME, timestamps_in_millis.get(0).longValue());
  }
  
  @Test
  public void testDownsampler_CounterMultipleStartStop() {
    source = spy(SeekableViewsForTest.fromArray(new DataPoint[] {
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 0, 268),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 1, 268),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 2, 268),        
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 3, 377),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 4, 712),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 5, 997),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 6, 1298),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 7, 1681),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 8, 1968),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 9, 2268),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 10, 2568),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 11, 2868),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 12, 2868),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 13, 2868),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 14, 2868),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 15, 3168),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 16, 3558),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 17, 3963),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 18, 4249),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 19, 4550),
        MutableDataPoint.ofDoubleValue(BASE_TIME + 10*60*1000L * 20, 4550)
        
    }));
    downsampler = new Downsampler(source, 10000, SUM, "o", TimeZone.getTimeZone("IST"), new RateOptions(true, RateOptions.MONOTONIC_INCREMEMNT_COUNTER, 32767,1.2));
    verify(source, never()).next();
    List<Double> values = Lists.newArrayList();
    List<Long> timestamps_in_millis = Lists.newArrayList();
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      values.add(dp.doubleValue());
      timestamps_in_millis.add(dp.timestamp());
    }

    
    assertEquals(2, values.size());
    assertEquals(2600, values.get(0), 0.0000001);
    assertEquals(1682, values.get(1), 0.0000001);
    assertEquals(BASE_TIME + 10*60*1000L * 2, timestamps_in_millis.get(0).longValue());
    assertEquals(BASE_TIME + 10*60*1000L * 14, timestamps_in_millis.get(1).longValue());
  }
  
  @Test
  public void testDownsampler_15seconds() {
    source = spy(SeekableViewsForTest.fromArray(new DataPoint[] {
        MutableDataPoint.ofLongValue(BASE_TIME + 5000L, 1),
        MutableDataPoint.ofLongValue(BASE_TIME + 15000L, 2),
        MutableDataPoint.ofLongValue(BASE_TIME + 25000L, 4),
        MutableDataPoint.ofLongValue(BASE_TIME + 35000L, 8),
        MutableDataPoint.ofLongValue(BASE_TIME + 45000L, 16),
        MutableDataPoint.ofLongValue(BASE_TIME + 55000L, 32)
    }));
    downsampler = new Downsampler(source, 15000, SUM);
    verify(source, never()).next();
    List<Double> values = Lists.newArrayList();
    List<Long> timestamps_in_millis = Lists.newArrayList();
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      values.add(dp.doubleValue());
      timestamps_in_millis.add(dp.timestamp());
    }

    assertEquals(4, values.size());
    assertEquals(1, values.get(0), 0.0000001);
    assertEquals(BASE_TIME + 00000L, timestamps_in_millis.get(0).longValue());
    assertEquals(6, values.get(1), 0.0000001);
    assertEquals(BASE_TIME + 15000L, timestamps_in_millis.get(1).longValue());
    assertEquals(8, values.get(2), 0.0000001);
    assertEquals(BASE_TIME + 30000L, timestamps_in_millis.get(2).longValue());
    assertEquals(48, values.get(3), 0.0000001);
    assertEquals(BASE_TIME + 45000L, timestamps_in_millis.get(3).longValue());
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testRemove() {
    new Downsampler(source, THOUSAND_SEC_INTERVAL, AVG).remove();
  }

  @Test
  public void testSeek() {
    downsampler = new Downsampler(source, THOUSAND_SEC_INTERVAL, AVG);
    downsampler.seek(BASE_TIME + 3600000L);
    verify(source, never()).next();
    List<Double> values = Lists.newArrayList();
    List<Long> timestamps_in_millis = Lists.newArrayList();
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      values.add(dp.doubleValue());
      timestamps_in_millis.add(dp.timestamp());
    }

    assertEquals(3, values.size());
    assertEquals(45, values.get(0), 0.0000001);
    assertEquals(BASE_TIME + 3600000L, timestamps_in_millis.get(0).longValue());
    assertEquals(40, values.get(1), 0.0000001);
    assertEquals(BASE_TIME + 6600000L, timestamps_in_millis.get(1).longValue());
    assertEquals(50, values.get(2), 0.0000001);
    assertEquals(BASE_TIME + 8600000L, timestamps_in_millis.get(2).longValue());
  }

  @Test
  public void testSeek_skipPartialInterval() {
    downsampler = new Downsampler(source, THOUSAND_SEC_INTERVAL, AVG);
    downsampler.seek(BASE_TIME + 3800000L);
    verify(source, never()).next();
    List<Double> values = Lists.newArrayList();
    List<Long> timestamps_in_millis = Lists.newArrayList();
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      values.add(dp.doubleValue());
      timestamps_in_millis.add(dp.timestamp());
    }

    // seek timestamp was BASE_TIME + 3800000L or 1,357,002,200,000 ms.
    // The interval that has the timestamp began at 1,357,002,000,000 ms. It
    // had two data points but was abandoned because the requested timestamp
    // was not aligned. The next two intervals at 1,357,003,000,000 and
    // at 1,357,004,000,000 did not have data points. The first interval that
    // had a data point began at 1,357,002,005,000 ms or BASE_TIME + 6600000L.
    assertEquals(2, values.size());
    assertEquals(40, values.get(0), 0.0000001);
    assertEquals(BASE_TIME + 6600000L, timestamps_in_millis.get(0).longValue());
    assertEquals(50, values.get(1), 0.0000001);
    assertEquals(BASE_TIME + 8600000L, timestamps_in_millis.get(1).longValue());
  }

  @Test
  public void testSeek_doubleIteration() {
    downsampler = new Downsampler(source, THOUSAND_SEC_INTERVAL, AVG);
    while (downsampler.hasNext()) {
      downsampler.next();
    }
    downsampler.seek(BASE_TIME + 3600000L);
    List<Double> values = Lists.newArrayList();
    List<Long> timestamps_in_millis = Lists.newArrayList();
    while (downsampler.hasNext()) {
      DataPoint dp = downsampler.next();
      assertFalse(dp.isInteger());
      values.add(dp.doubleValue());
      timestamps_in_millis.add(dp.timestamp());
    }

    assertEquals(3, values.size());
    assertEquals(45, values.get(0), 0.0000001);
    assertEquals(BASE_TIME + 3600000L, timestamps_in_millis.get(0).longValue());
    assertEquals(40, values.get(1), 0.0000001);
    assertEquals(BASE_TIME + 6600000L, timestamps_in_millis.get(1).longValue());
    assertEquals(50, values.get(2), 0.0000001);
    assertEquals(BASE_TIME + 8600000L, timestamps_in_millis.get(2).longValue());
  }

  @Test
  public void testSeek_abandoningIncompleteInterval() {
    source = SeekableViewsForTest.fromArray(new DataPoint[] {
        MutableDataPoint.ofLongValue(BASE_TIME + 100L, 40),
        MutableDataPoint.ofLongValue(BASE_TIME + 1100L, 40),
        MutableDataPoint.ofLongValue(BASE_TIME + 2100L, 40),
        MutableDataPoint.ofLongValue(BASE_TIME + 3100L, 40),
        MutableDataPoint.ofLongValue(BASE_TIME + 4100L, 40),
        MutableDataPoint.ofLongValue(BASE_TIME + 5100L, 40),
        MutableDataPoint.ofLongValue(BASE_TIME + 6100L, 40),
        MutableDataPoint.ofLongValue(BASE_TIME + 7100L, 40),
        MutableDataPoint.ofLongValue(BASE_TIME + 8100L, 40),
        MutableDataPoint.ofLongValue(BASE_TIME + 9100L, 40),
        MutableDataPoint.ofLongValue(BASE_TIME + 10100L, 40)
      });
    downsampler = new Downsampler(source, TEN_SEC_INTERVAL, SUM);
    // The seek is aligned by the downsampling window.
    downsampler.seek(BASE_TIME);
    assertTrue("seek(BASE_TIME)", downsampler.hasNext());
    DataPoint first_dp = downsampler.next();
    assertEquals("seek(1356998400000)", BASE_TIME, first_dp.timestamp());
    assertEquals("seek(1356998400000)", 400, first_dp.doubleValue(), 0.0000001);
    // No seeks but the last one is aligned by the downsampling window.
    for (long seek_timestamp = BASE_TIME + 1000L;
         seek_timestamp < BASE_TIME + 10100L; seek_timestamp += 1000) {
      downsampler.seek(seek_timestamp);
      assertTrue("ts = " + seek_timestamp, downsampler.hasNext());
      DataPoint dp = downsampler.next();
      // Timestamp should be greater than or equal to the seek timestamp.
      assertTrue(String.format("%d >= %d", dp.timestamp(), seek_timestamp),
                 dp.timestamp() >= seek_timestamp);
      assertEquals(String.format("seek(%d)", seek_timestamp),
                   BASE_TIME + 10000L, dp.timestamp());
      assertEquals(String.format("seek(%d)", seek_timestamp),
                   40, dp.doubleValue(), 0.0000001);
    }
  }

  @Test
  public void testToString() {
    downsampler = new Downsampler(source, THOUSAND_SEC_INTERVAL, AVG);
    DataPoint dp = downsampler.next();
    System.out.println(downsampler.toString());
    assertTrue(downsampler.toString().contains(dp.toString()));
  }
}
