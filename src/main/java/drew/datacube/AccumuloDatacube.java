package drew.datacube;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.urbanairship.datacube.BoxedByteArray;
import com.urbanairship.datacube.DataCube;
import com.urbanairship.datacube.DataCubeIo;
import com.urbanairship.datacube.DbHarness;
import com.urbanairship.datacube.DbHarness.CommitType;
import com.urbanairship.datacube.Dimension;
import com.urbanairship.datacube.IdService;
import com.urbanairship.datacube.ReadBuilder;
import com.urbanairship.datacube.Rollup;
import com.urbanairship.datacube.SyncLevel;
import com.urbanairship.datacube.WriteBuilder;
import com.urbanairship.datacube.bucketers.HourDayMonthBucketer;
import com.urbanairship.datacube.bucketers.StringToBytesBucketer;
import com.urbanairship.datacube.dbharnesses.MapDbHarness;
import com.urbanairship.datacube.idservices.CachingIdService;
import com.urbanairship.datacube.idservices.HBaseIdService;
import com.urbanairship.datacube.idservices.MapIdService;
import com.urbanairship.datacube.ops.LongOp;

public class AccumuloDatacube {
	public static void main(String[] args) throws Exception {
		
		Configuration conf = HBaseConfiguration.create();
		
		IdService hbaseIdService = new HBaseIdService(conf, 
				"cubeLookup".getBytes(),
				"cubeCounter".getBytes(),
				"fam".getBytes(),
				"myCube".getBytes());
		
		IdService idService = new CachingIdService(5, hbaseIdService);
		ConcurrentMap<BoxedByteArray,byte[]> backingMap = 
		        new ConcurrentHashMap<BoxedByteArray, byte[]>();

		DbHarness<LongOp> dbHarness = new MapDbHarness<LongOp>(backingMap, LongOp.DESERIALIZER, 
		        CommitType.READ_COMBINE_CAS, idService);

		HourDayMonthBucketer hourDayMonthBucketer = new HourDayMonthBucketer();

		Dimension<DateTime> time = new Dimension<DateTime>("time", hourDayMonthBucketer, false, 8);
		Dimension<String> zipcode = new Dimension<String>("zipcode", new StringToBytesBucketer(), 
		        true, 5);

		DataCubeIo<LongOp> cubeIo = null;
		DataCube<LongOp> cube;

		Rollup hourAndZipRollup = new Rollup(zipcode, time, HourDayMonthBucketer.hours);
		Rollup dayAndZipRollup = new Rollup(zipcode, time, HourDayMonthBucketer.days);
		Rollup hourRollup = new Rollup(time, HourDayMonthBucketer.hours);
		Rollup dayRollup = new Rollup(time, HourDayMonthBucketer.days);

		List<Dimension<?>> dimensions =  ImmutableList.<Dimension<?>>of(time, zipcode);
		List<Rollup> rollups = ImmutableList.of(hourAndZipRollup, dayAndZipRollup, hourRollup,
		        dayRollup);

		cube = new DataCube<LongOp>(dimensions, rollups);

		cubeIo = new DataCubeIo<LongOp>(cube, dbHarness, 1, Long.MAX_VALUE, SyncLevel.FULL_SYNC);

		DateTime now = new DateTime(DateTimeZone.UTC);

		// Do an increment of 5 for a certain time and zipcode
		cubeIo.writeSync(new LongOp(5), new WriteBuilder(cube)
		        .at(time, now)
		        .at(zipcode, "97201"));

		// Do an increment of 10 for the same zipcode in a different hour of the same day
		DateTime differentHour = now.withHourOfDay((now.getHourOfDay()+1)%24);
		cubeIo.writeSync(new LongOp(10), new WriteBuilder(cube)
		        .at(time, differentHour)
		        .at(zipcode, "97201"));

		// Read back the value that we wrote for the current hour, should be 5 
		Optional<LongOp> thisHourCount = cubeIo.get(new ReadBuilder(cube)
		         .at(time, HourDayMonthBucketer.hours, now)
		        .at(zipcode, "97201"));
		Assert.assertTrue(thisHourCount.isPresent());
		Assert.assertEquals(5L, thisHourCount.get().getLong());

		// Read back the value we wrote for the other hour, should be 10
		Optional<LongOp> differentHourCount = cubeIo.get(new ReadBuilder(cube)
		        .at(time, HourDayMonthBucketer.hours, differentHour)
		        .at(zipcode, "97201"));
		Assert.assertTrue(differentHourCount.isPresent());
		Assert.assertEquals(10L, differentHourCount.get().getLong());

		// The total for today should be the sum of the two increments
		Optional<LongOp> todayCount = cubeIo.get(new ReadBuilder(cube)
		        .at(time, HourDayMonthBucketer.days, now)
		        .at(zipcode, "97201"));
		Assert.assertTrue(todayCount.isPresent());
		Assert.assertEquals(15L, todayCount.get().getLong());
	}
}
