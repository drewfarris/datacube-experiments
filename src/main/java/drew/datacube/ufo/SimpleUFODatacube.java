package drew.datacube.ufo;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import junit.framework.Assert;

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
import com.urbanairship.datacube.idservices.MapIdService;
import com.urbanairship.datacube.ops.LongOp;

public class SimpleUFODatacube {
	public static void main(String[] args) throws Exception {
		IdService idService = new CachingIdService(5, new MapIdService());
		ConcurrentMap<BoxedByteArray,byte[]> backingMap = 
		        new ConcurrentHashMap<BoxedByteArray, byte[]>();

		DbHarness<LongOp> dbHarness = new MapDbHarness<LongOp>(backingMap, LongOp.DESERIALIZER, 
		        CommitType.READ_COMBINE_CAS, idService);

		HourDayMonthBucketer hourDayMonthBucketer = new HourDayMonthBucketer();

		Dimension<DateTime> eventDate = new Dimension<DateTime>("eventDate", hourDayMonthBucketer, false, 8);
		Dimension<DateTime> reportDate = new Dimension<DateTime>("reportDate", hourDayMonthBucketer, false, 8);
		Dimension<String> city = new Dimension<String>("city", new StringToBytesBucketer(), true, 5);
		Dimension<String> state = new Dimension<String>("state", new StringToBytesBucketer(), true, 5);
		Dimension<String> shape = new Dimension<String>("shape", new StringToBytesBucketer(), true, 5);
		
		DataCubeIo<LongOp> cubeIo = null;
		DataCube<LongOp> cube;

		Rollup eventMonthAndStateRollup = new Rollup(state, eventDate, HourDayMonthBucketer.months);
		Rollup eventDayAndStateRollup = new Rollup(state, eventDate, HourDayMonthBucketer.months);
		Rollup eventDayRollup = new Rollup(eventDate, HourDayMonthBucketer.days);
		Rollup eventMonthRollup = new Rollup(eventDate, HourDayMonthBucketer.months);
		
		List<Dimension<?>> dimensions =  ImmutableList.<Dimension<?>>of(eventDate, reportDate, city, state, shape);
		List<Rollup> rollups = ImmutableList.of(eventMonthAndStateRollup, eventDayAndStateRollup, eventDayRollup, eventMonthRollup);

		cube = new DataCube<LongOp>(dimensions, rollups);

		cubeIo = new DataCubeIo<LongOp>(cube, dbHarness, 1, Long.MAX_VALUE, SyncLevel.FULL_SYNC);

		// Do an increment of 5 for a certain time and zipcode
		cubeIo.writeSync(new LongOp(5), new WriteBuilder(cube)
		        .at(eventDate, now)
		        .at(reportDate, now)
		        .at(location, location));


		// Read back the value that we wrote for the current hour, should be 5 
		Optional<LongOp> thisHourCount = cubeIo.get(new ReadBuilder(cube)
		         .at(time, HourDayMonthBucketer.hours, now)
		        .at(zipcode, "97201"));
		Assert.assertTrue(thisHourCount.isPresent());
		Assert.assertEquals(5L, thisHourCount.get().getLong());
	}
}
