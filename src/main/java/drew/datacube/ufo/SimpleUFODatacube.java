package drew.datacube.ufo;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.joda.time.DateTime;

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

import drew.ufo.reader.UFODataEntry;
import drew.ufo.reader.UFODataReader;

public class SimpleUFODatacube {
	
	DataCubeIo<LongOp> cubeIo = null;
	DataCube<LongOp> cube;
	
	Dimension<DateTime> eventDate;
	Dimension<DateTime> reportDate;
	Dimension<String> city;
	Dimension<String> state;
	Dimension<String> shape;
 	
	public SimpleUFODatacube() {
		init();
	}
	
	protected void init() {
		IdService idService = new CachingIdService(5, new MapIdService());
		ConcurrentMap<BoxedByteArray,byte[]> backingMap = 
		        new ConcurrentHashMap<BoxedByteArray, byte[]>();

		DbHarness<LongOp> dbHarness = new MapDbHarness<LongOp>(backingMap, LongOp.DESERIALIZER, 
		        CommitType.READ_COMBINE_CAS, idService);

		HourDayMonthBucketer hourDayMonthBucketer = new HourDayMonthBucketer();

		eventDate = new Dimension<DateTime>("eventDate", hourDayMonthBucketer, false, 8);
		reportDate = new Dimension<DateTime>("reportDate", hourDayMonthBucketer, false, 8);
		city = new Dimension<String>("city", StringToBytesBucketer.getInstance(), true, 10);
		state = new Dimension<String>("state", StringToBytesBucketer.getInstance(), true, 6);
		shape = new Dimension<String>("shape", StringToBytesBucketer.getInstance(), true, 5);
		
		
		Rollup eventMonthAndStateRollup = new Rollup(state, eventDate, HourDayMonthBucketer.months);
		Rollup eventDayAndStateRollup = new Rollup(state, eventDate, HourDayMonthBucketer.days);
		Rollup eventDayRollup = new Rollup(eventDate, HourDayMonthBucketer.days);
		Rollup eventMonthRollup = new Rollup(eventDate, HourDayMonthBucketer.months);
		
		List<Dimension<?>> dimensions =  ImmutableList.<Dimension<?>>of(eventDate, reportDate, city, state, shape);
		List<Rollup> rollups = ImmutableList.of(eventMonthAndStateRollup, eventDayAndStateRollup, eventDayRollup, eventMonthRollup);

		cube = new DataCube<LongOp>(dimensions, rollups);
		cubeIo = new DataCubeIo<LongOp>(cube, dbHarness, 1, Long.MAX_VALUE, SyncLevel.FULL_SYNC);
	}
	
	public void addUFOEntry(UFODataEntry entry) throws IOException, InterruptedException {
		cubeIo.writeSync(new LongOp(1), new WriteBuilder(cube)
		        .at(eventDate, entry.getEventDate())
		        .at(reportDate, entry.getReportDate())
		        .at(city, entry.getCity())
		        .at(state, entry.getState())
		        .at(shape, entry.getShape()));
	}
	
	public long getEventMonthCount(DateTime time) throws IOException, InterruptedException {
		Optional<LongOp> thisMonthCount = cubeIo.get(new ReadBuilder(cube)
			.at(eventDate, HourDayMonthBucketer.months, time));
		if (thisMonthCount.isPresent()) {
			return thisMonthCount.get().getLong();
		}
		else {
			return 0;
		}	
	}

	public long getEventStateMonthCount(String stateValue, DateTime time) throws IOException, InterruptedException {
		Optional<LongOp> thisMonthCount = cubeIo.get(new ReadBuilder(cube)
			.at(eventDate, HourDayMonthBucketer.months, time)
			.at(state, stateValue));
		if (thisMonthCount.isPresent()) {
			return thisMonthCount.get().getLong();
		}
		else {
			return 0;
		}	
	}
	public static void main(String[] args) throws Exception {
		String input = "/home/drew/projects/ml-for-hackers/ML_for_Hackers/01-Introduction/data/ufo/ufo_awesome.tsv";
		SimpleUFODatacube ufoCube = new SimpleUFODatacube();
		UFODataReader reader = new UFODataReader();
		reader.open(input);
		reader.setLimit(100);
		for (UFODataEntry entry: reader) {
			ufoCube.addUFOEntry(entry);
		}
		
		for (int i=1; i<=12;i++) {
			DateTime time = new DateTime(1995,i,1,1,0,0,0);
			long count = ufoCube.getEventMonthCount(time);
			System.err.println("US " + time.getMonthOfYear() + "/" + time.getYear() + "\t" + count);
		}
		
		System.err.println("");
		
		for (int i=1; i<=12;i++) {
			DateTime time = new DateTime(1995,i,1,1,0,0,0);
			long count = ufoCube.getEventStateMonthCount("NJ",time);
			System.err.println("NJ " + time.getMonthOfYear() + "/" + time.getYear() + "\t" + count);
		}
	}
}
