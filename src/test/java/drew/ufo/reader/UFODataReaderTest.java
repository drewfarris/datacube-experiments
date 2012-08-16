package drew.ufo.reader;

import org.junit.Test;

import drew.ufo.reader.UFODataEntry;
import drew.ufo.reader.UFODataReader;

public class UFODataReaderTest {

	@Test
	public void testIterator() throws Exception {
		UFODataReader reader = new UFODataReader("src/test/resources/ufo_test_data.tsv");
		int count = 0;
		
		for (UFODataEntry e: reader) {
			count++;
			System.err.println(e.toString());
		}
		
		reader.close();
		
	}

}
