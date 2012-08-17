package drew.ufo.reader;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import au.com.bytecode.opencsv.CSVReader;

public class UFODataReader implements Iterable<UFODataEntry> {

	private static final Logger log = Logger.getLogger(UFODataReader.class);
	
	Reader in;
	int limit = 0;
	
	public UFODataReader() { }
	
	public UFODataReader(String file) throws IOException {
		open(file);
	}
	
	public UFODataReader(File file) throws IOException {
		open(file);
	}

	public void open(String filename) throws IOException {
		this.open(new File(filename));
	}
	
	public void open(File file) throws IOException {
		if (in != null) in.close();
		in = new FileReader(file); 
	}
	
	public void setLimit(int limit) {
		this.limit = limit;
	}
	
	public void close() throws IOException {
		in.close();
	}
	
	public Iterator<UFODataEntry> iterator() {
		if (in == null) throw new IllegalStateException("No Reader available for data");
		return new UFODataIterator(in, limit);
	}

	public static class UFODataIterator implements Iterator<UFODataEntry> {
		private CSVReader in;
		private UFODataBuilder builder = new UFODataBuilder();
		protected UFODataEntry current;
		protected int limit;
		protected int count = 0;
		
		final DateFormat ufoDataFormat = new SimpleDateFormat("yyyyMMdd");
		
		public UFODataIterator(Reader in, int limit) {
			this.in = new CSVReader(in, '\t');
			this.limit = limit;
		}
		
		@Override
		public boolean hasNext() {
			if (current == null) {
				getNext();
			}
			
			return (current != null);
		}
		
		protected void getNext() {
			count++;
			if (limit > 0 && count > limit) {
				if (log.isDebugEnabled()) {
					log.debug("Hit limit @ line " + count);
				}
				current = null;
				return;
			}
			
			if (log.isDebugEnabled() && (count % 25 == 0)) {
				log.info("Processed " + count + " lines");
			}
			
			try { 
				while (true) {
					try {
					String[] entry = in.readNext();
						if (entry == null) {
							return;
						}
						if (entry.length != 6) {
							continue;
						}
						current = builder.withEventDate(parseDate(entry[0]))
							.withReportDate(parseDate(entry[1]))
							.withCity(parseCity(entry[2]))
							.withState(parseState(entry[2]))
							.withShape(entry[3].trim())
							.withDuration(entry[4].trim())
							.withDesciption(entry[5].trim())
							.create();
						break;
					}
					catch (ParseException ex) {
						log.warn("Parse exception @ line " + count + ": " + ex.getMessage());
					}
				}
			}
			catch (IOException ex) {
				log.error("IOException @ line " + count, ex);
			}
		}
		
		protected DateTime parseDate(String input) throws ParseException {
			return new DateTime(ufoDataFormat.parse(input).getTime());
		}
		
		protected String parseCity(String input) throws ParseException {
			input = input.trim();
			int pos = input.indexOf(',');
			if (pos > 1) {
				return input.substring(0,pos).trim();
			}
			throw new ParseException("Could not parse city from input string '" + input + "'",-1);
		}
		
		protected String parseState(String input) throws ParseException {
			input = input.trim();
			int pos = input.indexOf(',');
			if (pos > 1 && input.length() == pos + 4) {
				return input.substring(pos+2,pos+4).trim();
			}
			throw new ParseException("Could not parse state from input string '" + input + "'",-1);
		}
		
		@Override
		public UFODataEntry next() {
			if (current == null) {
				getNext();
			}
			
			if (current == null) {
				throw new NoSuchElementException();
			}
			
			try {
				return current;
			}
			finally {
				current = null;
			}
		}
		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}
	
	public static class UFODataBuilder {
		UFODataEntry entry;
		
		public UFODataBuilder() {
			this(new UFODataEntry());
		}
		
		public UFODataBuilder(UFODataEntry entry) {
			this.entry = entry;
		}
		
		public UFODataBuilder newEntry() {
			entry = new UFODataEntry();
			return this;
		}
		
		public UFODataBuilder withEventDate(DateTime eventDate) {
			entry.setEventDate(eventDate);
			return this;
		}
		
		public UFODataBuilder withReportDate(DateTime reportDate) {
			entry.setReportDate(reportDate);
			return this;
		}
		
		public UFODataBuilder withCity(String city) {
			entry.setCity(city);
			return this;
		}
		
		public UFODataBuilder withState(String state) {
			entry.setState(state);
			return this;
		}
		
		public UFODataBuilder withShape(String shape) {
			entry.setShape(shape);
			return this;
		}
		
		public UFODataBuilder withDuration(String duration) {
			entry.setDuration(duration);
			return this;
		}
		
		public UFODataBuilder withDesciption(String description) {
			entry.setDescription(description);
			return this;
		}
		
		public UFODataEntry create() {
			return entry;
		}
		
	}
}
