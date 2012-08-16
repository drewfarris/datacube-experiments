package drew.ufo.reader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.NoSuchElementException;

import au.com.bytecode.opencsv.CSVReader;

public class UFODataReader implements Iterable<UFODataEntry> {

	Reader in;

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
	
	public void close() throws IOException {
		in.close();
	}
	
	public Iterator<UFODataEntry> iterator() {
		if (in == null) throw new IllegalStateException("No Reader available for data");
		return new UFODataIterator(in);
	}

	public static class UFODataIterator implements Iterator<UFODataEntry> {
		private CSVReader in;
		private UFODataBuilder builder = new UFODataBuilder();
		protected UFODataEntry current;
		
		private final DateFormat ufoDataFormat = new SimpleDateFormat("yyyyMMdd");
		
		public UFODataIterator(Reader in) {
			this.in = new CSVReader(in, '\t');
		}
		
		@Override
		public boolean hasNext() {
			if (current == null) {
				getNext();
			}
			
			return (current != null);
		}
		
		protected void getNext() {
			try { 
				while (true) {
					try {
					String[] entry = in.readNext();
						if (entry.length != 6) {
							continue;
						}
						current = builder.withEventDate(ufoDataFormat.parse(entry[0]))
							.withReportDate(ufoDataFormat.parse(entry[1]))
							.withCity(parseCity(entry[2]))
							.withState(parseState(entry[2]))
							.withShape(entry[3])
							.withDuration(entry[4])
							.withDesciption(entry[5])
							.create();
						break;
					}
					catch (ParseException ex) {
						// do something.
					}
				}
			}
			catch (IOException ex) {
				// do something.
			}
		}
		
		protected String parseCity(String input) throws ParseException {
			return "New York";
		}
		
		protected String parseState(String input) throws ParseException {
			return "NY";
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
		
		public UFODataBuilder withEventDate(Date eventDate) {
			entry.setEventDate(eventDate);
			return this;
		}
		
		public UFODataBuilder withReportDate(Date reportDate) {
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
