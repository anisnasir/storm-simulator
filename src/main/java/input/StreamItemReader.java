package input;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 * Reads a stream of StreamItems from a file.
 */
public class StreamItemReader {
	private BufferedReader in;
	//private ZipfDistribution zipf;
	public StreamItemReader(String inFileName) throws IOException {
		BufferedReader in = null;
		try {
			InputStream rawin = new FileInputStream(inFileName);
			if (inFileName.endsWith(".gz"))
				rawin = new GZIPInputStream(rawin);
			this.in = new BufferedReader(new InputStreamReader(rawin));
		} catch (FileNotFoundException e) {
			System.err.println("File not found");
			e.printStackTrace();
			System.exit(1);
		}
	}
	public StreamItemReader(BufferedReader in) throws IOException {
		this.in = in;
	}
	public List<StreamItem<String>> nextItem() throws IOException {
		String line = null;
		try {
			line = in.readLine();
		} catch (IOException e) {
			System.err.println("Unable to read from file");
			throw e;
		}

		if (line == null || line.length() == 0)
			return null;

		String[] tokens = line.split("\t");
		if (tokens.length < 2)
			return null;

		long timestamp = Long.parseLong(tokens[0]);
		String sentence = tokens[1];
		
		//System.out.println(value);
		String []words = sentence.split(" ");
		List<StreamItem<String>> results = new ArrayList<StreamItem<String>>();
		for(String word: words) {
			//long value = 1+getProcessingTime(word);
			results.add(new StreamItem<String>(timestamp, word, 1));
		}
		
		return results;
	}
	public void close() throws IOException {
		in.close();
	}
}
