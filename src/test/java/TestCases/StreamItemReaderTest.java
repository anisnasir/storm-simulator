package TestCases;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;

import org.junit.Test;

import com.gihub.gdfm.shobaidogu.IOUtils;

import Input.StreamItem;
import Input.StreamItemReader;

public class StreamItemReaderTest {

    @Test
    public void testNextItem() throws IOException {
        BufferedReader testInput = IOUtils.getBufferedReader("/tweets.txt");
        StreamItemReader sir = new StreamItemReader(testInput);
        List<StreamItem<String>> item;
        int lines = 0, words = 0;
        while ((item = sir.nextItem()) != null) {
            lines++;
        }
        assertEquals(10, lines);
    }
}
