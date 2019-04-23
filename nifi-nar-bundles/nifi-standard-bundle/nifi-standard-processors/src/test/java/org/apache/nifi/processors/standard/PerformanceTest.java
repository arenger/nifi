package org.apache.nifi.processors.standard;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import java.nio.file.Paths;

public class PerformanceTest {

    @Test
    public void test() throws Exception {
        { // invoke static loading and "prime the pumps" by loading a small file
            TestRunner testRunner = TestRunners.newTestRunner(new SplitLargeJson());
            testRunner.setProperty(SplitLargeJson.JSON_PATH_EXPRESSION, "$[0][0]");

            testRunner.enqueue(Paths.get("/tmp/little.json"));
            testRunner.run();

            testRunner.assertTransferCount(SplitLargeJson.REL_SPLIT, 1);
        }

        TestRunner testRunner = TestRunners.newTestRunner(new SplitLargeJson());
        testRunner.setProperty(SplitLargeJson.JSON_PATH_EXPRESSION, "$[55][1037]");
        testRunner.enqueue(Paths.get(System.getProperty("testFile")));

        long start = System.currentTimeMillis();
        testRunner.run();
        long stop  = System.currentTimeMillis();
        testRunner.getLogger().error(String.format("Process time: %.3f sec\n", (double)(stop - start)/1000));
    }
}

