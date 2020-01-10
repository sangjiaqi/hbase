package org.apache.hadoop.hbase.master.cleaner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.CoordinatedStateManagerFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.snapshot.SnapshotLogCleaner;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertFalse;

@Category(SmallTests.class)
public class TestSnapshotLogCleanerBug {

    private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        TEST_UTIL.startMiniZKCluster();
    }

    @AfterClass
    public static void cleanup() throws IOException {
        Configuration conf = TEST_UTIL.getConfiguration();
        Path rootDir = FSUtils.getRootDir(conf);
        FileSystem fs = FileSystem.get(conf);
        // cleanup
        fs.delete(rootDir, true);
        System.out.println("Finish");
    }

    @Test
    public void testSnapshotLogCleaning() throws IOException, KeeperException, InterruptedException {
        Server server = new TestLogsCleaner.DummyServer();
        Configuration conf = TEST_UTIL.getConfiguration();
        final FileSystem fs = FileSystem.get(conf);
        FSUtils.setRootDir(conf, TEST_UTIL.getDataTestDir());
        Path rootDir = FSUtils.getRootDir(conf);
        final Path oldLogDir = new Path(TEST_UTIL.getDataTestDir(),
                HConstants.HREGION_OLDLOGDIR_NAME);
        CoordinatedStateManager cp = CoordinatedStateManagerFactory.getCoordinatedStateManager(
                TEST_UTIL.getConfiguration());
        HMaster master = new HMaster(TEST_UTIL.getConfiguration(), cp);
        Map<String, Object> param = new HashMap<String, Object>();
        param.put("master", master);

        LogCleaner logCleaner = new LogCleaner(1000, server, conf, fs, oldLogDir, param);

        SnapshotLogCleaner cleaner = new SnapshotLogCleaner();
        cleaner.setConf(conf);
        cleaner.init(logCleaner.params);

        // write an hfile to the snapshot directory
        String snapshotName = "snapshot";
        byte[] snapshot = Bytes.toBytes(snapshotName);
        Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotName, rootDir);
        Path snapshotLogDir = new Path(snapshotDir, HConstants.HREGION_LOGDIR_NAME);
        String timestamp = "1339643343027";
        String hostFromMaster = "localhost%2C59648%2C1339643336601";

        Path hostSnapshotLogDir = new Path(snapshotLogDir, hostFromMaster);
        String snapshotlogfile = hostFromMaster + "." + timestamp + ".hbase";

        // add the reference to log in the snapshot
        fs.create(new Path(hostSnapshotLogDir, snapshotlogfile));

        // now check to see if that log file would get deleted.
        Path oldlogDir = new Path(rootDir, HConstants.HREGION_OLDLOGDIR_NAME);
        Path logFile = new Path(oldlogDir, snapshotlogfile);
        fs.create(logFile);

        // make sure that the file isn't deletable
        assertFalse(cleaner.isFileDeletable(fs.getFileStatus(logFile)));
    }

}