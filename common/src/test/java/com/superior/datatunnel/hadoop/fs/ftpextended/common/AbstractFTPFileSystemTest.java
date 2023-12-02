package com.superior.datatunnel.hadoop.fs.ftpextended.common;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertDeleted;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertIsDirectory;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertIsFile;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertPathDoesNotExist;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertPathExists;
import static org.apache.hadoop.fs.contract.ContractTestUtils.touch;
import static org.apache.hadoop.fs.contract.ContractTestUtils.verifyFileContents;
import static org.apache.hadoop.fs.contract.ContractTestUtils.writeTextFile;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.rules.Timeout;

/**
 * Set of tests whose execution is common for all tested file systems.
 */
public abstract class AbstractFTPFileSystemTest {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractFTPFileSystemTest.class);

    protected static final String TEST_ROOT_DIR = GenericTestUtils.getRandomizedTempPath();

    public static final String TEST_DIR = "testsftp";

    public static final String TEST_JCEKS = "keystore.jceks";

    @Rule
    public TestName name = new TestName();

    @Rule
    public Timeout testTimeout = new Timeout(60, TimeUnit.SECONDS);

    private final Path localDir = new Path(TEST_DIR);

    private static FileSystem localFs = null;

    private AbstractFTPFileSystem ftpFs = null;

    @SuppressWarnings("checkstyle:visibilitymodifier")
    @Parameterized.Parameter(0)
    public boolean cache;

    @Parameterized.Parameters(name = "Cached dir tree = {0}")
    public static Collection<Boolean[]> data() {
        Boolean[][] data = {{true}, {false}};
        return Arrays.asList(data);
    }

    @BeforeClass
    public static void setUp() throws IOException {
        // Set up log4j configuration if none provided
        if (!LogManager.getRootLogger().getAllAppenders().hasMoreElements()) {
            org.apache.log4j.BasicConfigurator.configure();
            LogManager.getRootLogger().setLevel(Level.ERROR);
        }
        Path root = new Path(TEST_ROOT_DIR);
        localFs = FileSystem.getLocal(new Configuration());
        localFs.mkdirs(root);
        localFs.setWorkingDirectory(root);
        localFs.setWriteChecksum(false);
        localFs.enableSymlinks();
    }

    @AfterClass
    public static void cleanFS() throws IOException {
        if (localFs != null) {
            Path root = new Path(TEST_ROOT_DIR);
            if (localFs.exists(root)) {
                localFs.delete(root, true);
            }
        }
    }

    @Before
    public void setup() throws IOException {
        Thread.currentThread().setName(name.getMethodName());
        if (localFs.exists(localDir)) {
            localFs.delete(localDir, true);
        }
        localFs.mkdirs(localDir);
    }

    @After
    public void teardown() {
        if (ftpFs != null) {
            try {
                ftpFs.close();
            } catch (IOException e) {
                // ignore
            }
        }
        ftpFs = null;
        if (localFs != null) {
            try {
                localFs.delete(localDir, true);
            } catch (IOException e) {
                // ignore
            }
        }
    }

    /**
     * Creates a file and deletes it.
     *
     * @throws Exception
     */
    @Test
    public void testCreateFile() throws Exception {
        LOG.info("Testing:" + name.getMethodName().toLowerCase());

        Path file = new Path(name.getMethodName().toLowerCase());
        touch(ftpFs, file);
        assertPathExists(localFs, "Create file", file);
    }

    /**
     * Try to create a file which already exists without possibility to overwrite.
     *
     * @throws Exception
     */
    @Test(expected = java.io.IOException.class)
    public void testCreateExistingFileNoOverWrite() throws Exception {
        LOG.info("Testing:" + name.getMethodName().toLowerCase());
        touch(localFs, new Path(name.getMethodName().toLowerCase()));
        writeTextFile(ftpFs, new Path(name.getMethodName().toLowerCase()), null,
                false);
    }

    /**
     * Try to create a file which already exists.
     *
     * @throws Exception
     */
    @Test
    public void testCreateExistingFileOverWrite() throws Exception {
        LOG.info("Testing:" + name.getMethodName().toLowerCase());
        touch(localFs, new Path(name.getMethodName().toLowerCase()));
        Path f = new Path(name.getMethodName().toLowerCase());
        byte[] data = writeTextFile(ftpFs, f, "data", true);
        verifyFileContents(localFs, f, data);
    }

    /**
     * Try to create a file which already exists. Test will throw an exception
     * because directory is not possible to create
     *
     * @throws Exception
     */
    @Test(expected = java.io.IOException.class)
    public void testCreateFileNotPossible() throws Exception {
        LOG.info("Testing:" + name.getMethodName().toLowerCase());
        touch(ftpFs, new Path("dir\000withnull/" +
                name.getMethodName().toLowerCase()));
    }

    /**
     * Check making absolute path.
     *
     * @throws IOException
     */
    @Test
    public void testMakeAbsolute() throws IOException {
        try (Channel channel = ftpFs.connect()) {
            Path absolute = ftpFs.makeAbsolute(channel, new Path("relative"));
            assertEquals(new Path("/relative"), absolute);

            // try again - but input is absolute
            absolute = ftpFs.makeAbsolute(channel, new Path("/relative"));
            assertEquals(new Path("/relative"), absolute);
        }
    }

    /**
     * Check file and dir existence.
     *
     * @throws IOException
     */
    @Test
    public void testIsFile() throws IOException {
        Path file = new Path(name.getMethodName().toLowerCase());
        touch(localFs, file);
        Path dir = new Path(name.getMethodName().toLowerCase() + "_DIR");
        ftpFs.mkdirs(dir);
        assertIsFile(ftpFs, file);
        assertIsDirectory(ftpFs, dir);
    }

    /**
     * Checks if a new created file exists.
     *
     * @throws Exception
     */
    @Test
    public void testFileExists() throws Exception {
        Path file = new Path(name.getMethodName().toLowerCase());
        touch(localFs, file);
        assertPathExists(ftpFs, "Create file", file);
        assertPathExists(localFs, "Create file", file);
        assertDeleted(ftpFs, file, false);
        assertPathDoesNotExist(ftpFs, "Delete file", file);
        assertPathDoesNotExist(localFs, "Delete file", file);
    }

    /**
     * Test writing to a file and reading its value.
     *
     * @throws Exception
     */
    @Test
    public void testReadFile() throws Exception {
        Path file = new Path(name.getMethodName().toLowerCase());
        byte[] data = writeTextFile(localFs, file, "yaks", true);
        verifyFileContents(ftpFs, file, data);
        assertDeleted(ftpFs, file, false);
    }

    /**
     * Test writing to a file and reading its value byte by byte.
     *
     * @throws Exception
     */
    @Test
    public void testReadFileByByte() throws Exception {
        Path file = new Path(name.getMethodName().toLowerCase());
        byte[] data = writeTextFile(localFs, file, "yaks", true);
        try (FSDataInputStream is = ftpFs.open(file)) {
            byte[] b = new byte[data.length];
            int c;
            int i = 0;
            while ((c = is.read()) != -1) {
                b[i++] = (byte) c;
            }
            assertArrayEquals(data, b);
        }
        assertDeleted(ftpFs, file, false);
    }

    /**
     * Try open directory. Expecting exception
     *
     * @throws java.io.IOException
     */
    @Test(expected = IOException.class)
    public void testOpenDirectory() throws IOException {
        ftpFs.open(localDir);
    }

    @Test
    public void testOpenLink() throws IOException, URISyntaxException {
        Path file = new Path(localDir, name.getMethodName().toLowerCase());
        byte[] data = writeTextFile(localFs, file, "yaks", true);
        Path link = new Path(localDir, "link");
        Path link2 = new Path(localDir, "link2");
        localFs.createSymlink(new Path(name.getMethodName().toLowerCase()),
                link, false);
        localFs.createSymlink(new Path("link"), link2, false);

        try (FSDataInputStream is = ftpFs.open(link)) {
            byte[] b = new byte[data.length];
            is.read(b);
            assertArrayEquals(data, b);
        }

        try (FSDataInputStream is = ftpFs.open(link2)) {
            byte[] b = new byte[data.length];
            is.read(b);
            assertArrayEquals(data, b);
        }
    }

    /**
     * Test getting the status of a file.
     *
     * @throws Exception
     */
    @Test
    public void testStatFile() throws Exception {
        Path file = new Path(name.getMethodName().toLowerCase());
        byte[] data = writeTextFile(localFs, file, "yaks", true);

        FileStatus lstat = localFs.getFileStatus(file);
        FileStatus sstat = ftpFs.getFileStatus(file);
        assertNotNull(sstat);
        String localPath = lstat.getPath().toUri().getPath();
        String serverPath = sstat.getPath().toUri().getPath();
        assertEquals(localPath.substring(localPath.length() - serverPath.length()),
                serverPath);
        assertEquals(data.length, sstat.getLen());
        assertEquals(lstat.getLen(), sstat.getLen());
        assertDeleted(ftpFs, file, false);
    }

    @Test
    public void testGetRoot() throws IOException {
        FileStatus stat = ftpFs.getFileStatus(new Path("/"));
        assertNotNull(stat);
        assertEquals(ftpFs.makeQualified(new Path("/")), stat.getPath());
    }

    /**
     * Test deleting a non empty directory.
     *
     * @throws Exception
     */
    @Test(expected = java.io.IOException.class)
    public void testDeleteNonEmptyDir() throws Exception {
        Path file = new Path(localDir, name.getMethodName().toLowerCase());
        touch(localFs, file);
        assertDeleted(ftpFs, localDir, false);
    }

    /**
     * Test deleting a file that does not exist.
     *
     * @throws Exception
     */
    @Test
    public void testDeleteNonExistFile() throws Exception {
        Path file = new Path(localDir, name.getMethodName().toLowerCase());
        assertFalse("File shouldn't be possible to delete",
                ftpFs.delete(file, false));
    }

    @Test
    public void testDeleteDirRecursively() throws Exception {
        touch(localFs, new Path("test.a1"));
        touch(localFs, new Path("test.a2"));
        touch(localFs, new Path("testdir/test.a3"));
        assertDeleted(ftpFs, localDir, true);
    }

    @Test
    public void testDeleteFile() throws Exception {
        Path f = new Path("test.a1");
        touch(localFs, f);
        assertDeleted(ftpFs, f, true);
    }

    /**
     * Test renaming a file.
     *
     * @throws Exception
     */
    @Test
    public void testRenameFile() throws Exception {
        byte[] data = "dingos".getBytes();
        Path file1 = new Path(name.getMethodName().toLowerCase() + "1");
        touch(localFs, file1);
        Path file2 = new Path(name.getMethodName().toLowerCase() + "2");

        assertTrue("Rename failed", ftpFs.rename(file1, file2));

        assertPathExists(ftpFs, "Rename test", file2);
        assertPathDoesNotExist(ftpFs, "Rename test", file1);

        assertPathExists(localFs, "Rename test", file2);
        assertPathDoesNotExist(localFs, "Rename test", file1);

        assertDeleted(ftpFs, file2, false);
    }

    /**
     * Test renaming a file that does not exist.
     *
     * @throws Exception
     */
    @Test(expected = java.io.IOException.class)
    public void testRenameNonExistFile() throws Exception {
        Path file1 = new Path(localDir, name.getMethodName().toLowerCase() + "1");
        Path file2 = new Path(localDir, name.getMethodName().toLowerCase() + "2");
        ftpFs.rename(file1, file2);
    }

    /**
     * Test renaming a file onto an existing file.
     *
     * @throws Exception
     */
    @Test(expected = java.io.IOException.class)
    public void testRenamingFileOntoExistingFile() throws Exception {
        Path file1 = new Path(name.getMethodName().toLowerCase() + "1");
        touch(localFs, file1);
        Path file2 = new Path(name.getMethodName().toLowerCase() + "2");
        touch(localFs, file2);
        ftpFs.rename(file1, file2);
    }

    /**
     * Test get working directory.
     *
     * @throws Exception
     */
    @Test
    public void testGetWorkingDirectory() throws Exception {
        Path p = ftpFs.getWorkingDirectory();
        assertEquals(new Path("/"), p);
    }

    /**
     * Test creating directory.
     *
     * @throws Exception
     */
    @Test
    public void testMkdirs() throws Exception {
        Path p = new Path(localDir, name.getMethodName().toLowerCase());
        ftpFs.mkdirs(p, FsPermission.getDefault());
        assertPathExists(localFs, "Mk directory", p);
        assertIsDirectory(ftpFs, p);
    }

    /**
     * Test creating directory with name of an existing file.
     *
     * @throws Exception
     */
    @Test(expected = java.io.IOException.class)
    public void testMkdirsToExistingFile() throws Exception {
        Path file1 = new Path(name.getMethodName().toLowerCase());
        touch(localFs, file1);
        assertFalse(
                "Shouldn't be able to create directory with the " +
                        "same name as existing file",
                ftpFs.mkdirs(file1, FsPermission.getDefault()));
    }

    @Test
    public void testGlobUnix() throws Exception {
        touch(localFs, new Path(localDir, "test.a1"));
        touch(localFs, new Path(localDir, "test.a2"));
        touch(localFs, new Path(localDir, "testdir/test.a3"));
        FileStatus[] fs = ftpFs.globStatus(new Path(localDir, "test.a*"));
        assertEquals(2, fs.length);
        assertTrue("Not all files found", Arrays.asList(Stream.of(fs).map(item ->
                        item.getPath().getName()).toArray(String[]::new))
                .containsAll(Arrays.asList("test.a1", "test.a2")));

        fs = ftpFs.globStatus(new Path(localDir, "*/test.a*"));
        assertEquals(1, fs.length);
        assertEquals("test.a3", fs[0].getPath().getName());

        // Try whole directory structure
        fs = ftpFs.globStatus(localDir);
        assertEquals(1, fs.length);
        assertEquals(localDir.getName(), fs[0].getPath().getName());
    }

    @Test
    public void testGlobRegexp() throws Exception {
        Configuration conf = ftpFs.getConf();
        URI uri = ftpFs.getUri();
        ftpFs.close();
        conf.setEnum(ConnectionInfo.getPropertyName(
                        ConnectionInfo.FSParameter.FS_FTP_GLOB_TYPE, ftpFs.getUri()),
                AbstractFTPFileSystem.GlobType.REGEXP);
        ftpFs = (AbstractFTPFileSystem) FileSystem.get(uri, conf);
        touch(ftpFs, new Path(localDir, "test.a1"));
        touch(ftpFs, new Path(localDir, "test.a2"));
        touch(ftpFs, new Path(localDir, "testdir/test.a3"));
        FileStatus[] fs = ftpFs.globStatus(new Path(localDir, "test\\.a.*"));
        assertEquals(2, fs.length);
        assertTrue("Not all files found", Arrays.asList(Stream.of(fs).map(item ->
                        item.getPath().getName()).toArray(String[]::new))
                .containsAll(Arrays.asList("test.a1", "test.a2")));

        fs = ftpFs.globStatus(new Path(localDir, ".*test\\.a.*"));
        assertEquals(3, fs.length);
        assertTrue("Not all files found", Arrays.asList(Stream.of(fs).map(item ->
                        item.getPath().getName()).toArray(String[]::new))
                .containsAll(Arrays.asList("test.a1", "test.a2", "test.a3")));

        fs = ftpFs.globStatus(new Path(localDir, ".*test\\.a[0-9]"));
        assertEquals(3, fs.length);
        assertTrue("Not all files found", Arrays.asList(Stream.of(fs).map(item ->
                        item.getPath().getName()).toArray(String[]::new))
                .containsAll(Arrays.asList("test.a1", "test.a2", "test.a3")));

        // Try whole directory structure
        fs = ftpFs.globStatus(localDir);
        assertEquals(1, fs.length);
        assertEquals(localDir.getName(), fs[0].getPath().getName());
    }

    @Test
    public void testGetScheme() {
        assertEquals(ftpFs.getUri().getScheme(), ftpFs.getScheme());
    }

    protected void setFS(URI uri, Configuration conf) throws IOException {
        ftpFs = (AbstractFTPFileSystem) FileSystem.get(uri, conf);
    }

    public static void setEnv() throws Exception {
        Map<String, String> env = System.getenv();
        Class<?> cl = env.getClass();
        Field field = cl.getDeclaredField("m");
        field.setAccessible(true);
        Map<String, String> writableEnv = (Map<String, String>) field.get(env);
        writableEnv.put("HADOOP_CREDSTORE_PASSWORD", "testtest");
    }
}
