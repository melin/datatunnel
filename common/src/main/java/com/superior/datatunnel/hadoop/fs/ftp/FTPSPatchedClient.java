package com.superior.datatunnel.hadoop.fs.ftp;

import com.superior.datatunnel.hadoop.fs.common.ErrorStrings;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPSClient;

/**
 * FTPSClient doesn't handle listing paths containing square brackets.
 * Hadoop uses commons.net version 3.1 which has incorrect implementation of
 * ftps client. This subclass fixes the issue - won't be necessary if we moved
 * to version 3.6 of commons.net
 */
public class FTPSPatchedClient extends FTPSClient {

    private final Pattern pattern = Pattern.compile("[\\[\\]]");

    @Override
    public FTPFile[] listFiles(String pathname) throws IOException {
        if (pathname == null) {
            return super.listFiles(null);
        }
        Matcher matcher = pattern.matcher(pathname);
        if (matcher.find()) {
            String wd = printWorkingDirectory();
            if (changeWorkingDirectory(pathname)) {
                FTPFile[] ftpFiles = super.listFiles(null);
                changeWorkingDirectory(wd);
                return ftpFiles;
            } else {
                throw new FileNotFoundException(String.format(ErrorStrings.E_SPATH_NOTEXIST, pathname));
            }
        } else {
            return super.listFiles(pathname);
        }
    }
}
