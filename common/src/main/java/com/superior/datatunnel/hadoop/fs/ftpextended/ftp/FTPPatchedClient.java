package com.superior.datatunnel.hadoop.fs.ftpextended.ftp;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import com.superior.datatunnel.hadoop.fs.ftpextended.common.ErrorStrings;

/**
 * FTPClient doesn't handle listing paths containing square brackets.
 */
public class FTPPatchedClient extends FTPClient {

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
                throw new FileNotFoundException(String.format(
                        ErrorStrings.E_SPATH_NOTEXIST, pathname));
            }
        } else {
            return super.listFiles(pathname);
        }
    }

}
