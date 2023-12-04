package com.superior.datatunnel.hadoop.fs.common;

/**
 * Error string constants.
 */
public final class ErrorStrings {

    public static final String E_CREATE_DIR = "create(): failed to create directory: %s";

    public static final String E_FAILED_GETHOME = "Failed to get home directory";

    public static final String E_DPATH_EXIST = "Destination path %s already exist, cannot rename!";

    public static final String E_MAKE_DIR_FORPATH = "Can't make directory for path \"%s\" under \"%s\".";

    public static final String E_REMOVE_FILE = "rm(): file failed to be deleted: %s";

    public static final String E_DIR_NOTEMPTY = "Directory: %s is not empty.";

    public static final String E_USER_NULL = "No user specified for ftp connection. Expand URI or credential file.";

    public static final String E_FAILED_DISCONNECT = "Failed to disconnect";

    public static final String E_RENAME = "rename(): file failed to be renamed: %s -> %s";

    public static final String E_CREATE_FILE = "create(): failed to create file: %s";

    public static final String E_FILE_EXIST = "File already exists: %s";

    public static final String E_REMOVE_DIR = "rmdir(): dir failed to be deleted: %s";

    public static final String E_FILE_STATUS = "Failed to get file status";

    public static final String E_DIR_CREATE_FROMFILE = "Can't make directory for path %s since it is a file.";

    public static final String E_PATH_DIR = "Path %s is a directory.";

    public static final String E_HOST_NULL = "Invalid host specified";

    public static final String E_SAME_DIRECTORY_ONLY = "only same directory renames are supported";

    public static final String E_NOT_SUPPORTED = "Not supported";

    public static final String E_FILE_CHECK_FAILED = "File check failed";

    public static final String E_SPATH_NOTEXIST = "Source path %s does not exist";

    public static final String E_FILE_NOTFOUND = "File %s does not exist.";

    public static final String E_NULL_INPUTSTREAM = "Null InputStream";

    public static final String E_CLIENT_NULL = "SFTP client null or not connected";

    public static final String E_STREAM_CLOSED = "Stream closed";

    public static final String E_SEEK_NOTSUPPORTED = "Seek not supported";

    public static final String E_CLIENT_NOTCONNECTED = "Client not connected";

    public static final String E_MODIFY_TIME = "Time of file %s can't be set";

    private ErrorStrings() {
    }

}
