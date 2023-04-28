package com.superior.datatunnel.plugin.ftp.fs;

public class FTPException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public FTPException(String message) {
        super(message);
    }

    public FTPException(Throwable t) {
        super(t);
    }

    public FTPException(String message, Throwable t) {
        super(message, t);
    }
}
