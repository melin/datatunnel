package com.superior.datatunnel.api;

import org.slf4j.helpers.MessageFormatter;

public class DataTunnelException extends RuntimeException {

    private String message;

    public DataTunnelException(String message) {
        super(message);
        this.message = message;
    }

    public DataTunnelException(String message, Throwable cause) {
        super(message, cause);
        this.message = message;
    }

    public DataTunnelException(String format, Object... params) {
        super();
        if (params.length > 0) {
            this.message = MessageFormatter.arrayFormat(format, params).getMessage();
        }
    }

    public DataTunnelException(Throwable cause, String format, Object... params) {
        super(cause);
        if (params.length > 0) {
            this.message = MessageFormatter.arrayFormat(format, params).getMessage();
        }
    }

    @Override
    public String getMessage() {
        return this.message;
    }
}
