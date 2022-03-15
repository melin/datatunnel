package com.dataworks.datatunnel.api;

/**
 * @author melin 2021/7/29 4:32 下午
 */
public class DataTunnelException extends RuntimeException {

    public DataTunnelException(String message) {
        super(message);
    }

    public DataTunnelException(String message, Throwable cause) {
        super(message, cause);
    }
}
