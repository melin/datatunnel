package com.dataworker.datax.common.exception;

/**
 * @author melin 2021/7/29 4:32 下午
 */
public class DataXException extends RuntimeException {

    public DataXException(String message) {
        super(message);
    }

    public DataXException(String message, Throwable cause) {
        super(message, cause);
    }
}
