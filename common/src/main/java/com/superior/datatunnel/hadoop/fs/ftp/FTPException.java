package com.superior.datatunnel.hadoop.fs.ftp;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * A class to wrap a {@link Throwable} into a Runtime Exception.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
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
