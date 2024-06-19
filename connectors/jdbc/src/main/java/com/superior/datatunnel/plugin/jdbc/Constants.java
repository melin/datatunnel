package com.superior.datatunnel.plugin.jdbc;

import java.nio.ByteBuffer;

public class Constants {

    public static final String FIELD_DELIMITER = "\u0001";

    public static void handlePgIdentifierBytes(byte[] bytes, ByteBuffer buffer) {
        for (byte aByte : bytes) {
            switch (aByte) {
                case '\\':
                    buffer.put((byte) ('\\'));
                    buffer.put((byte) ('\\'));
                    break;
                case '\t':
                    buffer.put((byte) ('\\'));
                    buffer.put((byte) ('t'));
                    break;
                case '\r':
                    buffer.put((byte) ('\\'));
                    buffer.put((byte) ('r'));
                    break;
                case '\n':
                    buffer.put((byte) ('\\'));
                    buffer.put((byte) ('n'));
                    break;
                case (byte) 0:
                    break;
                default:
                    buffer.put(aByte);
            }
        }
    }
}
