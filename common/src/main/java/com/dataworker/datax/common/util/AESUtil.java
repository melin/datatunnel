package com.dataworker.datax.common.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.util.Base64;

public class AESUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(AESUtil.class);

    private static final String ENCRYPT_KEY = "dataworker123456"; //长度必须是16

    /**
     * 加密
     * @param value
     * @return
     */
    public static String encrypt(String value) {
        try {
            IvParameterSpec iv = new IvParameterSpec(ENCRYPT_KEY.getBytes("UTF-8"));
            SecretKeySpec skeySpec = new SecretKeySpec(ENCRYPT_KEY.getBytes("UTF-8"), "AES");

            Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5PADDING");
            cipher.init(Cipher.ENCRYPT_MODE, skeySpec, iv);

            byte[] encrypted = cipher.doFinal(value.getBytes());

            return Base64.getUrlEncoder().encodeToString(encrypted);
        } catch (Exception ex) {
            LOGGER.error("AES加密出错: " + ex.getMessage(), ex);
        }

        return null;
    }

    /**
     * 解密
     * @param encrypted
     * @return String
     */
    public static String decrypt(String encrypted) {
        try {
            IvParameterSpec iv = new IvParameterSpec(ENCRYPT_KEY.getBytes("UTF-8"));
            SecretKeySpec skeySpec = new SecretKeySpec(ENCRYPT_KEY.getBytes("UTF-8"), "AES");

            Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5PADDING");
            cipher.init(Cipher.DECRYPT_MODE, skeySpec, iv);
            byte[] original = cipher.doFinal(Base64.getUrlDecoder().decode(encrypted));

            return new String(original);
        } catch (Exception ex) {
            LOGGER.error("AES解密出错: " + ex.getMessage(), ex);
        }

        return null;
    }

    public static void main(String[] args) {
        String a = encrypt("dataworker2020");
        System.out.println(a);
        String b = decrypt(a);
        System.out.println(b);
    }
}
