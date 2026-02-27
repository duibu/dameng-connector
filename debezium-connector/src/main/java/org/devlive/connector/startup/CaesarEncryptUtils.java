package org.devlive.connector.startup;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * 凯撒变换加密工具类
 * 用于数据库数据加密
 */
public class CaesarEncryptUtils {

    private static final int BASE_SHIFT = 16;
    private static final String SECRET = "Zl+57537759@!@#$";

    // 凯撒+Salt加密（可逆，用于LIKE）
    public static String encryptCaesarSalt(String plain, String fieldName) {
        if (StringUtils.isBlank(plain)) return null;
        int salt = saltOffset(fieldName);
        StringBuilder sb = new StringBuilder();
        for (char c : plain.toCharArray()) {
            sb.append((char) (c + BASE_SHIFT));
        }
        return sb.toString();
    }

    public static String decryptCaesarSalt(String cipher, String fieldName) {
        if (StringUtils.isBlank(cipher)) return null;
        int salt = saltOffset(fieldName);
        StringBuilder sb = new StringBuilder();
        for (char c : cipher.toCharArray()) {
            sb.append((char) (c - BASE_SHIFT));
        }
        return sb.toString();
    }

    // SHA-256 Hash（用于EQ精确查询）
    public static String hashSha256(String plain) {
        if (StringUtils.isBlank(plain)) return null;
        return DigestUtils.sha256Hex(plain);
    }

    private static int saltOffset(String fieldName) {
        // 生成 MD5 字节数组
        byte[] md5Bytes = DigestUtils.md5(fieldName + SECRET);
        // 取第一个字节并取绝对值
        return Math.abs(md5Bytes[0]) % 10;
    }
}
