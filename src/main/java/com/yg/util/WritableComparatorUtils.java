package com.yg.util;

import org.apache.hadoop.io.WritableComparator;

import java.io.UTFDataFormatException;

/**
 * Created by liuti on 2017/1/3.
 */
public class WritableComparatorUtils {
    public WritableComparatorUtils() {
    }

    public static String readUTF(byte[] bytes, int s) {
        try {
            int e = WritableComparator.readUnsignedShort(bytes, s);
            byte[] bytearr = new byte[e];
            char[] chararr = new char[e];
            int count = 0;
            int chararr_count = 0;
            System.arraycopy(bytes, s + 2, bytearr, 0, e);

            int c;
            while(count < e) {
                c = bytearr[count] & 255;
                if(c > 127) {
                    break;
                }

                ++count;
                chararr[chararr_count++] = (char)c;
            }

            while(count < e) {
                c = bytearr[count] & 255;
                byte char2;
                switch(c >> 4) {
                    case 0:
                    case 1:
                    case 2:
                    case 3:
                    case 4:
                    case 5:
                    case 6:
                    case 7:
                        ++count;
                        chararr[chararr_count++] = (char)c;
                        break;
                    case 8:
                    case 9:
                    case 10:
                    case 11:
                    default:
                        throw new UTFDataFormatException("malformed input around byte " + count);
                    case 12:
                    case 13:
                        count += 2;
                        if(count > e) {
                            throw new UTFDataFormatException("malformed input: partial character at end");
                        }

                        char2 = bytearr[count - 1];
                        if((char2 & 192) != 128) {
                            throw new UTFDataFormatException("malformed input around byte " + count);
                        }

                        chararr[chararr_count++] = (char)((c & 31) << 6 | char2 & 63);
                        break;
                    case 14:
                        count += 3;
                        if(count > e) {
                            throw new UTFDataFormatException("malformed input: partial character at end");
                        }

                        char2 = bytearr[count - 2];
                        byte char3 = bytearr[count - 1];
                        if((char2 & 192) != 128 || (char3 & 192) != 128) {
                            throw new UTFDataFormatException("malformed input around byte " + (count - 1));
                        }

                        chararr[chararr_count++] = (char)((c & 15) << 12 | (char2 & 63) << 6 | (char3 & 63) << 0);
                }
            }

            return new String(chararr, 0, chararr_count);
        } catch (Exception var10) {
            var10.printStackTrace();
            return null;
        }
    }
}
