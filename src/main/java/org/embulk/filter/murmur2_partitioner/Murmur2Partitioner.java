package org.embulk.filter.murmur2_partitioner;

import org.apache.kafka.common.utils.Utils;

import java.io.UnsupportedEncodingException;

public class Murmur2Partitioner {
    static int partition(String key, int partitionCount)
    {
        try {
            return partition(key.getBytes("UTF8"), partitionCount);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Cannot convert string to byte array");
        }
    }

    static int partition(Long key, int partitionCount)
    {
        byte[] bytes = new byte[]{(byte)((int)(key >>> 56)), (byte)((int)(key >>> 48)), (byte)((int)(key >>> 40)), (byte)((int)(key >>> 32)), (byte)((int)(key >>> 24)), (byte)((int)(key >>> 16)), (byte)((int)(key >>> 8)), key.byteValue()};
        return partition(bytes, partitionCount);
    }

    static int partition(byte[] bytes, int partitionCount)
    {
        return Utils.toPositive(Utils.murmur2(bytes)) % partitionCount;
    }
}
