package com.jay.mykafka.cluster;

/**
 * jie.zhou
 * 2018/10/26 10:47
 */
public class Partition implements Comparable<Partition> {
    private int brokerId;
    private int partitionId;

    public Partition(int brokerId, int partitionId) {
        this.brokerId = brokerId;
        this.partitionId = partitionId;
    }

    public String name() {
        return brokerId + "-" + partitionId;
    }

    public Partition parse(String s) {
        String[] pieces = s.split("-");
        if (pieces.length != 2) {
            throw new IllegalArgumentException("Expected name in the form x-y");
        }

        return new Partition(Integer.parseInt(pieces[0]), Integer.parseInt(pieces[1]));
    }

    @Override
    public int compareTo(Partition that) {
        if (this.brokerId == that.brokerId) {
            return this.partitionId - that.partitionId;
        }

        return this.brokerId - that.brokerId;
    }
}
