package com.bigdata.spark.core;

import scala.Serializable;
import scala.math.Ordered;

import java.util.Objects;

public class SecondarySortKey implements Ordered<SecondarySortKey> , Serializable {

    private static final long serialVersionUID = -2224007779484321011L;

    private String first;
    private int second;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SecondarySortKey that = (SecondarySortKey) o;
        return second == that.second &&
                Objects.equals(first, that.first);
    }

    @Override
    public int hashCode() {

        return Objects.hash(first, second);
    }

    public String getFirst() {

        return first;
    }

    public void setFirst(String first) {
        this.first = first;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
    }

    @Override
    public int compare(SecondarySortKey that) {
        if (first.compareTo(that.first) != 0) {
            return first.compareTo(that.first);
        }
        return second - that.second;

    }

    @Override
    public boolean $less(SecondarySortKey that) {
        if (first.compareTo(that.first) < 0) {
            return true;
        } else if (this.first.compareTo(that.first) == 0 && (this.second - that.second) < 0) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater(SecondarySortKey that) {
        if (first.compareTo(that.first) > 0) {
            return true;
        } else if (this.first.compareTo(that.first) == 0 && (this.second - that.second) > 0) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $less$eq(SecondarySortKey that) {
        if (this.$less(that)) {
            return true;
        } else return this.first.equals(that.first) && this.second == that.second;
    }

    @Override
    public boolean $greater$eq(SecondarySortKey that) {
        if (this.$greater(that)) {
            return true;
        } else return this.first.equals(that.first) && this.second == that.second;
    }

    @Override
    public int compareTo(SecondarySortKey that) {
        if (first.compareTo(that.first) != 0) {
            return first.compareTo(that.first);
        }
        return second - that.second;
    }
}
