package com.course.kafka.broker.message;

import java.util.Set;

public class PrimeFactorsMessage {

    private Set<Long> primeFactors;

    public Set<Long> getPrimeFactors() {
        return primeFactors;
    }

    public void setPrimeFactors(Set<Long> primeFactors) {
        this.primeFactors = primeFactors;
    }

    @Override
    public String toString() {
        return "PrimeFactorsMessage [primeFactors=" + primeFactors + "]";
    }

}
