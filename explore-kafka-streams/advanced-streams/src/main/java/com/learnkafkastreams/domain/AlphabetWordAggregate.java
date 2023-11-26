package com.learnkafkastreams.domain;

import lombok.extern.slf4j.Slf4j;

import java.util.HashSet;
import java.util.Set;

@Slf4j
public record AlphabetWordAggregate(String key,
                                    Set<String> valueList,
                                    int runningCount) {


    public AlphabetWordAggregate() {
       this("", new HashSet<>(), 0);
    }


    public AlphabetWordAggregate updateNewEvents(String key, String newValue){
        var runningCount = this.runningCount+1;
        valueList.add(newValue);

        return new AlphabetWordAggregate(key, valueList, runningCount);
    }


    public static void main(String[] args) {


        var al =new AlphabetWordAggregate();

    }

}


