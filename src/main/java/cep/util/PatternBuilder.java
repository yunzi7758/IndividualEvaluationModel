package cep.util;

import cep.domain.Rule;
import individualAssessment1.domain.Event;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.windowing.time.Time;

public class PatternBuilder<T, F extends T>   {


    private Pattern pattern;

    public PatternBuilder(Pattern pattern) {
        this.pattern = pattern;
    }

    public static <X> PatternBuilder<X, X> begin(String s) {
        return new PatternBuilder<X,X>(Pattern.begin(s));
    }


    public PatternBuilder times(Rule rule) {
        getPattern().times(rule.getDoOrNotTimes().intValue());
        return this;
    }

    public Pattern getPattern() {
        return pattern;
    }

    public void setPattern(Pattern pattern) {
        this.pattern = pattern;
    }

    public PatternBuilder followedByAny(String s) {
        getPattern().followedByAny(s);
        return this;
    }

    public PatternBuilder next(String s) {
        setPattern(getPattern().next(s));
        return this;
    }

    public PatternBuilder where(Rule rule) {
        getPattern().where(new SimpleCondition<Event>() {
            @Override
            public boolean filter(Event event) throws Exception {
                return ((String) event.getPayload().get("type")).equals(rule.getOperationType());
            }
        });
        return this;
    }

    public PatternBuilder within(Rule rule) {
        if (Rule.UNIT_SECOND .equals(rule.getTimeUnit()) ){
            getPattern().within(Time.seconds(rule.getTimeValue())) ;
        } else if (Rule.UNIT_HOUR .equals(rule.getTimeUnit()) ){
            getPattern().within(Time.hours(rule.getTimeValue())) ;
        } else if (Rule.UNIT_DAY .equals(rule.getTimeUnit()) ){
            getPattern().within(Time.days(rule.getTimeValue())) ;
        }

        return this;
    }
}
