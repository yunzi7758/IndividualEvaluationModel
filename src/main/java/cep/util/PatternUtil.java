package cep.util;

import cep.domain.Scene;
import individualAssessment1.domain.Event;
import individualAssessment1.domain.Rule;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;

import java.util.List;

public class PatternUtil {

    public static Pattern createPattern(){


        Pattern<Event, Event> pattern = Pattern.<Event>begin("1")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) throws Exception {
                        return event.getPayload().get("type").equals(Rule.TYPE_LOGIN);
                    }
                })
                .times(10)
                .next("2")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) throws Exception {
                        return event.getPayload().get("type").equals(Rule.TYPE_PAY);
                    }
                }).times(5)
                ;
//        Scene scene = new Scene();
//        List<Rule> rules = scene.getRules();
        

        return pattern;
    }
}
