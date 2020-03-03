package cep.util;

import cep.domain.Scene;
import individualAssessment1.domain.Event;
import individualAssessment1.domain.Rule;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;

import java.util.List;

public class PatternUtil {

    public static Pattern createPattern(){


        Pattern<Event, Event> pattern = Pattern.<Event>begin("first")
                .where(new IterativeCondition<Event>() {
                    private static final long serialVersionUID = -6301755149429716724L;

                    @Override
                    public boolean filter(Event value, Context<Event> ctx) throws Exception {
                        return value.getPayload().get("type").equals(Rule.TYPE_LOGIN);
                    }
                }).times(10);
//        Scene scene = new Scene();
//        List<Rule> rules = scene.getRules();
        

        return pattern;
    }
}
