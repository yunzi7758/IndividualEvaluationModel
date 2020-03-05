package cep.util;

import com.google.common.collect.Lists;

import cep.domain.Rule;
import cep.domain.Scene;
import individualAssessment1.domain.Event;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.*;

public class PatternUtil {

    public static void main(String[] args) {
        System.out.println(loadScenes());
    }

    public static Map<Long, Pattern> sidPatternMap = new HashMap<>(8);

    static {
        List<Scene> scenes = loadScenes();

        for (Scene scene : scenes) {
            PatternBuilder<Event, Event> pattern = doCreatePatternByScene(scene);
            sidPatternMap.put(scene.getId(), pattern.getPattern());
        }


    }

    public static Pattern getPatternBySceneId(Long id) {
        return sidPatternMap.get(id);
    }

    private static List<Scene> loadScenes() {
        List<Scene> scenes = new ArrayList<>(8);

        Scene scene2 = new Scene();
        scenes.add(scene2);
        scene2.setId(2L);
        scene2.setSceneStructure(Scene.STRUCTURE_2);

        List<Rule> rules2 = new LinkedList<>();
        scene2.setRules(rules2);

        Rule rule1 = new Rule();
        rule1.setSceneIc(scene2.getId());
        rule1.setOperationType(Rule.TYPE_LOGIN);
        rule1.setDoOrNot(Rule.DO);
        rule1.setAndOr(Rule.AND);
        rule1.setDoOrNotTimes(0L);
        rule1.setTimeUnit(Rule.UNIT_SECOND);
        rule1.setTimeValue(5L);

        rules2.add(rule1);

        Rule rule2 = new Rule();
        rule2.setSceneIc(scene2.getId());
        rule2.setOperationType(Rule.TYPE_PAY);
        rule2.setDoOrNot(Rule.DO);
        rule2.setAndOr(Rule.AND);
        rule2.setDoOrNotTimes(0L);
        rule2.setTimeUnit(Rule.UNIT_SECOND);
        rule2.setTimeValue(5L);

        rules2.add(rule2);


        return scenes;
    }

    private static PatternBuilder<Event, Event> doCreatePatternByScene(Scene scene) {
        List<cep.domain.Rule> rules = scene.getRules();

        PatternBuilder<Event, Event> pattern = null;

        //        Pattern<Event, Event> pattern = Pattern.<Event>begin("1")
//                .where(new SimpleCondition<Event>() {
//                    @Override
//                    public boolean filter(Event event) throws Exception {
//                        return event.getPayload().get("type").equals(Rule.TYPE_LOGIN);
//                    }
//                })
//                .times(10)
//                .next("2")
//                .where(new SimpleCondition<Event>() {
//                    @Override
//                    public boolean filter(Event event) throws Exception {
//                        return event.getPayload().get("type").equals(Rule.TYPE_PAY);
//                    }
//                }).times(5)
//                ;

        if (scene.getSceneStructure().equals(Scene.STRUCTURE_1)) {
            for (int i = 0; i < rules.size(); i++) {
                Rule rule = rules.get(i);
                if (i == 0) {
                    pattern = PatternBuilder.begin(i + 1 + "").where(rule).times(rule);

                } else {
                    pattern = pattern.followedByAny(i + 1 + "").where(rule).times(rule);
                }
            }
        } else if (scene.getSceneStructure().equals(Scene.STRUCTURE_2)) {

            for (int i = 0; i < rules.size(); i++) {
                Rule rule = rules.get(i);
                if (i == 0) {
                    pattern = PatternBuilder.begin(i + 1 + "").where(rule).within(rule);

                } else {
                    pattern = pattern.next(i + 1 + "").where(rule);
                }
            }
        }

//        Pattern<Event, Event> patternTest = Pattern.<Event>begin("1")
//                .where(new SimpleCondition<Event>() {
//                    @Override
//                    public boolean filter(Event event) throws Exception {
//                        return event.getPayload().get("type").equals(Rule.TYPE_LOGIN);
//                    }
//                })
//                .within(Time.seconds(1))
//                .next("2")
//                .where(new SimpleCondition<Event>() {
//                    @Override
//                    public boolean filter(Event event) throws Exception {
//                        return event.getPayload().get("type").equals(Rule.TYPE_PAY);
//                    }
//                });

        return pattern;
    }

}
