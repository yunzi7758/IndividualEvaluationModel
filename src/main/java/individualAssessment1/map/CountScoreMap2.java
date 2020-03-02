package individualAssessment1.map;

import individualAssessment1.domain.Event;
import individualAssessment1.domain.IaOutData;
import individualAssessment1.domain.OutData;
import individualAssessment1.domain.Rule;
import individualAssessment1.uitl.IndividualAssessment;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;

import java.io.IOException;
import java.util.*;


/**
 *
 */
public class CountScoreMap2 extends RichMapFunction<Event, IaOutData> {
    private ValueState<HashMap<String, HashMap<String, Long>>> iaidMemberidScoreMapState;
    private Map<String, List<Rule>> idRulesMap;
    private Map<String, Set<String>> ruleOperationTypeIdsMap;


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 初始化状态
        iaidMemberidScoreMapState = getRuntimeContext().getState(new ValueStateDescriptor<HashMap<String, HashMap<String, Long>>>(
                "iaidMemberidScoreMap",
                TypeInformation.of(new TypeHint<HashMap<String, HashMap<String, Long>>>() {})
        ));


    }

    private void initIaidMemberidScoreMapState() throws IOException {
        if (idRulesMap != null){
            HashMap<String, HashMap<String, Long>> value = iaidMemberidScoreMapState.value();
            if (null == value) {
                // keyby 之后 将数据分区隔离了，他们的state 是不共享的。
                initState();
            }else {
                return;
            }

        }

        idRulesMap = IndividualAssessment.idRulesMap;
        ruleOperationTypeIdsMap = IndividualAssessment.ruleOperationTypeIdsMap;
        initState();


    }

    private void initState() throws IOException {
        // 初始化状态中 个体评估模型ID
        if (idRulesMap != null) {
            HashMap<String, HashMap<String, Long>> value = iaidMemberidScoreMapState.value();
            if (null == value) {
                value = new HashMap<>(8);
            }

            Set<Map.Entry<String, List<Rule>>> entries = idRulesMap.entrySet();
            for (Map.Entry<String, List<Rule>> entry : entries) {

                value.put(entry.getKey(), null);

            }
            iaidMemberidScoreMapState.update(value);


        }
    }

    @Override
    public IaOutData map(Event inputStr) throws Exception {


        initIaidMemberidScoreMapState();

        OutData outData = new OutData();
        List<IaOutData> iaOutDatas = new ArrayList<>(8);
        outData.setIaOutDatas(iaOutDatas);
        Event event = null;
        HashMap<String, HashMap<String, Long>> value = null;



        String memberId = "";

        if (ruleOperationTypeIdsMap != null) {
            // 将输入字符串转换为 event对象
//            event = JSON.parseObject(inputStr, Event.class);

            event = inputStr;
            // 获取当前行为类型
            String operationType = (String) event.getPayload().get("type");

            // 根据行为类型 获取相应的 个体评估模型列表
            Set<String> iaIds = ruleOperationTypeIdsMap.get(operationType);

            // 获取状态值
            value = iaidMemberidScoreMapState.value();
//            System.out.println("state value:"+value);



            String iaId = (String) event.getPayload().get("model_id");
//            for (String iaId : iaIds) {
                IaOutData iaOutData = new IaOutData();
                iaOutDatas.add(iaOutData);
                // 获取状态中的会员分数信息
                HashMap<String, Long> memberIdScoreMap = value.get(iaId);
                if (memberIdScoreMap == null) {
                    memberIdScoreMap = new HashMap<>(8);
                    value.put(iaId,memberIdScoreMap);
                }

                // 获取当前操作的会员ID
                memberId = (String) event.getPayload().get("member_id");

                // 获取当前会员已有的分数
                Long score = memberIdScoreMap.get(memberId);

                if (score == null) {
                    // 如果不存在分数，则给默认基础分
//                    System.out.println("{member_id:" + memberId +
//                            "} is new member, score  is default 10");
                    score = 10L;
                }
// else {
//                    // 使用sum 所以设0先
//                    score = 0L;
//                }
//            score = 0L;

                // 获取当前 个体评估模型的 所有规则
                List<Rule> rules = idRulesMap.get(iaId);
//            System.out.println("old score : "+ score);
                // 用规则的分值 加 已有分值
                for (Rule rule : rules) {
                    if (rule.getOperationType().equals(operationType)) {
                        score += rule.getScore();
                    }
                }
                System.out.println("{iaia:" + iaId+
                        " member_id:" + memberId + " operationType: "+operationType  +
                        " } is old member , score is {" + score +
                        "}");

                //
                memberIdScoreMap.put(memberId, score);

                iaOutData.setIaid(Long.parseLong(iaId));
                iaOutData.setMemberId(memberId);
                iaOutData.setScore(score);
//            }

        }
//        System.out.println("update value"+value);
        iaidMemberidScoreMapState.update(value);


//        return outData;
        return outData.getIaOutDatas().get(0);
    }
}
