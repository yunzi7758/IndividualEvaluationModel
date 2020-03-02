package individualAssessment1.uitl;

import individualAssessment1.domain.Rule;
import lombok.Data;
import org.apache.commons.collections.CollectionUtils;

import java.util.*;

/**
 * 个体评估模型
 */
@Data
public class IndividualAssessment {

    // 个人评估模型ID 对应 多个规则
    public static Map<String, List<Rule>> idRulesMap ;

    // 规则里的操作类型 对应 多个个人评估模型ID
    public static Map<String, Set<String>> ruleOperationTypeIdsMap ;

    public void loadRules() {
        System.out.println("loadRules()");
        if (idRulesMap == null){
            idRulesMap = new HashMap<String, List<Rule>>(8);



            String a1 = "1" , b1 = "2";
            List<Rule> aRules = new ArrayList<Rule>(2);
            idRulesMap.put(a1,aRules);

            Rule a1rule = new Rule();
            a1rule.setOperationType(Rule.TYPE_LOGIN);
            a1rule.setScore(5L);
            a1rule.setIaid(Long.parseLong(a1));
            aRules.add(a1rule);


            ruleOperationTypeIdsMapPut(Rule.TYPE_LOGIN,a1);


            Rule a1rule2 = new Rule();
            a1rule2.setOperationType(Rule.TYPE_PAY);
            a1rule2.setScore(10L);
            a1rule2.setIaid(Long.parseLong(a1));
            aRules.add(a1rule2);

            ruleOperationTypeIdsMapPut(Rule.TYPE_PAY,a1);


            List<Rule> bRules = new ArrayList<Rule>(2);
            idRulesMap.put(b1,bRules);

            Rule b1rule = new Rule();
            b1rule.setOperationType(Rule.TYPE_LOGIN);
            b1rule.setScore(1L);
            b1rule.setIaid(Long.parseLong(b1));
            bRules.add(b1rule);

            ruleOperationTypeIdsMapPut(Rule.TYPE_LOGIN,b1);


            System.out.println(idRulesMap);
        }
    }

    void ruleOperationTypeIdsMapPut(String type,String id){
        System.out.println("ruleOperationTypeIdsMapPut() type :" +type+
                "  id : " +id);
        if(ruleOperationTypeIdsMap == null){
            ruleOperationTypeIdsMap = new HashMap<>(8);
        }
        Set<String> ids = ruleOperationTypeIdsMap.get(type);
        if (CollectionUtils.isEmpty(ids)){
            ids = new HashSet<>(8);
            ruleOperationTypeIdsMap.put(type,ids);
        }
        ids.add(id);
    }


}

