package individualAssessment1.domain;

import lombok.Data;
import org.apache.commons.lang3.StringUtils;

/**
 * 个体评估模型的规则
 */
@Data
public class Rule {

    public static final String TYPE_LOGIN = "login";
    public static final String TYPE_PAY = "pay";

    // 个体评估模型ID
    private Long iaid;

    // 行为
    private String operationType;

    // 加分值
    private Long score;

    public static Long getScoreByType(String type) {
        if (StringUtils.isEmpty(type)){
            return 0L;
        }else if (type.equals("login")){
            return 1L;
        }else if (type.equals("pay")){

            return 2L;
        }
        return 0L;
    }


}
