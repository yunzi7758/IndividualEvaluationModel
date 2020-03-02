package individualAssessment1.domain;

import lombok.Data;

/**
 * 输出到es的数据
 *
 * 某个会员在某个评估模型上获得的当前评分
 */
@Data
public class IaOutData {

    // 个体评估模型ID
    private Long iaid;

    // 会员ID
    private String memberId;

    // 评分
    private Long score;

    @Override
    public String toString() {

        return "IaOutData{" +
                "iaid=" + iaid +
                ", memberId='" + memberId + '\'' +
                ", score=" + score +
                '}';
    }
}
