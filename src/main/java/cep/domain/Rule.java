package cep.domain;

/**
 * 个体评估模型的规则
 */
public class Rule {

    public static final String TYPE_LOGIN = "login";
    public static final String TYPE_PAY = "pay";

    // 个体评估模型ID
    private Long sceneIc;

    // 行为
    private String operationType;

    // 做过或者没做过
    private Long doOrNot;

    // ===== 事件关系

    public static final Long AND = 1L;
    public static final Long OR = 2L;
    // 且 或
    private Long andOr;

    // 当前事件发生次数
    private Long doOrNotTimes;

    // ===== 事件序列


    // 下个事件发生的 时间限制

    public static final Long UNIT_SECOND = 1L;
    // 时间单位
    private Long timeUnit;

    // 时间值
    private Long timeValue;


    public Long getSceneIc() {
        return sceneIc;
    }

    public void setSceneIc(Long sceneIc) {
        this.sceneIc = sceneIc;
    }

    public String getOperationType() {
        return operationType;
    }

    public void setOperationType(String operationType) {
        this.operationType = operationType;
    }

    public Long getDoOrNot() {
        return doOrNot;
    }

    public void setDoOrNot(Long doOrNot) {
        this.doOrNot = doOrNot;
    }

    public Long getAndOr() {
        return andOr;
    }

    public void setAndOr(Long andOr) {
        this.andOr = andOr;
    }

    public Long getDoOrNotTimes() {
        return doOrNotTimes;
    }

    public void setDoOrNotTimes(Long doOrNotTimes) {
        this.doOrNotTimes = doOrNotTimes;
    }

    public Long getTimeUnit() {
        return timeUnit;
    }

    public void setTimeUnit(Long timeUnit) {
        this.timeUnit = timeUnit;
    }

    public Long getTimeValue() {
        return timeValue;
    }

    public void setTimeValue(Long timeValue) {
        this.timeValue = timeValue;
    }
}
