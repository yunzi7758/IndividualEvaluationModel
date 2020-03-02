package individualAssessment1.domain;

import lombok.Data;

import java.util.Map;


/**
 * 输入参数
 */
@Data
public class Event {
    private String table;
    private Map<String, Object> payload;
}
