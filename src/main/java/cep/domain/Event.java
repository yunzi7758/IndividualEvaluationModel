package cep.domain;

import java.util.Map;


/**
 * 输入参数
 */
public class Event {
    private String table;
    private Map<String, Object> payload;

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public Map<String, Object> getPayload() {
        return payload;
    }

    public void setPayload(Map<String, Object> payload) {
        this.payload = payload;
    }
}
