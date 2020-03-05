package cep.domain;

import java.util.List;

public class Scene {
    public static final Long STRUCTURE_1 = 1L;
    public static final Long STRUCTURE_2 = 2L;
    private Long id;
    private Long sceneStructure;

    // LinkedList 连续的规则
    private List<Rule> rules;





    public List<Rule> getRules() {
        return rules;
    }

    public void setRules(List<Rule> rules) {
        this.rules = rules;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getSceneStructure() {
        return sceneStructure;
    }

    public void setSceneStructure(Long sceneStructure) {
        this.sceneStructure = sceneStructure;
    }

    @Override
    public String toString() {
        return "Scene{" +
                "id=" + id +
                ", sceneStructure=" + sceneStructure +
                ", rules=" + rules +
                '}';
    }
}
