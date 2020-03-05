package cep.domain;

public class SceneOutData {


    private String memberId;

    private Long sceneId;

    public String getMemberId() {
        return memberId;
    }

    public void setMemberId(String memberId) {
        this.memberId = memberId;
    }

    public Long getSceneId() {
        return sceneId;
    }

    public void setSceneId(Long sceneId) {
        this.sceneId = sceneId;
    }


    @Override
    public String toString() {
        return "SceneOutData{" +
                "memberId='" + memberId + '\'' +
                ", sceneId=" + sceneId +
                '}';
    }
}
