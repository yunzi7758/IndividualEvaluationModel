package individualAssessment1.domain;


import lombok.Data;

import java.util.*;

/**
 * 输出到es的数据
 *
 * 某个会员在某个评估模型上获得的当前评分
 */
@Data
public class OutData {



    // 个体评估模型ID
    private List<IaOutData> iaOutDatas;

    public List<IaOutData> getFinalIaOutDatas() {

        HashMap<Long, HashMap<String, Long>> iaidMemberidScoreMap = new HashMap<>(iaOutDatas.size());


        for (IaOutData iaOutData : iaOutDatas) {
            HashMap<String, Long> memberidScoreMap = iaidMemberidScoreMap.get(iaOutData.getIaid());
            if (memberidScoreMap == null){
                memberidScoreMap = new HashMap<>(iaOutDatas.size());
                iaidMemberidScoreMap.put(iaOutData.getIaid(),memberidScoreMap);
            }

            memberidScoreMap.put(iaOutData.getMemberId(),iaOutData.getScore());
        }

        List<IaOutData> retIaOutDatas  = new ArrayList<>(8);
        Set<Map.Entry<Long, HashMap<String, Long>>> iaidMemberidScoreMapEntries = iaidMemberidScoreMap.entrySet();
        if (iaidMemberidScoreMapEntries != null){
            for (Map.Entry<Long, HashMap<String, Long>> longHashMapEntry : iaidMemberidScoreMapEntries) {
                if (longHashMapEntry.getValue() != null){
                    Set<Map.Entry<String, Long>> entries = longHashMapEntry.getValue().entrySet();
                    if (entries != null){
                        for (Map.Entry<String, Long> entry : entries) {

                            IaOutData iaOutData = new IaOutData();
                            iaOutData.setIaid(longHashMapEntry.getKey());
                            iaOutData.setMemberId(entry.getKey());
                            iaOutData.setScore(entry.getValue());

                            retIaOutDatas.add(iaOutData);
                        }
                    }
                }
            }
        }

        
        return retIaOutDatas;
    }
    
    public List<IaOutData> getIaOutDatas() {
        return iaOutDatas;
    }

    public void setIaOutDatas(List<IaOutData> iaOutDatas) {
        this.iaOutDatas = iaOutDatas;
    }

    @Override
    public String toString() {

        StringBuilder stringBuilder = new StringBuilder("iaOutDatas:");
        for (IaOutData iaOutData : iaOutDatas) {
            stringBuilder.append(iaOutData.toString());
        }
        return stringBuilder.toString();
    }
}

