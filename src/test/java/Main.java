import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main {
    public static void main(String[] args) {
        String str = "{\n" +
                "\n" +
                "  \"category1\": \"住宿服务\",\n" +
                "\n" +
                "  \"category2\": \"宾馆酒店\",\n" +
                "\n" +
                "  \"category3\": \"\",\n" +
                "\n" +
                "  \"eventId\": 28680,\n" +
                "\n" +
                "  \"event_id\": 14,\n" +
                "\n" +
                "  \"is_in_home_mars_geohash6\": \"false\",\n" +
                "\n" +
                "  \"is_in_maicai_delivery_area\": \"false\",\n" +
                "\n" +
                "  \"is_in_work_mars_geohash6\": \"false\",\n" +
                "\n" +
                "  \"poi_id\": \"1296303664889106441\",\n" +
                "\n" +
                "  \"poi_name\": \"The Beta轻奢酒店(重庆沙坪坝三峡广场店)\",\n" +
                "\n" +
                "  \"push_type\": \"HI\",\n" +
                "\n" +
                "  \"report_time\": \"2021-03-30 18:57:23\",\n" +
                "\n" +
                "  \"sceneId\": 15605,\n" +
                "\n" +
                "  \"source\": 1,\n" +
                "\n" +
                "  \"timestamp\": 1617102892995,\n" +
                "\n" +
                "  \"traceId\": 1830531241328167390,\n" +
                "\n" +
                "  \"userId\": 789970854\n" +
                "\n" +
                "}";
        JSONObject jsonValue = JSON.parseObject(str);
        Map<Long,Object> map = formatJsonData(jsonValue);
        System.out.println(JSON.parseObject(JSON.toJSONString(map)));
    }

    private static Map<Long,Object> formatJsonData(JSONObject jsonValue){
        List<Map<String,Object>> openSceneList = new ArrayList<>();
        Map<String,Object> map = new HashMap<>();
        //抽取出sceneId，eventId，timestamp3个字段   标准字段
        long sceneId = jsonValue.getLong("sceneId");
        long eventId = jsonValue.getLong("eventId");
        long timeStamp = jsonValue.getLong("timestamp");
        map.put("sceneId",sceneId);
        map.put("eventId",eventId);
        map.put("timeStamp",timeStamp);
        //去除不必要的key
        jsonValue.remove("sceneId");
        jsonValue.remove("eventId");
        jsonValue.remove("timestamp");
        jsonValue.remove("traceId");
        map.put("extra",jsonValue);
        openSceneList.add(map);

        //根据eventId获取opensceneId
        long openSceneId = 123l;

        return new HashMap<Long,Object>(){
            {
                put(openSceneId,openSceneList);
            }
        };
    }
}
