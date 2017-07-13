import com.ydy.api.es.SmartSearch;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by ydy on 2017/7/10.
 */
public class Demo {
    public static void main(String[] args) {
        SmartSearch smartSearch = new SmartSearch("192.168.1.1", "es.ydy");

        String index = "哪个索引";
        String type = "哪个类型";
        String text = "你的搜索输入";
        int size = 10;
        int from = 0;

        SmartSearch.SearchResult result = smartSearch.searchAllWithQA(index, type, text, size, from, true);

        System.out.println(result.response);

        for (Map.Entry<String, Object> answer : result.answers.entrySet()) {
            System.out.print(answer.getKey() + ": ");
            smartSearch.printObject(answer.getValue());
            System.out.println();

            List<List<String>> res = new ArrayList<>();
            smartSearch.parseNestedObject(answer.getValue(), new ArrayList<String>(), res);

            for (List<String> l : res) {
                StringBuilder sb = new StringBuilder();
                for (String s : l) {
                    sb.append(s);
                }
                System.out.println(sb.toString());
            }
        }

        smartSearch.close();
    }
}
