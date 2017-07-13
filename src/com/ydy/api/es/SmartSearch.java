package com.ydy.api.es;

import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequest;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.suggest.SuggestRequestBuilder;
import org.elasticsearch.action.suggest.SuggestResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.completion.CompletionSuggestionBuilder;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.util.*;

/**
 * Created by ydy on 2017/7/10.
 */
public class SmartSearch {
    // ElasticSearch连接实例
    private Client client;

    // 智能问答中字段名的同义词 <同义词，字段名>
    private Map<String, String> synonyms;

    // 智能问答中不回答的字段名
    private Set<String> skips;

    /**
     * 构造一个SmartSearch实例，与ElasticSearch集群交互
     * @param ip ElasticSearch集群任意节点的ip或hostname
     * @param clusterName clusterName ElasticSearch集群的名称
     */
    public SmartSearch(String ip, String clusterName) {
        this(ip, clusterName, 9300);
    }

    /**
     * 构造一个SmartSearch实例，与ElasticSearch集群交互
     * @param ip ElasticSearch集群任意节点的ip或hostname
     * @param clusterName clusterName ElasticSearch集群的名称
     * @param transport ElasticSearch的transport端口，默认9300
     */
    public SmartSearch(String ip, String clusterName,int transport) {
        if (ip == null || ip.length() == 0 || clusterName == null ||
                clusterName.length() == 0) {
            return;
        }

        synonyms = new HashMap<>();
        skips = new HashSet<>();

        try {
            Settings settings = Settings.settingsBuilder()
                    .put("cluster.name", clusterName).build();

            client = TransportClient.builder().settings(settings).build()
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(ip), transport));
        } catch (Exception e) {
            e.printStackTrace();
        }

        try(BufferedReader br = new BufferedReader(new FileReader("synonyms.txt"))) {
            String line = br.readLine();

            while (line != null) {
                String[] splits = line.split(" ");
                if (splits.length > 1) {
                    for (int i = 1; i < splits.length; i++) {
                        synonyms.put(splits[i], splits[0]);
                    }
                }
                line = br.readLine();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        try(BufferedReader br = new BufferedReader(new FileReader("skips.txt"))) {
            String line = br.readLine();

            while (line != null) {
                skips.add(line);
                line = br.readLine();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("synonyms: " + synonyms);
        System.out.println("skips: " + skips);
    }

    /**
     * 获得ElasticSearch自带API的Client实例
     * 可以进行自带API的其他操作
     * @return Client实例
     */
    public Client getClient() {
        return client;
    }


    /**
     * 关闭ElasticSearch连接
     */
    public void close() {
        if (client == null) return;
        client.close();
    }

    /**
     * 搜索返回结果的封装
     */
    public static class SearchResult {
        // 搜索输入的分词结果，可用于高亮
        public List<String> highlights;

        // 对搜索输入的智能回答内容 <字段名，嵌套字段>
        public Map<String, Object> answers;

        // ElasticSearch自带API的搜索返回实例
        public SearchResponse response;

        // 全文搜索并排序后的结果集，JSON格式
        public String json;

        public SearchResult(List<String> highlights, Map<String, Object> answers, SearchResponse response, String json) {
            this.highlights = highlights;
            this.answers = answers;
            this.response = response;
            this.json = json;
        }
    }

    /**
     * 全字段搜索加智能问答API
     * @param index 搜索的index
     * @param type 搜索的type
     * @param text 搜索的输入
     * @param size 用于搜索结果分页，一页多少个
     * @param from 用于搜索结果分页，第几页
     * @param explain 是否返回排序的解释，即打分
     * @return 搜索结果
     */
    public SearchResult searchAllWithQA(String index, String type, String text, int size, int from, boolean explain) {
        // 使用IKAnalyzer对搜索输入分词
        List<String> keywords = analyze(index, "ik", text);

        // 全文搜索
        SearchResponse res = searchAll(index, type, keywords, size, from, explain);

        // 智能问答
        Map<String, Object> qa = getQA(index, type, res, keywords);

        return new SearchResult(keywords, qa, res, res.toString());
    }

    /**
     * 使用分词器分词
     * @param index index
     * @param analyzer 分词类型
     * @param text 被分词语句
     * @return 分词结果链表
     */
    public List<String> analyze(String index, String analyzer, String text) {
        List<String> res = new ArrayList<>();
        if (text == null || text.length() == 0 || analyzer == null ||
                analyzer.length() == 0) {
            return res;
        }

        AnalyzeRequest analyzeRequest = new AnalyzeRequest(index).text(text).analyzer(analyzer);
        List<AnalyzeResponse.AnalyzeToken> tokens = client.admin().indices().analyze(analyzeRequest).actionGet().getTokens();
        for (AnalyzeResponse.AnalyzeToken token : tokens) {
            res.add(token.getTerm());
        }
        return res;
    }

    /**
     * 全字段搜索
     * @param index 搜索的index
     * @param type 搜索的type
     * @param keywords 搜索输入的分词
     * @param size 用于搜索结果分页，一页多少个
     * @param from 用于搜索结果分页，第几页
     * @param explain 是否返回排序的解释，即打分
     * @return 使用TF-IDF排序后的搜索结果集
     */
    public SearchResponse searchAll(String index, String type, List<String> keywords, int size, int from, boolean explain) {
        // _all是一个默认生成的所有字段内容组合后的新字段，用于全字段匹配
        return search(index, type, "_all", null, keywords, size, from, null, null, null, explain);
    }

    /**
     * 搜索接口
     * @param index 搜索的index
     * @param type 搜索的type
     * @param queryTerm 搜索的字段名
     * @param resultTerm 返回的字段集，null将全部返回
     * @param keywords 搜索输入的分词
     * @param size 用于搜索结果分页，一页多少个
     * @param from 用于搜索结果分页，第几页
     * @param sort 搜索结果是否增加按某个字段排序，null表示不增加
     * @param sortOrder 正序还是逆序, null表示不增加
     * @param highLight 是否对某些字段增加自动高亮标签，null表示不增加
     * @param explain 是否返回排序的解释，即打分
     * @return 搜索结果集
     */
    public SearchResponse search(String index, String type, String queryTerm, List<String> resultTerm,
                                 List<String> keywords, int size, int from, String sort, SortOrder sortOrder,
                                 List<String> highLight, boolean explain) {
        if (index == null || type == null || queryTerm == null || keywords == null ||
                index.length() == 0 || type.length() == 0 || queryTerm.length() == 0 ||
                keywords.size() == 0 || size < 0 || from < 0) {
            return null;
        }

        BoolQueryBuilder boolShould = QueryBuilders.boolQuery();

        for (String keyword : keywords) {
            boolShould.should(QueryBuilders.matchPhraseQuery(queryTerm, keyword));
            // 除TF-IDF外，对某个字段增加打分权值
            // 使该字段命中匹配的搜索结果往前排
            // 权值默认为1
            boolShould.should(QueryBuilders.matchPhraseQuery("tag", keyword).boost(2.0f));
        }

        BoolQueryBuilder boolMust = QueryBuilders.boolQuery();

        boolMust.must(boolShould);

        SearchRequestBuilder builder = client.prepareSearch(index)
                .setTypes(type)
                .setQuery(boolMust)
                .setSize(size)
                .setFrom(from);

        builder.setExplain(explain);

        if (highLight != null) {
            for (String s : highLight) {
                builder.addHighlightedField(s);
            }
        }

        if (resultTerm != null) {
            for (String s : resultTerm) {
                builder.addField(s);
            }
        }

        if (sort != null && sortOrder != null) {
            builder.addSort(sort, sortOrder);
        }

        SearchResponse res = builder.get();

        return res;
    }

    /**
     * 智能问答API
     * @param index 搜索的index
     * @param type 搜索的type
     * @param res 全字段搜索的结果
     * @param keywords 搜索输入的分词
     * @return 返回一个或多个字段的嵌套，进一步缩小搜索范围
     */
    public Map<String, Object> getQA(String index, String type, SearchResponse res, List<String> keywords) {
        Map<String, Object> result = new HashMap<>();

        //Set<String> fieldSet = new HashSet<>();
        //getFieldSet(res, fieldSet);

        //System.out.println("fieldSet: " + fieldSet);

        // 对分词结果增加自定义的同义词
        List<String> targetFields = getTargetField(type, keywords);

        System.out.println("targets: " + targetFields);

        for (String s : targetFields) {
            // 自定义某些词不进行字段名搜索
            if (skips.contains(s)) continue;

            // 搜索与分词及同义词相同的字段名
            // null表示不存在
            Object obj = search(res, s);
            System.out.println("target: " + s + " : " + obj);
            if (obj != null) {
                result.put(s, obj);
            }
        }
        return result;
    }

    /**
     * 对分词结果增加自定义的同义词
     * @param type 搜索的type
     * @param keywords 搜索输入的分词
     * @return 分词加同义词
     */
    public List<String> getTargetField(String type, List<String> keywords) {
        List<String> res = new ArrayList<>(keywords);

        System.out.println("analyzer: " + keywords);

        for (String keyword : keywords) {
            if (synonyms.containsKey(keyword)) {
                System.out.println("synonyms: " + keyword + " => " + synonyms.get(keyword));
                res.add(synonyms.get(keyword));
            }
        }

        // 可对不同type，加载不同的同义词
//        for (String keyword : keywords) {
//            if (type.equals("type1")) {
//                if (type1-synonyms.containsKey(keyword)) {
//                    System.out.println("synonyms: " + keyword + " => " + type1-synonyms.get(keyword));
//                    res.add(type1-synonyms.get(keyword));
//                }
//            } else if (type.equals("type2")) {
//                if (type2-synonyms.containsKey(keyword)) {
//                    System.out.println("synonyms: " + keyword + " => " + type2-synonyms.get(keyword));
//                    res.add(type2-synonyms.get(keyword));
//                }
//            }
//        }

        return res;
    }

    /**
     * 分词结果中不是搜索结果中字段名的将不被搜索
     * @param type 搜索的type
     * @param keywords 搜索输入的分词
     * @param fieldSet 搜索结果中的所有字段名
     * @return
     */
    public List<String> getTargetField(String type, List<String> keywords, Set<String> fieldSet) {
        List<String> res = new ArrayList<>();

        System.out.println("analyzer: " + keywords);

        for (String keyword : keywords) {
            if (fieldSet.contains(keyword)) {
                res.add(keyword);
            }

            if (synonyms.containsKey(keyword)) {
                System.out.println("synonyms: " + keyword + " => " + synonyms.get(keyword));
                res.add(synonyms.get(keyword));
            }
        }
        return res;
    }

    /**
     * 在搜索结果集最相关的结果中搜索是否含有能回答智能问答的字段
     * @param res 搜索结果集
     * @param target 寻找的字段名
     * @return 返回回答的字段，数组或者字段嵌套，null表示无法回答
     */
    public Object search(SearchResponse res, String target) {
        SearchHit[] hits = res.getHits().getHits();
        if (hits.length > 0) {
            // 我们认为答案在搜索结果集的第一个中，因为第一个最相关
            return search(hits[0].getSource(), target);
        }
        return null;
    }

    /**
     * DFS搜索
     * @param node 搜索结果集中第一个或最相关的结果
     * @param target 寻找的字段名
     * @return 返回回答的字段，数组或者字段嵌套，null表示无法回答
     */
    public Object search(Map<String, Object> node, String target) {
        if (node == null) return null;
        for (Map.Entry<String, Object> entry : node.entrySet()) {
            // 如果该字段是一个值
            if (entry.getKey().equals(target)) {
                return entry.getValue();
            }

            // 如果该字段是一个嵌套的字段
            if (entry.getValue() instanceof Map) {
                Object obj = search((Map<String, Object>)entry.getValue(), target);
                if (obj != null) return obj;
            }

            // 如果该字段是一个数组
            if (entry.getValue() instanceof List) {
                List<Object> l = (List<Object>) entry.getValue();
                Map<String, Object> map = new HashMap<>();
                int i = 0;
                for (Object o : l) {
                    map.put(String.valueOf(i++), o);
                }
                Object obj = search(map, target);
                if (obj != null) return obj;
            }
        }
        return null;
    }

    /**
     * 对search返回的Object进行解析
     * 对字段，数组或者嵌套的字段这三种情况
     * 记录所有叶子节点的路径上的字段名和该叶子节点的值
     * @param obj 待解析的智能问答搜索结果
     * @param tmp 回溯时装中间结果
     * @param res 扁平化成链表，外层元素代表不同叶子节点
     * 内层元素代表所有叶子节点的路径上的字段名和该叶子节点的值
     */
    public void parseNestedObject(Object obj, List<String> tmp, List<List<String>> res) {
        if (obj != null) {
            if (obj instanceof Map) {
                parseNestedObject((Map<String, Object>) obj, tmp, res);
            } else if (obj instanceof List){
                List<Object> l = (List<Object>) obj;
                Map<String, Object> map = new HashMap<>();
                int i = 0;
                for (Object o : l) {
                    map.put("^" + String.valueOf(i++), o);
                }
                parseNestedObject(map, tmp, res);
            } else {
                List<String> l = new ArrayList<>();
                l.add(String.valueOf(obj));
                res.add(l);
            }
        } else {
            System.out.print("the object parsed is empty.");
        }
    }

    /**
     * 回溯法
     * 记录所有叶子节点的路径上的字段名和该叶子节点的值
     * @param node 解析map嵌套
     * @param tmp 回溯时装中间结果
     * @param res 扁平化成链表，外层元素代表不同叶子节点
     * 内层元素代表所有叶子节点的路径上的字段名和该叶子节点的值
     */
    public void parseNestedObject(Map<String, Object> node, List<String> tmp, List<List<String>> res) {
        if (node == null) return;
        for (Map.Entry<String, Object> entry : node.entrySet()) {
            tmp.add(entry.getKey());

            if (entry.getValue() instanceof Map) {
                tmp.add(".");
                parseNestedObject((Map<String, Object>) entry.getValue(), tmp, res);
                tmp.remove(tmp.size() - 1);
                tmp.remove(tmp.size() - 1);
            } else if (entry.getValue() instanceof List) {
                tmp.add(".");
                List<Object> l = (List<Object>) entry.getValue();
                Map<String, Object> map = new HashMap<>();
                int i = 0;
                for (Object o : l) {
                    map.put("^" + String.valueOf(i++), o);
                }
                parseNestedObject(map, tmp, res);
                tmp.remove(tmp.size() - 1);
                tmp.remove(tmp.size() - 1);
            } else {
                tmp.add(":");
                tmp.add(String.valueOf(entry.getValue()));
                res.add(new ArrayList<String>(tmp));
                tmp.remove(tmp.size() - 1);
                tmp.remove(tmp.size() - 1);
                tmp.remove(tmp.size() - 1);
            }
        }
    }

    /**
     * 将所有字段名写入文本
     * 用于IK分词器中的字典mydict.dic，指定不被拆分的词
     * @param index 被导出的index
     * @param type 被导出的type
     * @param fileName 文件名
     * @param size 返回结果个数
     */
    public void writeFields(String index, String type, String fileName, int size) {
        SearchResponse res = client.prepareSearch(index)
                .setTypes(type)
                .setSize(size)
                .get();

        Set<String> fieldSet = new HashSet<>();
        getAllFieldSet(res, fieldSet);

        writeToFile(fieldSet, fileName);
    }

    /**
     * 将所有搜索结果的所有字段名存入集合
     * 因为同一个type下，每个搜索结果的字段并不保证完全一样的
     * 可以有缺失或增添
     * @param res 搜索结果
     * @param set 字段名集
     */
    public void getAllFieldSet(SearchResponse res,  Set<String> set) {
        SearchHit[] hits = res.getHits().getHits();
        for (int i = 0; i < hits.length; i++) {
            getFieldSet(hits[i].getSource(), set);
        }
    }

    /**
     * 只将第一个搜索结果的所有字段名存入集合
     * @param res 搜索结果
     * @param set 字段名集
     */
    public void getFieldSet(SearchResponse res,  Set<String> set) {
        SearchHit[] hits = res.getHits().getHits();
        if (hits.length > 0) {
            getFieldSet(hits[0].getSource(), set);
        }
    }

    /**
     * DFS
     * @param node 搜索结果的root节点
     * @param set 字段名集
     */
    public void getFieldSet(Map<String, Object> node, Set<String> set) {
        for (Map.Entry<String, Object> entry : node.entrySet()) {
            set.add(entry.getKey());

            if (entry.getValue() instanceof Map) {
                getFieldSet((Map<String, Object>) entry.getValue(), set);
            }

            if (entry.getValue() instanceof List) {
                List<Object> l = (List<Object>) entry.getValue();
                Map<String, Object> map = new HashMap<>();
                if (l.size() <= 0) continue;
                // skip, since list elements have the same fields.
                map.put(String.valueOf(0), l.get(0));
                getFieldSet(map, set);
            }
        }
    }

    /**
     * 写入文本
     * @param iterable 实现Iterable接口的Collection
     * @param filename 文件名
     */
    public void writeToFile(Iterable<String> iterable, String filename) {
        try (PrintWriter writer = new PrintWriter(new FileWriter(filename))) {
            for (String s : iterable) {
                writer.println(s);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * ElasticSearch对搜索输入的自动补全
     * 需要在建立索引时添加相关字段，默认前缀匹配
     * 通过在相关字段添加逐个削去首个字的待补全语句的集合
     * 可实现中间字匹配
     * @param index index
     * @param field　在建立索引时添加的相关字段名
     * @param text 待补全的输入
     * @param size 最多返回多少个补全结果
     * @return 返回补全结果链表
     */
    public List<String> getSuggest(String index, String field, String text, int size) {
        List<String> res =  new ArrayList<>();
        if (index == null || field == null || text == null || client == null || index.length() == 0 ||
                field.length() == 0 || text.length() == 0) {
            return res;
        }

        CompletionSuggestionBuilder suggestBuilder = new CompletionSuggestionBuilder("completeMe");
        suggestBuilder.text(text);
        suggestBuilder.field(field);
        suggestBuilder.size(size);
        SuggestRequestBuilder suggestRequestBuilder = client.prepareSuggest(index).addSuggestion(suggestBuilder);

        SuggestResponse suggestResponse = suggestRequestBuilder.execute().actionGet();
        Iterator<? extends Suggest.Suggestion.Entry.Option> iter = suggestResponse.getSuggest().getSuggestion("completeMe").iterator().next().getOptions().iterator();

        while (iter.hasNext()) {
            Suggest.Suggestion.Entry.Option next = iter.next();
            res.add(next.getText().toString());
        }

        return res;
    }

    /**
     * 使用ElasticSearch自带的高亮功能
     * 自动为返回的JSON中匹配的分词两边添加tag
     * @param index index
     * @param type type
     * @param field 被高亮的字段
     * @param keywords 分词
     * @param size 分页每页结果个数
     * @param from 分页第几页
     * @param explain 是否添加打分解释
     * @return 被高亮的搜索结果集
     */
    public SearchResponse searchWithHighlight(String index, String type, String field, List<String> keywords,
                                              int size, int from, boolean explain) {
        List<String> hightLight = new ArrayList<>();
        hightLight.add(field);
        return search(index, type, field, null, keywords, size, from, null, null, hightLight, explain);
    }

    /**
     * 打印智能问答的返回结果
     * @param obj 智能问答的返回结果
     */
    public void printObject(Object obj) {
        if (obj != null) {
            if (obj instanceof Map) {
                System.out.print((Map<String, Object>) obj);
            } else if (obj instanceof List){
                System.out.print((List<Object>) obj);
            } else {
                System.out.print(String.valueOf(obj));
            }
        } else {
            System.out.print("not found.");
        }
    }

    /**
     * 批量建立索引，以免每个doc建立一次连接
     * @param nums 多少个doc使用一次连接导入
     * @param gb 多大量将flush
     * @param concurrent 多线程数
     * @param iterable 导入的数据结构，每个map是一个doc
     * @param index 被导入index
     * @param type 被导入type
     * @param interval 每导入多少汇报一次
     */
    public void bulkLoad(int nums, int gb, int concurrent, Iterable<Map<String, Object>> iterable,
                         String index, String type, int interval) {
        // 注册监听，回调函数
        BulkProcessor bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long l, BulkRequest bulkRequest) {

            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, BulkResponse bulkResponse) {

            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, Throwable throwable) {

            }
        })
                .setBulkActions(nums) // 多少个doc使用一次连接导入
                .setBulkSize(new ByteSizeValue(gb, ByteSizeUnit.GB)) // 多大量将flush
                .setConcurrentRequests(concurrent) // 多线程数
                .build();

        int k = 0;
        for (Map<String, Object> map : iterable) {
            bulkProcessor.add(new IndexRequest(index, type, String.valueOf(++k)).source(map));
            if (k % interval == 0) {
                System.out.println(k);
            }
        }

        bulkProcessor.close();
    }
}
