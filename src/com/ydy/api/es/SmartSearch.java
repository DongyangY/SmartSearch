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
    // ElasticSearch����ʵ��
    private Client client;

    // �����ʴ����ֶ�����ͬ��� <ͬ��ʣ��ֶ���>
    private Map<String, String> synonyms;

    // �����ʴ��в��ش���ֶ���
    private Set<String> skips;

    /**
     * ����һ��SmartSearchʵ������ElasticSearch��Ⱥ����
     * @param ip ElasticSearch��Ⱥ����ڵ��ip��hostname
     * @param clusterName clusterName ElasticSearch��Ⱥ������
     */
    public SmartSearch(String ip, String clusterName) {
        this(ip, clusterName, 9300);
    }

    /**
     * ����һ��SmartSearchʵ������ElasticSearch��Ⱥ����
     * @param ip ElasticSearch��Ⱥ����ڵ��ip��hostname
     * @param clusterName clusterName ElasticSearch��Ⱥ������
     * @param transport ElasticSearch��transport�˿ڣ�Ĭ��9300
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
     * ���ElasticSearch�Դ�API��Clientʵ��
     * ���Խ����Դ�API����������
     * @return Clientʵ��
     */
    public Client getClient() {
        return client;
    }


    /**
     * �ر�ElasticSearch����
     */
    public void close() {
        if (client == null) return;
        client.close();
    }

    /**
     * �������ؽ���ķ�װ
     */
    public static class SearchResult {
        // ��������ķִʽ���������ڸ���
        public List<String> highlights;

        // ��������������ܻش����� <�ֶ�����Ƕ���ֶ�>
        public Map<String, Object> answers;

        // ElasticSearch�Դ�API����������ʵ��
        public SearchResponse response;

        // ȫ�������������Ľ������JSON��ʽ
        public String json;

        public SearchResult(List<String> highlights, Map<String, Object> answers, SearchResponse response, String json) {
            this.highlights = highlights;
            this.answers = answers;
            this.response = response;
            this.json = json;
        }
    }

    /**
     * ȫ�ֶ������������ʴ�API
     * @param index ������index
     * @param type ������type
     * @param text ����������
     * @param size �������������ҳ��һҳ���ٸ�
     * @param from �������������ҳ���ڼ�ҳ
     * @param explain �Ƿ񷵻�����Ľ��ͣ������
     * @return �������
     */
    public SearchResult searchAllWithQA(String index, String type, String text, int size, int from, boolean explain) {
        // ʹ��IKAnalyzer����������ִ�
        List<String> keywords = analyze(index, "ik", text);

        // ȫ������
        SearchResponse res = searchAll(index, type, keywords, size, from, explain);

        // �����ʴ�
        Map<String, Object> qa = getQA(index, type, res, keywords);

        return new SearchResult(keywords, qa, res, res.toString());
    }

    /**
     * ʹ�÷ִ����ִ�
     * @param index index
     * @param analyzer �ִ�����
     * @param text ���ִ����
     * @return �ִʽ������
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
     * ȫ�ֶ�����
     * @param index ������index
     * @param type ������type
     * @param keywords ��������ķִ�
     * @param size �������������ҳ��һҳ���ٸ�
     * @param from �������������ҳ���ڼ�ҳ
     * @param explain �Ƿ񷵻�����Ľ��ͣ������
     * @return ʹ��TF-IDF���������������
     */
    public SearchResponse searchAll(String index, String type, List<String> keywords, int size, int from, boolean explain) {
        // _all��һ��Ĭ�����ɵ������ֶ�������Ϻ�����ֶΣ�����ȫ�ֶ�ƥ��
        return search(index, type, "_all", null, keywords, size, from, null, null, null, explain);
    }

    /**
     * �����ӿ�
     * @param index ������index
     * @param type ������type
     * @param queryTerm �������ֶ���
     * @param resultTerm ���ص��ֶμ���null��ȫ������
     * @param keywords ��������ķִ�
     * @param size �������������ҳ��һҳ���ٸ�
     * @param from �������������ҳ���ڼ�ҳ
     * @param sort ��������Ƿ����Ӱ�ĳ���ֶ�����null��ʾ������
     * @param sortOrder ����������, null��ʾ������
     * @param highLight �Ƿ��ĳЩ�ֶ������Զ�������ǩ��null��ʾ������
     * @param explain �Ƿ񷵻�����Ľ��ͣ������
     * @return ���������
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
            // ��TF-IDF�⣬��ĳ���ֶ����Ӵ��Ȩֵ
            // ʹ���ֶ�����ƥ������������ǰ��
            // ȨֵĬ��Ϊ1
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
     * �����ʴ�API
     * @param index ������index
     * @param type ������type
     * @param res ȫ�ֶ������Ľ��
     * @param keywords ��������ķִ�
     * @return ����һ�������ֶε�Ƕ�ף���һ����С������Χ
     */
    public Map<String, Object> getQA(String index, String type, SearchResponse res, List<String> keywords) {
        Map<String, Object> result = new HashMap<>();

        //Set<String> fieldSet = new HashSet<>();
        //getFieldSet(res, fieldSet);

        //System.out.println("fieldSet: " + fieldSet);

        // �Էִʽ�������Զ����ͬ���
        List<String> targetFields = getTargetField(type, keywords);

        System.out.println("targets: " + targetFields);

        for (String s : targetFields) {
            // �Զ���ĳЩ�ʲ������ֶ�������
            if (skips.contains(s)) continue;

            // ������ִʼ�ͬ�����ͬ���ֶ���
            // null��ʾ������
            Object obj = search(res, s);
            System.out.println("target: " + s + " : " + obj);
            if (obj != null) {
                result.put(s, obj);
            }
        }
        return result;
    }

    /**
     * �Էִʽ�������Զ����ͬ���
     * @param type ������type
     * @param keywords ��������ķִ�
     * @return �ִʼ�ͬ���
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

        // �ɶԲ�ͬtype�����ز�ͬ��ͬ���
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
     * �ִʽ���в�������������ֶ����Ľ���������
     * @param type ������type
     * @param keywords ��������ķִ�
     * @param fieldSet ��������е������ֶ���
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
     * ���������������صĽ���������Ƿ����ܻش������ʴ���ֶ�
     * @param res ���������
     * @param target Ѱ�ҵ��ֶ���
     * @return ���ػش���ֶΣ���������ֶ�Ƕ�ף�null��ʾ�޷��ش�
     */
    public Object search(SearchResponse res, String target) {
        SearchHit[] hits = res.getHits().getHits();
        if (hits.length > 0) {
            // ������Ϊ��������������ĵ�һ���У���Ϊ��һ�������
            return search(hits[0].getSource(), target);
        }
        return null;
    }

    /**
     * DFS����
     * @param node ����������е�һ��������صĽ��
     * @param target Ѱ�ҵ��ֶ���
     * @return ���ػش���ֶΣ���������ֶ�Ƕ�ף�null��ʾ�޷��ش�
     */
    public Object search(Map<String, Object> node, String target) {
        if (node == null) return null;
        for (Map.Entry<String, Object> entry : node.entrySet()) {
            // ������ֶ���һ��ֵ
            if (entry.getKey().equals(target)) {
                return entry.getValue();
            }

            // ������ֶ���һ��Ƕ�׵��ֶ�
            if (entry.getValue() instanceof Map) {
                Object obj = search((Map<String, Object>)entry.getValue(), target);
                if (obj != null) return obj;
            }

            // ������ֶ���һ������
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
     * ��search���ص�Object���н���
     * ���ֶΣ��������Ƕ�׵��ֶ����������
     * ��¼����Ҷ�ӽڵ��·���ϵ��ֶ����͸�Ҷ�ӽڵ��ֵ
     * @param obj �������������ʴ��������
     * @param tmp ����ʱװ�м���
     * @param res ��ƽ�����������Ԫ�ش���ͬҶ�ӽڵ�
     * �ڲ�Ԫ�ش�������Ҷ�ӽڵ��·���ϵ��ֶ����͸�Ҷ�ӽڵ��ֵ
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
     * ���ݷ�
     * ��¼����Ҷ�ӽڵ��·���ϵ��ֶ����͸�Ҷ�ӽڵ��ֵ
     * @param node ����mapǶ��
     * @param tmp ����ʱװ�м���
     * @param res ��ƽ�����������Ԫ�ش���ͬҶ�ӽڵ�
     * �ڲ�Ԫ�ش�������Ҷ�ӽڵ��·���ϵ��ֶ����͸�Ҷ�ӽڵ��ֵ
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
     * �������ֶ���д���ı�
     * ����IK�ִ����е��ֵ�mydict.dic��ָ��������ֵĴ�
     * @param index ��������index
     * @param type ��������type
     * @param fileName �ļ���
     * @param size ���ؽ������
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
     * ��������������������ֶ������뼯��
     * ��Ϊͬһ��type�£�ÿ������������ֶβ�����֤��ȫһ����
     * ������ȱʧ������
     * @param res �������
     * @param set �ֶ�����
     */
    public void getAllFieldSet(SearchResponse res,  Set<String> set) {
        SearchHit[] hits = res.getHits().getHits();
        for (int i = 0; i < hits.length; i++) {
            getFieldSet(hits[i].getSource(), set);
        }
    }

    /**
     * ֻ����һ����������������ֶ������뼯��
     * @param res �������
     * @param set �ֶ�����
     */
    public void getFieldSet(SearchResponse res,  Set<String> set) {
        SearchHit[] hits = res.getHits().getHits();
        if (hits.length > 0) {
            getFieldSet(hits[0].getSource(), set);
        }
    }

    /**
     * DFS
     * @param node ���������root�ڵ�
     * @param set �ֶ�����
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
     * д���ı�
     * @param iterable ʵ��Iterable�ӿڵ�Collection
     * @param filename �ļ���
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
     * ElasticSearch������������Զ���ȫ
     * ��Ҫ�ڽ�������ʱ�������ֶΣ�Ĭ��ǰ׺ƥ��
     * ͨ��������ֶ���������ȥ�׸��ֵĴ���ȫ���ļ���
     * ��ʵ���м���ƥ��
     * @param index index
     * @param field���ڽ�������ʱ��ӵ�����ֶ���
     * @param text ����ȫ������
     * @param size ��෵�ض��ٸ���ȫ���
     * @return ���ز�ȫ�������
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
     * ʹ��ElasticSearch�Դ��ĸ�������
     * �Զ�Ϊ���ص�JSON��ƥ��ķִ��������tag
     * @param index index
     * @param type type
     * @param field ���������ֶ�
     * @param keywords �ִ�
     * @param size ��ҳÿҳ�������
     * @param from ��ҳ�ڼ�ҳ
     * @param explain �Ƿ���Ӵ�ֽ���
     * @return �����������������
     */
    public SearchResponse searchWithHighlight(String index, String type, String field, List<String> keywords,
                                              int size, int from, boolean explain) {
        List<String> hightLight = new ArrayList<>();
        hightLight.add(field);
        return search(index, type, field, null, keywords, size, from, null, null, hightLight, explain);
    }

    /**
     * ��ӡ�����ʴ�ķ��ؽ��
     * @param obj �����ʴ�ķ��ؽ��
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
     * ������������������ÿ��doc����һ������
     * @param nums ���ٸ�docʹ��һ�����ӵ���
     * @param gb �������flush
     * @param concurrent ���߳���
     * @param iterable ��������ݽṹ��ÿ��map��һ��doc
     * @param index ������index
     * @param type ������type
     * @param interval ÿ������ٻ㱨һ��
     */
    public void bulkLoad(int nums, int gb, int concurrent, Iterable<Map<String, Object>> iterable,
                         String index, String type, int interval) {
        // ע��������ص�����
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
                .setBulkActions(nums) // ���ٸ�docʹ��һ�����ӵ���
                .setBulkSize(new ByteSizeValue(gb, ByteSizeUnit.GB)) // �������flush
                .setConcurrentRequests(concurrent) // ���߳���
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
