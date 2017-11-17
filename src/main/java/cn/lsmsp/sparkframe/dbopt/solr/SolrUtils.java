package cn.lsmsp.sparkframe.dbopt.solr;

import cn.lsmsp.sparkframe.common.util.HttpRequest;
import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.impl.LBHttpSolrServer;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class SolrUtils {

  private  HttpSolrServer solr;
    public void getHttpSolrServer() {
//        1、创建solrserver对象：
        try {
             solr = new HttpSolrServer("http://localhost:8983/solr");
            solr.setConnectionTimeout(100);
            solr.setDefaultMaxConnectionsPerHost(100);
            solr.setMaxTotalConnections(100);
        } catch (Exception e) {
            System.out.println("请检查tomcat服务器或端口是否开启!");
            e.printStackTrace();
        }
    }

    public static String createCollection(String host,int port,String collection,int numShards,int maxShardsPerNode){
//       http://192.168.0.100:8080/solr/admin/collections?action=CREATE&name=collectionCrashCache&numShards=3&maxShardsPerNode=1
        String url = "http://"+host+":"+port+"/solr/admin/collections";
        String param = "action=CREATE&name="+collection+"&numShards="+numShards+"&maxShardsPerNode="+maxShardsPerNode;
        String s = HttpRequest.sendGet(url, param);
        return s;
    }

    public static String delCollection(String host,int port,String collection){
//       http://192.168.0.100:8080/solr/admin/collections?action=DELETE&name=collection
        String url = "http://"+host+":"+port+"/solr/admin/collections";
        String param = "action=DELETE&name="+collection;
        String s = HttpRequest.sendGet(url, param);
        return s;
    }

    public void addIndex(List<Item> list) {
//        2、添加索引
        Collection<SolrInputDocument> docs = new ArrayList<>();
        for (int i = 0; i < list.size(); i++) {
            Item item = list.get(i);
            //设置每个字段不得为空，可以在提交索引前进行检查
                SolrInputDocument doc = new SolrInputDocument();
                //在这里请注意date的格式，要进行适当的转化，上文已提到
                doc.addField("id", item.getId());
                docs.add(doc);
        }
        try {
            solr.add(docs);
            //对索引进行优化
            solr.optimize();
            solr.commit();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
//3、  使用bean对象添加索引
//    创建一个对应于solr索引的类别：
//
//    使用数据创建bean对象列表，
    public void addByBean(List beansList) {
        try {
            solr.addBeans(beansList);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                solr.optimize();
                solr.commit();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
//    索引提交完毕。
//
//            4、  删除索引
//    据查询结果删除：
    public void delIndex(List<String> ids) {
        try {
            //删除所有的索引
            solr.deleteByQuery("*:*");
            solr.commit();
        } catch (Exception e) {
            e.printStackTrace();
        }

//    根据索引号删除索引：
        try {
            solr.deleteById(ids);
            solr.commit();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
//5、  查询
//    SolrJ提供的查询功能比较强大，可以进行结果中查询、范围查询、排序等。
//    下面是笔者封装的一个查询函数：

    public  QueryResponse Search(String[] field, String[] key, int start,
                                       int count, String[] sortfield, Boolean[] flag, Boolean hightlight) {
        //检测输入是否合法
        if (null == field || null == key || field.length != key.length) {
            return null;
        }
        if (null == sortfield || null == flag || sortfield.length != flag.length) {
            return null;
        }

        SolrQuery query = null;
        try {
            //初始化查询对象
            query = new SolrQuery(field[0] + ":" + key[0]);
            for (int i = 0; i < field.length; i++) {
                query.addFilterQuery(field[i] + ":" + key[i]);
            }
            //设置起始位置与返回结果数
            query.setStart(start);
            query.setRows(count);
            //设置排序
            for(int i=0; i<sortfield.length; i++){
                if (flag[i]) {
//                    query.addSortField(sortfield[i], SolrQuery.ORDER.asc);
                } else {
//                    query.addSortField(sortfield[i], SolrQuery.ORDER.desc);
                }
            }
            //设置高亮
            if (null != hightlight) {
                query.setHighlight(true); // 开启高亮组件
                query.addHighlightField("title");// 高亮字段
                query.setHighlightSimplePre("<font color='red'>");// 标记
                query.setHighlightSimplePost("</font>");
                query.setHighlightSnippets(1);//结果分片数，默认为1
                query.setHighlightFragsize(1000);//每个分片的最大长度，默认为100
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        QueryResponse rsp = null;
        try {
            rsp = solr.query(query);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        //返回查询结果
        return rsp;
    }
//    补充一下范围查询的格式：[star t TO end]，start与end是相应数据格式的值的字符串形式，“TO”     一定要保持大写！

//            6、  读取查询结果
//    DocList读取返回结果：
    public void getResults(QueryResponse rsp) {
        SolrDocumentList solrList = rsp.getResults();

//    Beans读取返回结果：
        //Item即为上面定义的bean类
        List<Item> tmpLists = rsp.getBeans(Item.class);

//    读取高亮显示结果：

        Map<String, Map<String, List<String>>> hightlight = rsp.getHighlighting();
        for (int i = 0; i < tmpLists.size(); i++) {
            //hightlight的键为Item的id，值唯一，我们设置的高亮字段为title
            String hlString = hightlight.get(tmpLists.get(i).getId()).get("title").toString();
            if (null != hlString) {
                System.out.println(hlString);
            }
        }
    }
//7、  Facet的一个应用：自动补全
    //prefix为前缀，min为最大返回结果数
    public  String[] autoComplete(String prefix, int min) {
        String words[] = null;
        StringBuffer sb = new StringBuffer("");
        SolrQuery query = new SolrQuery("*.*");
        QueryResponse rsp= new QueryResponse();
        //Facet为solr中的层次分类查询
        try {
            query.setFacet(true);
            query.setQuery("*:*");
            query.setFacetPrefix(prefix);
            query.addFacetField("title");
            rsp = solr.query(query);
        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
            return null;
        }

        if(null != rsp){
            FacetField ff = rsp.getFacetField("title");
            List<FacetField.Count> countList = ff.getValues();
            if(null == countList){
                return null;
            }
            for(int i=0; i<countList.size(); i++){
                String tmp[] = countList.get(i).toString().split(" ");
                //排除单个字
                if(tmp[0].length()< 2){
                    continue;
                }
                sb.append(tmp[0] + " ");
                min--;
                if(min == 0){
                    break;
                }
            }
            words = sb.toString().split(" ");
        }else{
            return null;
        }
        return words;
    }


    public static void main(String[] args) throws Exception {
//        ModifiableSolrParams params = new ModifiableSolrParams();
//        params.set(HttpClientUtil.PROP_MAX_CONNECTIONS, 1000);//10  集群支持的最大连接数
//        params.set(HttpClientUtil.PROP_MAX_CONNECTIONS_PER_HOST, 500);//5  每台服务器的最大连接数
//        HttpClient client = HttpClientUtil.createClient(params);
//        LBHttpSolrServer lbServer = new LBHttpSolrServer(client);
        Item it = new Item();
        it.setId("9528");
        List<Item> list = new ArrayList<>();
        list.add(it);
        CloudSolrServer hss=new CloudSolrServer("10.20.4.160:2181,10.20.4.161:2181,10.20.4.162:2181/solr");
        Collection<SolrInputDocument> docs = new ArrayList<>();
        for (int i = 0; i < list.size(); i++) {
            Item item = list.get(i);
            //设置每个字段不得为空，可以在提交索引前进行检查
            SolrInputDocument doc = new SolrInputDocument();
            //在这里请注意date的格式，要进行适当的转化，上文已提到
            doc.addField("id", item.getId());
            docs.add(doc);
        }



        hss.setDefaultCollection("spark_test");
        SolrInputDocument solrInputDocument=new SolrInputDocument();
        solrInputDocument.addField("id", 1);
        solrInputDocument.addField("uname", "zhangsan");
        solrInputDocument.addField("sex", "Male");
        solrInputDocument.addField("age", 1);

        solrInputDocument.addField("hobby", "basketball");

        hss.add(docs);
        hss.optimize();
        hss.commit();
//        String s = createCollection("devdn01",8983,"spark_test",3,1);
////        String s = delCollection("devdn01", 8983, "spark_test");
//        System.out.println(s);
    }
}
