package cn.lsmsp.sparkframe.common.util;

/**
 * Created by wangcongjun on 2017/6/12.
 */
public class RestUtils {
    public static String post(String url,String param){
        return HttpRequest.sendPost(url,param);
    }
    public static String get(String url,String param){
       return  HttpRequest.sendGet(url,param);
    }
    public static void main(String[] args) {
        HttpRequest.sendPost("http://10.20.1.32/spark/test","data=9527");
    }
}
