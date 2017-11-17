package cn.lsmsp.sparkframe.dbopt.solr;

import org.apache.solr.client.solrj.beans.Field;

public class Item {
        @Field
        private String id;

        public void setId(String id) {
            this.id = id;
        }
        public String getId() {
            return id;
        }

        public Item(){
        }
    }
