package com.wjj.jdbc.schedule;

import org.elasticsearch.client.Client;

/**
 * @author wangjiajun
 * @date 2017/9/16 11:56
 */
public class ESClient {
    private Client client;
    public ESClient(Client client){
        this.client = client;
    }
    public Client getClient(){
        return this.client;
    }
    public void setClient(Client client){
        this.client = client;
    }
    public void close(){
        this.client.close();
    }
}
