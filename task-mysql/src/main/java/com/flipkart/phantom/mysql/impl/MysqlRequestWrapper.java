package com.flipkart.phantom.mysql.impl;

import com.flipkart.phantom.task.spi.RequestWrapper;

import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: saikat
 * Date: 31/10/13
 * Time: 5:41 PM
 * To change this template use File | Settings | File Templates.
 */
public class MysqlRequestWrapper implements RequestWrapper{

    /** Data */
    private byte[] data;

    /** uri */
    private String uri;

    /** Start Getter/Setter methods */

    public byte[] getData(){
        return data;
    }

    public void setData(byte[] data){
        this.data = data;
    }


    public String getUri(){
        return uri;
    }

    public void setUri(String uri){
        this.uri = uri;
    }

/**End Getter/Setter methods */

}