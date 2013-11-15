package com.flipkart.phantom.mysql.impl;

import com.flipkart.phantom.task.spi.RequestWrapper;

import java.util.ArrayList;
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


    /** flag to determine the state of request */
    private int flag;


    /** Buffer */
    private ArrayList<byte[]> buffer;

    /** Start Getter/Setter methods */

    public int getFlag() {
        return this.flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }


    public ArrayList<byte[]> getBuffer() {
        return buffer;
    }

    public void setBuffer(ArrayList<byte[]> buffer) {
        this.buffer = buffer;
    }

/**End Getter/Setter methods */

}