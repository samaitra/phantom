package com.flipkart.phantom.mysql.impl;

import com.flipkart.phantom.task.spi.RequestWrapper;
import java.util.ArrayList;


/**
 * <code>MysqlRequestWrapper</code> has the the Mysql request buffer wrapped in ArrayList object.
 * @author : samaitra
 * @version : 1.0
 * @date : 15/11/13
 *
 */
public class MysqlRequestWrapper implements RequestWrapper{


    /** flag to determine the state of request */
    private int flag;


    /** Mysql request buffer wrapped in ArrayList object */
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