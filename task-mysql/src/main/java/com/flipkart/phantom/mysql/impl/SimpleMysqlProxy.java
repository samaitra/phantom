package com.flipkart.phantom.mysql.impl;

import java.io.InputStream;
import java.sql.ResultSet;
import java.util.ArrayList;

/**
 * A simple implementation for the MysqlProxy abstract class
 * @author : samaitra
 * @version : 1.0
 * @date : 15/11/13
 *
 */
public class SimpleMysqlProxy extends MysqlProxy {

    /**
     * Abstract method implementation
     * @see com.flipkart.phantom.mysql.impl.MysqlProxy#fallbackRequest(int,ArrayList<byte[]>)
     */
    @Override
    public InputStream fallbackRequest(int flag , ArrayList<byte[]> buffer) {
        return null;
    }

    /**
     * Abstract method implementation
     * @return String Group name
     */
    @Override
    public String getGroupKey() {
        return "SimpleMysqlProxy";
    }

    /**
     * Abstract method implementation
     * @return String Command name
     */
    @Override
    public String getCommandKey() {
        return this.getName() + "MysqlPool";
    }

    /**
     * Abstract method implementation
     * @return String Thread pool name
     */
    @Override
    public String getThreadPoolKey() {
        return "SimpleMysqlThreadPool";
    }

}
