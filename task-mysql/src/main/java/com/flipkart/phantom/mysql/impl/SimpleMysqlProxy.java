package com.flipkart.phantom.mysql.impl;

import java.sql.ResultSet;

/**
 * Created with IntelliJ IDEA.
 * User: saikat
 * Date: 03/11/13
 * Time: 9:03 PM
 * To change this template use File | Settings | File Templates.
 */
public class SimpleMysqlProxy extends MysqlProxy {

    /**
     * Abstract method implementation
     * @see com.flipkart.phantom.mysql.impl.MysqlProxy#fallbackRequest(String, byte[])
     */
    @Override
    public ResultSet fallbackRequest(String uri, byte[] data) {
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
