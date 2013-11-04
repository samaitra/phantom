package com.flipkart.phantom.mysql.impl;

import com.flipkart.phantom.task.spi.AbstractHandler;
import com.flipkart.phantom.task.spi.TaskContext;

import java.sql.ResultSet;

/**
 * Created with IntelliJ IDEA.
 * User: saikat
 * Date: 31/10/13
 * Time: 5:39 PM
 * To change this template use File | Settings | File Templates.
 */
public abstract class MysqlProxy extends AbstractHandler{
    /** The default thread pool size*/
    public static final int DEFAULT_THREAD_POOL_SIZE = 10;

    /** Name of the proxy */
    private String name;

    /** The connection pool implementation instance */
    private MysqlConnectionPool pool;

    /** The thread pool size for this proxy*/
    private int threadPoolSize = MysqlProxy.DEFAULT_THREAD_POOL_SIZE;

    /**
     *  Init hook provided by the MysqlProxy
     */
    public void init(TaskContext context) throws Exception {
        if (pool == null) {
            throw new AssertionError("MysqlConnectionPool object 'pool' must be given");
        } else {
            pool.initConnectionPool();
        }
    }

    /**
     * Shutdown hooks provided by the MysqlProxy
     */
    public void shutdown(TaskContext context) throws Exception {
        pool.shutdown();
    }

    /**
     * The main method which makes the Mysql request
     */
    public ResultSet doRequest(String uri, byte[] data) throws Exception {
        return pool.execute(uri);
    }

    /**
     * Abstract fallback request method
     * @param uri String Mysql request URI
     * @param data byte[] Mysql request payload
     * @return ResultSet response after executing the fallback
     */
    public abstract ResultSet fallbackRequest(String uri, byte[] data);

    /**
     * Abstract method which gives group key
     * @return String group key
     */
    public abstract String getGroupKey();

    /**
     * Abstract method which gives command name
     * @return String command name
     */
    public abstract String getCommandKey();

    /**
     * Abstract method which gives the thread pool name
     * @return String thread pool name
     */
    public abstract String getThreadPoolKey();

    /**
     * Returns the thread pool size
     * @return thread pool size
     */
    public int getThreadPoolSize() {
        return this.threadPoolSize;
    }

    /**
     * Abstract method implementation
     * @see com.flipkart.phantom.task.spi.AbstractHandler#getDetails()
     */
    public String getDetails() {
        if (pool != null) {
            String details = "Endpoint: ";
            details += "jdbc:mysql://" + pool.getHost() + ":" + pool.getPort() + "\n";
            details += "Connection Timeout: " + pool.getConnectionTimeout() + "ms\n";
            details += "Operation Timeout: " + pool.getOperationTimeout() + "ms\n";
            details += "Max Connections: " + pool.getMaxConnections() + "\n";
            details += "Request Queue Size: " + pool.getRequestQueueSize() + "\n";
            return details;
        }
        return "No endpoint configured";
    }

    /**
     * Abstract method implementation
     * @see AbstractHandler#getType()
     */
    @Override
    public String getType() {
        return "MysqlProxy";
    }

    /** getters / setters */
    public void setName(String name) {
        this.name = name;
    }
    public String getName() {
        return this.name;
    }
    public MysqlConnectionPool getPool() {
        return pool;
    }
    public void setPool(MysqlConnectionPool pool) {
        this.pool = pool;
    }
    public void setThreadPoolSize(int threadPoolSize) {
        this.threadPoolSize = threadPoolSize;
    }


    /** getters / setters */




}
