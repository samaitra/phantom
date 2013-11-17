package com.flipkart.phantom.mysql.impl;

import com.flipkart.phantom.task.spi.AbstractHandler;
import com.flipkart.phantom.task.spi.TaskContext;

import java.io.InputStream;
import java.util.ArrayList;

/**
 * Abstract class for handling Mysql proxy requests
 *
 * @author : samaitra
 * @version : 1.0
 * @date : 15/11/13
 */
public abstract class MysqlProxy extends AbstractHandler{
    /** The default thread pool size*/
    public static final int DEFAULT_THREAD_POOL_SIZE = 10;

    /** Name of the proxy */
    private String name;

    /** The Mysql Driver implementation instance */
    private MysqlDriver driver;

    /** The thread pool size for this proxy*/
    private int threadPoolSize = MysqlProxy.DEFAULT_THREAD_POOL_SIZE;

    /**
     *  Init hook provided by the MysqlProxy
     */
    public void init(TaskContext context) throws Exception {
        if (this.driver == null) {
            throw new AssertionError("MysqlDriver object must be given");
        } else {
            this.driver.initConnection();
        }
    }

    /**
     * Shutdown hooks provided by the MysqlProxy
     */
    public void shutdown(TaskContext context) throws Exception {
        this.driver.shutdown();
    }

    /**
     * The main method which makes the Mysql request
     */
    public InputStream doRequest(int flag,ArrayList<byte[]> buffer) throws Exception {

        return this.driver.execute(flag,buffer);
    }

    /**
     * Abstract fallback request method
     * @param flag Flag Mysql request state
     * @return ResultSet response after executing the fallback
     */
    public abstract InputStream fallbackRequest(int flag, ArrayList<byte[]> buffer);

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
        if (this.driver != null) {
            String details = "Endpoint: ";
            details += "jdbc:mysql://" + driver.getHost() + ":" + driver.getPort() + "\n";
            details += "Connection Timeout: " + driver.getConnectionTimeout() + "ms\n";
            details += "Operation Timeout: " + driver.getOperationTimeout() + "ms\n";
            details += "Max Connections: " + driver.getMaxConnections() + "\n";
            details += "Request Queue Size: " + driver.getRequestQueueSize() + "\n";
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
    public MysqlDriver getDriver() {
        return this.driver;
    }
    public void setDriver(MysqlDriver pool) {
        this.driver = pool;
    }
    public void setThreadPoolSize(int threadPoolSize) {
        this.threadPoolSize = threadPoolSize;
    }


    /** getters / setters */




}
