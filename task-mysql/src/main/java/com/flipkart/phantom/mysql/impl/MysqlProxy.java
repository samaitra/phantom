package com.flipkart.phantom.mysql.impl;

import com.flipkart.phantom.task.spi.AbstractHandler;
import com.flipkart.phantom.task.spi.TaskContext;
import com.github.jmpjct.mysql.proto.*;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

/**
 /**
 * Abstract class for handling Mysql proxy requests
 *
 * @author : samaitra
 * @version : 1.0
 * @date : 15/11/13
 */

public abstract class MysqlProxy extends AbstractHandler {

    private static Logger logger = LoggerFactory.getLogger(MysqlProxy.class);

    private long sequenceId;

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    /**
     * Host to connect to
     */
    public String host = "localhost";

    /**
     * port to connect to
     */
    public int port = 3306;

    /**
     * mysql socket to connect mysql server
     */
    public Socket mysqlSocket = null;

    /**
     * mysql socket input stream
     */

    public InputStream mysqlIn = null;

    /**
     * mysql socket output stream
     */

    public OutputStream mysqlOut = null;

    /**
     * The default thread pool size
     */
    public static final int DEFAULT_THREAD_POOL_SIZE = 10;

    /**
     * Name of the proxy
     */
    private String name;

    /**
     * The thread pool size for this proxy
     */
    private int threadPoolSize = MysqlProxy.DEFAULT_THREAD_POOL_SIZE;

    /**
     * socket timeout in milis
     */
    private int operationTimeout = 10000;

    ArrayList<byte[]> buffer;
    Connection conn= null;

    /** Properties for initializing Generic Object Pool */
    private int poolSize =10;
    private long maxWait = 100;
    private int maxIdle = poolSize;
    private int minIdle = poolSize/2;
    private long timeBetweenEvictionRunsMillis = 20000;

    /** The GenericObjectPool object */
    private GenericObjectPool<MysqlConnection> mysqlConnectionPool;


    /** The Mysql Connection pool map */
    private ConcurrentHashMap<String,GenericObjectPool<MysqlConnection>> mysqlConnectionPoolMap = new ConcurrentHashMap<String, GenericObjectPool<MysqlConnection>>();


    @Override
    public void init(TaskContext context) throws Exception {

    }

    @Override
    public void shutdown(TaskContext context) throws Exception {

    }


    public void initConnectionPool(String connectionPoolKey, ArrayList<ArrayList<byte[]>> connRefBytes){

        //Create pool
        this.mysqlConnectionPool = new GenericObjectPool<MysqlConnection>(
                new MysqlConnectionObjectFactory(this,connRefBytes),
                this.poolSize,
                GenericObjectPool.WHEN_EXHAUSTED_GROW,
                this.maxWait ,
                this.maxIdle ,
                this.minIdle , false, false,
                this.timeBetweenEvictionRunsMillis,
                GenericObjectPool.DEFAULT_NUM_TESTS_PER_EVICTION_RUN,
                GenericObjectPool.DEFAULT_MIN_EVICTABLE_IDLE_TIME_MILLIS,
                true);

        // Add connection pool object in mysql connection pool map.
        this.mysqlConnectionPoolMap.put(connectionPoolKey,this.mysqlConnectionPool);
    }

    /**
     * The main method which makes the Mysql request
     */

    public InputStream doRequest(MysqlRequestWrapper mysqlRequestWrapper) throws Exception {


        ArrayList<byte[]> buffer = mysqlRequestWrapper.getBuffer();
        ArrayList<ArrayList<byte[]>> connRefBytes = mysqlRequestWrapper.getConnRefBytes();

        //extracting user credentials as key for mysql connection pool map.
        String connectionPoolKey = new String(connRefBytes.get(0).get(0));

        if(this.mysqlConnectionPoolMap.get(connectionPoolKey) == null){
            initConnectionPool(connectionPoolKey,connRefBytes);
        }

        MysqlConnection mysqlConnection = this.mysqlConnectionPoolMap.get(connectionPoolKey).borrowObject();
        String query = Com_Query.loadFromPacket(buffer.get(0)).query;

        //logger.info("Query to mysql from proxy :" + query);
        Packet.write(mysqlConnection.mysqlOut, buffer);

        return mysqlConnection.mysqlIn;
    }


    /**
     * Abstract fallback request method
     *
     * @param mysqlRequestWrapper  Mysql request
     * @return ResultSet response after executing the fallback
     */
    public abstract InputStream fallbackRequest(MysqlRequestWrapper mysqlRequestWrapper);

    /**
     * Abstract method which gives group key
     *
     * @return String group key
     */
    public abstract String getGroupKey();

    /**
     * Abstract method which gives command name
     *
     * @return String command name
     */
    public abstract String getCommandKey();

    /**
     * Abstract method which gives the thread pool name
     *
     * @return String thread pool name
     */
    public abstract String getThreadPoolKey();

    /**
     * Returns the thread pool size
     *
     * @return thread pool size
     */
    public int getThreadPoolSize() {
        return this.threadPoolSize;
    }

    public abstract int getOperationTimeout();

    /**
     * Abstract method implementation
     *
     * @see com.flipkart.phantom.task.spi.AbstractHandler#getDetails()
     */
    public String getDetails() {
        if (this != null) {
            String details = "Endpoint: ";
            details += "jdbc:mysql://" + this.getHost() + ":" + this.getPort() + "\n";
            return details;
        }
        return "No endpoint configured";
    }

    /**
     * Abstract method implementation
     *
     * @see AbstractHandler#getType()
     */
    @Override
    public String getType() {
        return "MysqlProxy";
    }

    /**
     * getters / setters
     */
    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    public void setThreadPoolSize(int threadPoolSize) {
        this.threadPoolSize = threadPoolSize;
    }

    public void setOperationTimeout(int operationTimeout) {
        this.operationTimeout = operationTimeout;
    }
    /** getters / setters */


}
