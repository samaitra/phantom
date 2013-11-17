package com.flipkart.phantom.mysql.impl;

import com.flipkart.phantom.task.spi.AbstractHandler;
import com.flipkart.phantom.task.spi.Executor;
import com.flipkart.phantom.task.spi.TaskContext;
import com.github.jmpjct.mysql.proto.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;

/**
 * Created with IntelliJ IDEA.
 * User: saikat
 * Date: 16/11/13
 * Time: 12:30 PM
 * To change this template use File | Settings | File Templates.
 */
public abstract class MysqlProxy2 extends AbstractHandler {

    private static Logger logger = LoggerFactory.getLogger(MysqlProxy2.class);

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
    private int threadPoolSize = MysqlProxy2.DEFAULT_THREAD_POOL_SIZE;

    /**
     * socket timeout in milis
     */
    private int operationTimeout = 10000;

    ArrayList<byte[]> buffer;
    Connection conn= null;
    /**
     * Init hook provided by the MysqlProxy
     */
    public void init(TaskContext context) throws Exception {

    }

    public void initConnection() throws Exception {
    //TODO Need to create a jdbc connection pool

        try {

            String dburl = "jdbc:mysql://"+this.host+":"+this.port+"/mysql?characterEncoding=utf8&user=root&password=";
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            conn = DriverManager.getConnection(dburl);

            if(conn!=null){

                logger.info("Database connection successfully established.");
            }

        }catch (Exception e){
            e.printStackTrace();
        }


    }


    /**
     * Shutdown hooks provided by the MysqlProxy
     */
    public void shutdown(TaskContext context) throws Exception {

    }

    /**
     * The main method which makes the Mysql request
     */

    public InputStream doRequest(int flag, ArrayList<byte[]> buffer) throws Exception {

        initConnection();
        InputStream is = null;

        switch (flag) {
            case Flags.MODE_SEND_QUERY:
                String query = Com_Query.loadFromPacket(buffer.get(0)).query;
                logger.info("Query to mysql from proxy :" + query);
                Statement s = conn.createStatement();
                java.sql.ResultSet rs = s.executeQuery(query);

                if(rs.next()){
                   is = rs.getBinaryStream(1);
                }
                break;
            default:
                break;

        }
        //TODO Decide whether to return ResultSet or InputStream
        return is;
    }

    /**
     * Abstract fallback request method
     *
     * @param flag Flag Mysql request state
     * @return ResultSet response after executing the fallback
     */
    public abstract InputStream fallbackRequest(int flag, ArrayList<byte[]> buffer);

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
