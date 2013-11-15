package com.flipkart.phantom.mysql.impl;

import com.github.jmpjct.mysql.proto.Flags;
import com.github.jmpjct.mysql.proto.Packet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.concurrent.Semaphore;

/**
 * <code>MysqlDriver</code> does the mysql connection management for Mysql proxy requests and writes mysql requests to Mysql server socket.
 *
 * @author : samaitra
 * @version : 1.0
 * @date : 15/11/13
 */
public class MysqlDriver {

    private static Logger logger = LoggerFactory.getLogger(MysqlDriver.class);

    /** The Java Sql Connection */
    private Connection conn;

    /** Host to connect to */
    public String host = "localhost";

    /** port to connect to */
    public int port = 3306;

    /** The database url */
    private String dbUrl;

    /** The database name */
    private String dbname = "test";

    /** The database username */
    private String username = "root";

    /** The database password */
    private String password = "";

    /** connection timeout in milis */
    private int connectionTimeout = 1000;

    /** socket timeout in milis */
    private int operationTimeout = 1000;

    /** max number of connections allowed */
    private int maxConnections = 20;

    /** max size of request queue */
    private int requestQueueSize = 0;

    /** the semaphore to separate the process queue */
    private Semaphore processQueue;

    /** mysql socket to connect mysql server */
    public Socket mysqlSocket = null;

    /** mysql socket input stream */

    public InputStream mysqlIn = null;

    /** mysql socket output stream */

    public OutputStream mysqlOut = null;
    /**
     * Initialize mysql socket connection
     */
    public void initConnection() throws Exception{

        try {

            this.mysqlSocket = new Socket(this.host, this.port);
            this.mysqlSocket.setPerformancePreferences(0, 2, 1);
            this.mysqlSocket.setTcpNoDelay(true);
            this.mysqlSocket.setTrafficClass(0x10);
            this.mysqlSocket.setKeepAlive(true);

            logger.debug("Connected to mysql server at "+this.host+":"+this.port);
            this.mysqlIn = new BufferedInputStream(this.mysqlSocket.getInputStream(), 16384);
            this.mysqlOut = this.mysqlSocket.getOutputStream();

        } catch (Exception e) {
            throw e;
        }
    }

    /**
     * Method to execute a request
     * @return InputSteam object
     */
    public InputStream execute(int flag,ArrayList<byte[]> buffer) throws Exception {

        switch (flag){
            case Flags.MODE_INIT:
                initConnection();
                break;
            case Flags.MODE_SEND_AUTH:
                Packet.write(this.mysqlOut, buffer);
                break;
            case Flags.MODE_SEND_QUERY:
                Packet.write(this.mysqlOut,buffer);
               break;
            default:
                break;

        }

        return this.mysqlIn;
        }

    /** shutdown the socket connections */
    public void shutdown() {
        if (this.mysqlSocket == null) {
            return;
        }

        try {
            this.mysqlSocket.close();
        }
        catch(IOException e) {}
    }

    /** Getters / Setters */

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public String getDbname() {
        return dbname;
    }

    public void setDbname(String dbname) {
        this.dbname = dbname;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    public void setConnectionTimeout(int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }

    public int getOperationTimeout() {
        return operationTimeout;
    }

    public void setOperationTimeout(int operationTimeout) {
        this.operationTimeout = operationTimeout;
    }

    public int getMaxConnections() {
        return maxConnections;
    }

    public void setMaxConnections(int maxConnections) {
        this.maxConnections = maxConnections;
    }

    public int getRequestQueueSize() {
        return requestQueueSize;
    }

    public void setRequestQueueSize(int requestQueueSize) {
        this.requestQueueSize = requestQueueSize;
    }
    /** Getters / Setters */


}
