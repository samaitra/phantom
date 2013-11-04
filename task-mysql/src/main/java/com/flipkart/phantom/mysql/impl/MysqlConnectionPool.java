package com.flipkart.phantom.mysql.impl;

import com.mysql.jdbc.PreparedStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.Semaphore;

/**
 * Created with IntelliJ IDEA.
 * User: saikat
 * Date: 04/11/13
 * Time: 10:41 AM
 * To change this template use File | Settings | File Templates.
 */
public class MysqlConnectionPool {

    private static Logger logger = LoggerFactory.getLogger(MysqlConnectionPool.class);

    /** The Java Sql Connection */
    private Connection conn;

    /** Host to connect to */
    private String host = "localhost";

    /** port to connect to */
    private Integer port = 3306;

    /** The database url */
    private String dbUrl;

    /** The database name */
    private String dbname = "automation";

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

    /**
     * Initialize the connection pool
     */
    public void initConnectionPool() {

        this.dbUrl = "jdbc:mysql://" + host + ":" + port + "/" + dbname + "?autoReconnect=true";


        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            this.conn = DriverManager.getConnection(dbUrl, username, password);


        } catch (Exception e) {
            System.out.println("Could not connect to database.");
            e.printStackTrace();

        }
    }

    /**
     * Method to execute a request
     * @return response ResultSet object
     */
    public ResultSet execute(String query) throws Exception {
        logger.debug("Sending request: "+query);
        if (processQueue.tryAcquire()) {
            ResultSet response;
            try {
                PreparedStatement ps = (PreparedStatement) conn.prepareStatement(query);

                response = ps.executeQuery(query);
            } catch (Exception e) {
                processQueue.release();
                throw e;
            }
            processQueue.release();
            return response;
        } else {
            throw new Exception("Process queue full!");
        }
    }


    /** shutdown the client connections */
    public void shutdown() {
        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
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
