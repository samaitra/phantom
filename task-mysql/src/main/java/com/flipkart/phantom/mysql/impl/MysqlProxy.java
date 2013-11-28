package com.flipkart.phantom.mysql.impl;

import com.flipkart.phantom.task.spi.AbstractHandler;
import com.flipkart.phantom.task.spi.TaskContext;
import com.github.jmpjct.mysql.proto.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.sql.Connection;
import java.util.ArrayList;

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
    /**
     * Init hook provided by the MysqlProxy
     */
    public void init(TaskContext context) throws Exception {

    }

    public MysqlConnection initConnection(ArrayList<ArrayList<byte[]>> connRefBytes) throws Exception {


        MysqlConnection mysqlConnection = new MysqlConnection(this.host,this.port,connRefBytes);


//        //TODO Need to create a connection pool
//
//
//        try {
//
//            this.mysqlSocket = new Socket(this.host, this.port);
//            this.mysqlSocket.setPerformancePreferences(0, 2, 1);
//            this.mysqlSocket.setTcpNoDelay(true);
//            this.mysqlSocket.setTrafficClass(0x10);
//            this.mysqlSocket.setKeepAlive(true);
//            logger.info("Connected to mysql server at "+this.host+":"+this.port);
//            this.mysqlIn = new BufferedInputStream(this.mysqlSocket.getInputStream(), 16384);
//            this.mysqlOut = this.mysqlSocket.getOutputStream();
//
//        } catch (Exception e) {
//            throw e;
//        }
//
//        /* I am assuming an successful connection by replaying the client connRefBytes. There is a possibility
//        that in this phase there are error in mysql connection and requests may not get handled by the proxy.
//        Need to handle scenarios when proxy connection fails.
//        */
//
//        byte[] packet = Packet.read_packet(this.mysqlIn);
//        int c = 0;
//        for(ArrayList<byte[]> buf : connRefBytes){
//            logger.info("connRefBytes : "+new String(buf.get(0)));
//            Packet.write(this.mysqlOut, buf);
//
//            if(c>0){
//
//                /*
//                Writing connection queries responses in a client out file. This is to clear the Mysql Input Stream
//                for establishing connection.
//                */
//
//                boolean bufferResultSet = false;
//
//                File f = new File("client_out.log");
//                if (!f.exists()) {
//                    f.createNewFile();
//                }
//
//                OutputStream clientOut = new FileOutputStream(f);
//
//                packet = Packet.read_packet(this.mysqlIn);
//                this.buffer.add(packet);
//                this.sequenceId = Packet.getSequenceId(packet);
//
//                switch (Packet.getType(packet)) {
//                    case Flags.OK:
//                    case Flags.ERR:
//                        break;
//
//                    default:
//                        this.buffer = Packet.read_full_result_set(this.mysqlIn, clientOut, this.buffer, bufferResultSet);
//                        break;
//                }
//            }else{
//                packet = Packet.read_packet(this.mysqlIn);
//                this.buffer = new ArrayList<byte[]>();
//                this.buffer.add(packet);
//
//                if (Packet.getType(packet) != Flags.OK) {
//                    logger.debug("Auth is not okay!");
//                }
//            }
//            c++;
//
//        }

        return mysqlConnection;

    }


    /**
     * Shutdown hooks provided by the MysqlProxy
     */
    public void shutdown(TaskContext context) throws Exception {

    }

    /**
     * The main method which makes the Mysql request
     */

    public InputStream doRequest(int flag, ArrayList<byte[]> buffer, ArrayList<ArrayList<byte[]>> connRefBytes) throws Exception {

        MysqlConnection mysqlConnection = initConnection(connRefBytes);
        String query = Com_Query.loadFromPacket(buffer.get(0)).query;
        logger.info("Query to mysql from proxy :" + query);
        Packet.write(mysqlConnection.mysqlOut, buffer);

        return mysqlConnection.mysqlIn;
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
