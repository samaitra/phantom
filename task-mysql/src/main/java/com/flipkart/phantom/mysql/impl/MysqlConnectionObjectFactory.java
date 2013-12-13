package com.flipkart.phantom.mysql.impl;

import org.apache.commons.pool.PoolableObjectFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;


/**
 * Created by saikat on 13/12/13.
 */
public class MysqlConnectionObjectFactory implements PoolableObjectFactory<MysqlConnection> {

    /** Mysql Proxy instance for initializing the Factory */
    private MysqlProxy mysqlProxy;


    private ArrayList<ArrayList<byte[]>> connRefBytes;

    /**
     * Constructor for initializing this Factory with a MysqlProxy
     *
     */
    public MysqlConnectionObjectFactory(MysqlProxy mysqlProxy,ArrayList<ArrayList<byte[]>> connRefBytes) {
        this.setMysqlProxy(mysqlProxy);
        this.setConnRefBytes(connRefBytes);
    }


    /** Logger for this class*/
    private static final Logger LOGGER = LoggerFactory.getLogger(MysqlConnectionObjectFactory.class);

    public MysqlConnection makeObject() throws Exception {
       return new MysqlConnection(mysqlProxy.getHost(),mysqlProxy.getPort(),this.connRefBytes);
    }

    public void destroyObject(MysqlConnection conn) throws Exception {
        LOGGER.info("Closing a mysql connection for server : {} at port : {}", this.getMysqlProxy().getHost(), this.getMysqlProxy().getPort());
        conn.mysqlSocket.close();
    }

    /**
     * Interface method implementation. Does nothing
     * @see org.apache.commons.pool.PoolableObjectFactory#activateObject(Object)
     */
    public boolean validateObject(MysqlConnection conn) {
    //TODO implement validateObject
    return true;
    }

    /**
     * Interface method implementation. Does nothing
     * @see org.apache.commons.pool.PoolableObjectFactory#activateObject(Object)
     */
    public void activateObject(MysqlConnection conn) throws Exception {
        // no op
    }

    /**
     * Interface method implementation. Does nothing
     * @see org.apache.commons.pool.PoolableObjectFactory#passivateObject(Object)
     */
    public void passivateObject(MysqlConnection conn) throws Exception {
        // no op
    }


    /** Getter/Setter Methods */

    public MysqlProxy getMysqlProxy() {
        return mysqlProxy;
    }

    public void setMysqlProxy(MysqlProxy mysqlProxy) {
        this.mysqlProxy = mysqlProxy;
    }

    public ArrayList<ArrayList<byte[]>> getConnRefBytes() {
        return connRefBytes;
    }

    public void setConnRefBytes(ArrayList<ArrayList<byte[]>> connRefBytes) {
        this.connRefBytes = connRefBytes;
    }

    /** End Getter/Setter Methods */
}
