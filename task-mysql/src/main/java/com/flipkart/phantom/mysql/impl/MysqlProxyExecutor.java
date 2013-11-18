package com.flipkart.phantom.mysql.impl;

import com.flipkart.phantom.task.spi.Executor;
import com.flipkart.phantom.task.spi.RequestWrapper;
import com.flipkart.phantom.task.spi.TaskContext;
import com.github.jmpjct.mysql.proto.Flags;
import com.netflix.hystrix.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.ArrayList;

/**
 * Implements the HystrixCommand class for executing HTTP proxy requests
 * @author : samaitra
 * @version : 1.0
 * @date : 15/11/13
 */
public class MysqlProxyExecutor extends HystrixCommand<InputStream> implements Executor {

    private static final Logger LOGGER = LoggerFactory.getLogger(MysqlProxyExecutor.class);

    /** request mode flag */
    int flag;

    /** data */
    byte[] data;

    ArrayList<byte[]> buffer;

    ArrayList<ArrayList<byte[]>> connRefBytes;

    /** the proxy client */
    private MysqlProxy proxy;

    public int mode = Flags.MODE_INIT;

    /** current task context */
    private TaskContext taskContext;

    /** only constructor uses the proxy client, task context and the mysql requestWrapper */
    public MysqlProxyExecutor(MysqlProxy proxy, TaskContext taskContext, RequestWrapper requestWrapper) {
        super(
                HystrixCommand.Setter.withGroupKey(HystrixCommandGroupKey.Factory.asKey(proxy.getGroupKey()))
                        .andCommandKey(HystrixCommandKey.Factory.asKey(proxy.getCommandKey()))
                        .andThreadPoolKey(HystrixThreadPoolKey.Factory.asKey(proxy.getThreadPoolKey()))
                        .andThreadPoolPropertiesDefaults(HystrixThreadPoolProperties.Setter().withCoreSize(proxy.getThreadPoolSize()))
                        .andCommandPropertiesDefaults(HystrixCommandProperties.Setter().withExecutionIsolationThreadTimeoutInMilliseconds(proxy.getOperationTimeout()))
        );
        this.proxy = proxy;
        this.taskContext = taskContext;

        /** Get the Mysql Request */
        MysqlRequestWrapper mysqlRequestWrapper = (MysqlRequestWrapper) requestWrapper;

        /** get necessary data required for the output */
        this.flag = mysqlRequestWrapper.getFlag();
        this.buffer = mysqlRequestWrapper.getBuffer();
        this.connRefBytes = mysqlRequestWrapper.getConnRefBytes();
    }
    /**
     * Interface method implementation
     * @return response ResultSet for the give request
     * @throws Exception
     */
    @Override
    public InputStream run() {
        try{
        return this.proxy.doRequest(flag,buffer,connRefBytes);

        }catch(Exception e){
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Interface method implementation
     * @return response ResultSet from the fallback
     * @throws Exception
     */
    @Override
    protected InputStream getFallback() {
        return this.proxy.fallbackRequest(flag,buffer);
    }

    /** Getter/Setter methods */
    public MysqlProxy getProxy() {
        return proxy;
    }
    /** End Getter/Setter methods */
}
