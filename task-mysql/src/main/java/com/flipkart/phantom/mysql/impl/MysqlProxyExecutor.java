package com.flipkart.phantom.mysql.impl;

import com.flipkart.phantom.task.spi.Executor;
import com.flipkart.phantom.task.spi.RequestWrapper;
import com.flipkart.phantom.task.spi.TaskContext;
import com.netflix.hystrix.*;

import java.io.InputStream;
import java.sql.ResultSet;
import java.util.ArrayList;

/**
 * Created with IntelliJ IDEA.
 * User: saikat
 * Date: 31/10/13
 * Time: 5:40 PM
 * To change this template use File | Settings | File Templates.
 */
public class MysqlProxyExecutor extends HystrixCommand<InputStream> implements Executor {


    /** request mode flag */
    int flag;

    /** data */
    byte[] data;

    ArrayList<byte[]> buffer;

    /** the proxy client */
    private MysqlProxy proxy;

    /** current task context */
    private TaskContext taskContext;

    /** only constructor uses the proxy client, task context and the mysql requestWrapper */
    public MysqlProxyExecutor(MysqlProxy proxy, TaskContext taskContext, RequestWrapper requestWrapper) {
        super(
                HystrixCommand.Setter.withGroupKey(HystrixCommandGroupKey.Factory.asKey(proxy.getGroupKey()))
                        .andCommandKey(HystrixCommandKey.Factory.asKey(proxy.getCommandKey()))
                        .andThreadPoolKey(HystrixThreadPoolKey.Factory.asKey(proxy.getThreadPoolKey()))
                        .andThreadPoolPropertiesDefaults(HystrixThreadPoolProperties.Setter().withCoreSize(proxy.getThreadPoolSize()))
                        .andCommandPropertiesDefaults(HystrixCommandProperties.Setter().withExecutionIsolationThreadTimeoutInMilliseconds(proxy.getDriver().getOperationTimeout()))
        );

        this.proxy = proxy;
        this.taskContext = taskContext;

        /** Get the Mysql Request */
        MysqlRequestWrapper mysqlRequestWrapper = (MysqlRequestWrapper) requestWrapper;

        /** get necessary data required for the output */
        this.flag = mysqlRequestWrapper.getFlag();
        this.buffer = mysqlRequestWrapper.getBuffer();
    }
    /**
     * Interface method implementation
     * @return response ResultSet for the give request
     * @throws Exception
     */
    @Override
    protected InputStream run() throws Exception {
        return proxy.doRequest(flag,buffer);
    }

    /**
     * Interface method implementation
     * @return response ResultSet from the fallback
     * @throws Exception
     */
    @Override
    protected InputStream getFallback() {
        return proxy.fallbackRequest(flag,buffer);
    }

    /** Getter/Setter methods */
    public MysqlProxy getProxy() {
        return proxy;
    }
    /** End Getter/Setter methods */
}
