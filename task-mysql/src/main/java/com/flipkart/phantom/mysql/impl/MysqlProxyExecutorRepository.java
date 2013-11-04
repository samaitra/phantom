package com.flipkart.phantom.mysql.impl;

import com.flipkart.phantom.mysql.impl.registry.MysqlProxyRegistry;
import com.flipkart.phantom.task.spi.Executor;
import com.flipkart.phantom.task.spi.RequestWrapper;
import com.flipkart.phantom.task.spi.TaskContext;
import com.flipkart.phantom.task.spi.registry.AbstractHandlerRegistry;
import com.flipkart.phantom.task.spi.repository.ExecutorRepository;

/**
 * Created with IntelliJ IDEA.
 * User: saikat
 * Date: 31/10/13
 * Time: 5:40 PM
 * To change this template use File | Settings | File Templates.
 */
public class MysqlProxyExecutorRepository implements ExecutorRepository {

    /** repository */
    private MysqlProxyRegistry registry;

    /** The TaskContext instance */
    private TaskContext taskContext;

    /**
     * Returns {@link com.flipkart.phantom.task.spi.Executor} for the specified requestWrapper
     * @param commandName command Name as specified by Executor. Not used in this case.
     * @param proxyName   proxyName the MysqlProxy name
     * @param requestWrapper requestWrapper Object containing requestWrapper Data
     * @return  an {@link MysqlProxyExecutor} instance
     */
    public Executor getExecutor (String commandName, String proxyName, RequestWrapper requestWrapper)  {
        MysqlProxy proxy = (MysqlProxy) registry.getHandler(proxyName);
        if (proxy.isActive()) {
            return new MysqlProxyExecutor(proxy, this.taskContext, requestWrapper);
        }
        throw new RuntimeException("The HttpProxy is not active.");
    }

    /** Getter/Setter methods */

    @Override
    public AbstractHandlerRegistry getRegistry() {
        return registry;
    }

    @Override
    public void setRegistry(AbstractHandlerRegistry registry) {
        this.registry = (MysqlProxyRegistry)registry;
    }

    @Override
    public TaskContext getTaskContext() {
        return this.taskContext;
    }

    @Override
    public void setTaskContext(TaskContext taskContext) {
        this.taskContext = taskContext;
    }
    /** End Getter/Setter methods */


}
