package com.flipkart.phantom.runtime.impl.server.netty.handler.mysql;

import com.flipkart.phantom.event.ServiceProxyEventProducer;
import com.flipkart.phantom.mysql.impl.MysqlProxyExecutor;
import com.flipkart.phantom.mysql.impl.MysqlRequestWrapper;
import com.flipkart.phantom.mysql.impl.protocol.*;
import com.flipkart.phantom.task.spi.Executor;
import com.flipkart.phantom.task.spi.repository.ExecutorRepository;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.group.ChannelGroup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: saikat
 * Date: 31/10/13
 * Time: 12:51 PM
 * To change this template use File | Settings | File Templates.
 */
public class MysqlChannelHandler extends SimpleChannelHandler implements InitializingBean {

    /** The empty routing key which is default*/
    public static final String ALL_ROUTES = "";

    /** Logger for this class*/
    private static final Logger LOGGER = LoggerFactory.getLogger(MysqlChannelHandler.class);

    /** The default channel group*/
    private ChannelGroup defaultChannelGroup;

    /** The MysqlProxyRepository to lookup MysqlProxy from */
    private ExecutorRepository repository;

    /** The Mysql proxy handler map*/
    private Map<String, String> proxyMap = new HashMap<String, String>();

    /** The default Mysql proxy handler */
    private String defaultProxy;

    /** The publisher used to broadcast events to Service Proxy Subscribers */
    private ServiceProxyEventProducer eventProducer;

    /** Event Type for publishing all events which are generated here */
    private final static String Mysql_HANDLER = "Mysql_HANDLER";

    private String flag = "init";

    ArrayList<byte[]> buffer;
    private long sequenceId;
    private String schema;
    private String query;

    public boolean bufferResultSet = true;
    public boolean packResultSet = true;

    /**
     * Interface method implementation. Checks if all mandatory properties have been set
     * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
     */
    public void afterPropertiesSet() throws Exception {
        Assert.notNull(this.defaultProxy, "The 'defaultProxy' may not be null");
        // add the default proxy for all routes i.e. default
        this.proxyMap.put(MysqlChannelHandler.ALL_ROUTES, defaultProxy);
    }

    /**
     * Overriden superclass method. Adds the newly created Channel to the default channel group and calls the super class {@link #channelOpen(org.jboss.netty.channel.ChannelHandlerContext, org.jboss.netty.channel.ChannelStateEvent)} method
     * @see org.jboss.netty.channel.SimpleChannelUpstreamHandler#channelOpen(org.jboss.netty.channel.ChannelHandlerContext, org.jboss.netty.channel.ChannelStateEvent)
     */
    public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent event) throws Exception {
        super.channelOpen(ctx, event);
    }

    @Override
    public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent event) throws Exception {

        ChannelBuffer input=null;
        try{
          input = (ChannelBuffer) ((MessageEvent) event).getMessage();
        }catch (Exception e){}
        ChannelBuffer output = ChannelBuffers.dynamicBuffer(4096);


        if(this.flag.equals("init")){

        MysqlRequestWrapper executorMysqlRequest = new MysqlRequestWrapper();
        executorMysqlRequest.setUri("init");

        String proxy = this.proxyMap.get(MysqlChannelHandler.ALL_ROUTES);
        Executor executor = this.repository.getExecutor(proxy,proxy,executorMysqlRequest);
        InputStream in = null;
        try{
          in = (InputStream) executor.execute();
        }catch (Exception e){
            throw new RuntimeException("Error in reading server handshake message :" + proxy + ".", e);
        }finally {

        // Publishes event both in case of success and failure.
            Class eventSource = (executor == null) ? this.getClass() :((MysqlProxyExecutor)executor).getProxy().getClass();
            eventProducer.publishEvent(executor, "init", eventSource, Mysql_HANDLER);
        }

        byte[] packet = Packet.read_packet(in);

        AuthChallenge authChallenge = AuthChallenge.loadFromPacket(packet);

        // Remove some flags from the reply
        authChallenge.removeCapabilityFlag(Flags.CLIENT_COMPRESS);
        authChallenge.removeCapabilityFlag(Flags.CLIENT_SSL);
        authChallenge.removeCapabilityFlag(Flags.CLIENT_LOCAL_FILES);

        // Set the default result set creation to the server's character set
        ResultSet.characterSet = authChallenge.characterSet;

        // Set Replace the packet in the buffer
       this.buffer = new ArrayList<byte[]>();
       this.buffer.add(authChallenge.toPacket());

        output = ChannelBuffers.copiedBuffer(authChallenge.toPacket());
        event.getChannel().write(output).addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture channelFuture) throws Exception {
                LOGGER.debug("Write complete for server handshake.");

            }
        });
        this.flag = "readAuth";

        }

        super.handleUpstream(ctx, event);
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent messageEvent) throws Exception{


        ChannelBuffer output = ChannelBuffers.dynamicBuffer(4096);

        if(this.flag.equals("readAuth")){

            this.buffer =  (ArrayList<byte[]>)messageEvent.getMessage();

            AuthResponse authReply = AuthResponse.loadFromPacket(this.buffer.get(0));

            if (!authReply.hasCapabilityFlag(Flags.CLIENT_PROTOCOL_41)) {
                LOGGER.debug("We do not support Protocols under 4.1");

                return;
            }

            authReply.removeCapabilityFlag(Flags.CLIENT_COMPRESS);
            authReply.removeCapabilityFlag(Flags.CLIENT_SSL);
            authReply.removeCapabilityFlag(Flags.CLIENT_LOCAL_FILES);

            String schema = authReply.schema;


            MysqlRequestWrapper executorMysqlRequest = new MysqlRequestWrapper();
            executorMysqlRequest.setUri("clientAuth");
            executorMysqlRequest.setBuffer(this.buffer);
            String proxy = this.proxyMap.get(MysqlChannelHandler.ALL_ROUTES);
            Executor executor = this.repository.getExecutor(proxy,proxy,executorMysqlRequest);
            InputStream in = null;
            try{
                in = (InputStream) executor.execute();
            }catch (Exception e){
                throw new RuntimeException("Error in reading server Auth Response :" + proxy + ".", e);
            }finally {

                // Publishes event both in case of success and failure.
                Class eventSource = (executor == null) ? this.getClass() :((MysqlProxyExecutor)executor).getProxy().getClass();
                eventProducer.publishEvent(executor, "init", eventSource, Mysql_HANDLER);
            }

            byte[] packet = Packet.read_packet(in);

            this.buffer = new ArrayList<byte[]>();
            this.buffer.add(packet);

            if (Packet.getType(packet) != Flags.OK) {
                LOGGER.debug("Auth is not okay!");
            }

            this.flag = "readQuery";

            output =  ChannelBuffers.copiedBuffer(this.buffer.get(0));
            messageEvent.getChannel().write(output).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture channelFuture) throws Exception {
                    LOGGER.debug("Write complete for server AuthResult");

                }
            });

        }

        else {

            this.bufferResultSet = false;
            this.buffer = new ArrayList<byte[]>();
            this.buffer = (ArrayList<byte[]>) messageEvent.getMessage();
            byte[] packet = this.buffer.get(0);

            this.sequenceId = Packet.getSequenceId(packet);
//          LOGGER.debug("Client sequenceId: " + this.sequenceId);

            switch (Packet.getType(packet)) {
                case Flags.COM_QUIT:
                    LOGGER.info("COM_QUIT");
                    this.halt(messageEvent);
                    break;

                // Extract out the new default schema
                case Flags.COM_INIT_DB:
                   LOGGER.trace("COM_INIT_DB");
                    this.schema = ComInitdb.loadFromPacket(packet).schema;
                    break;

                // Query
                case Flags.COM_QUERY:
                    LOGGER.trace("COM_QUERY");
                    this.query = ComQuery.loadFromPacket(packet).query;
//                  LOGGER.debug("my query  : "+this.query);
                    break;

                default:
                    break;

            }

            MysqlRequestWrapper executorMysqlRequest = new MysqlRequestWrapper();
            executorMysqlRequest.setUri("sendQuery");
            executorMysqlRequest.setBuffer(this.buffer);
            String proxy = this.proxyMap.get(MysqlChannelHandler.ALL_ROUTES);
            Executor executor = this.repository.getExecutor(proxy,proxy,executorMysqlRequest);
            InputStream in = null;
            try{
                in = (InputStream) executor.execute();
            }catch (Exception e){
                throw new RuntimeException("Error in Send Query :" + proxy + ".", e);
            }finally {

                Class eventSource = (executor == null) ? this.getClass() :((MysqlProxyExecutor)executor).getProxy().getClass();
                eventProducer.publishEvent(executor, "init", eventSource, Mysql_HANDLER);
            }

            packet = Packet.read_packet(in);

            this.buffer = new ArrayList<byte[]>();
            this.buffer.add(packet);

            this.sequenceId = Packet.getSequenceId(packet);

            switch (Packet.getType(packet)) {
                case Flags.OK:
                case Flags.ERR:
                    break;

                default:
                    this.buffer = Packet.read_full_result_set(in, messageEvent, this.buffer, this.bufferResultSet);
                    break;
            }

            this.flag = "readQuery";

            Packet.write(messageEvent,this.buffer);



        }

    }

    private void halt(MessageEvent e) {
        e.getChannel().close();
    }

    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent event) throws Exception {
        LOGGER.warn("Exception {} thrown on Channel {}. Disconnect initiated", event, event.getChannel());
        event.getCause().printStackTrace();
        event.getChannel().close();
    }


    /** Start Getter/Setter methods */
    public ChannelGroup getDefaultChannelGroup() {
        return this.defaultChannelGroup;
    }
    public void setDefaultChannelGroup(ChannelGroup defaultChannelGroup) {
        this.defaultChannelGroup = defaultChannelGroup;
    }
    public ExecutorRepository getRepository() {
        return this.repository;
    }
    public void setRepository(ExecutorRepository repository) {
        this.repository = repository;
    }
    public Map<String, String> getProxyMap() {
        return this.proxyMap;
    }
    public void setProxyMap(Map<String, String> proxyMap) {
        this.proxyMap = proxyMap;
    }
    public String getDefaultProxy() {
        return this.defaultProxy;
    }
    public void setDefaultProxy(String defaultProxy) {
        this.defaultProxy = defaultProxy;
    }

    public void setEventProducer(ServiceProxyEventProducer eventProducer) {
        this.eventProducer = eventProducer;

    }
    /** End Getter/Setter methods */
}
