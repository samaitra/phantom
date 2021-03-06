package com.flipkart.phantom.runtime.impl.server.netty.handler.mysql;

import com.flipkart.phantom.event.ServiceProxyEventProducer;
import com.flipkart.phantom.mysql.impl.MysqlProxyExecutor;
import com.flipkart.phantom.mysql.impl.MysqlRequestWrapper;
import com.flipkart.phantom.runtime.impl.server.netty.channel.mysql.MysqlNettyChannelBuffer;
import com.flipkart.phantom.task.spi.Executor;
import com.flipkart.phantom.task.spi.repository.ExecutorRepository;
import com.github.jmpjct.mysql.proto.*;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.group.ChannelGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * <code>MysqlChannelHandler</code> is a sub-type of {@link SimpleChannelHandler} that acts as a proxy for Mysql calls using the mysql protocol.
 * It wraps the Mysql call using a {@link MysqlProxyExecutor} that provides useful features like monitoring, fallback etc.
 *
 * @author : samaitra
 * @version : 1.0
 * @date : 15/11/13
 */
public class MysqlChannelHandler extends SimpleChannelHandler implements InitializingBean {

    /** Host to connect to */
    public String host;

    /** port to connect to */
    public int port;

    /** mysql socket to connect mysql server */
    public Socket mysqlSocket = null;

    /** mysql socket input stream */
    public InputStream mysqlIn = null;

    /** mysql socket output stream */
    public OutputStream mysqlOut = null;

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

    /** The flag to determine the state in connection */
    private int flag = Flags.MODE_INIT;

    /** The response byte array holder*/
    private ArrayList<byte[]> buffer;

    /** The response InputStream from mysql socket */
    private InputStream in = null;

    private String query;

    private String commandKey;

    public boolean bufferResultSet = true;

    private long sequenceId;

    private String schema;

    private Executor executor;

    private String proxy;

    private int count=0;

    private ArrayList<ArrayList<byte[]>> connRefBytes = new ArrayList<ArrayList<byte[]>>();
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


        if(this.flag == Flags.MODE_INIT){
            this.in = execute(ctx,this.flag);
            writeServerHandshake(ctx,event,this.in);
        }

        super.handleUpstream(ctx,event);
    }


    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent messageEvent) throws Exception{

        if(this.flag == Flags.MODE_READ_AUTH){
            handleAuthentication(ctx,messageEvent);
        }else {
            handleQueries(ctx,messageEvent);
        }

        /* Increment count to keep track of connection customs.
         * There are 7 customary queries being fired by MysqlClient before connection can be established.
         */

        this.count++;
    }

    private InputStream execute(ChannelHandlerContext ctx, int flag) throws Exception{

        /*
        Need to create a socket connection in the handler to establish successful authentication ritual with mysql server.
        Delegating this process to Mysql proxy makes the order of messages incorrect. When messages reaches the Hystrix thread pool
        and responses are received the order of messages are not as per client expectations.
        */

        this.mysqlSocket = new Socket(this.host, this.port);
        this.mysqlSocket.setPerformancePreferences(0, 2, 1);
        this.mysqlSocket.setTcpNoDelay(true);
        this.mysqlSocket.setTrafficClass(0x10);
        this.mysqlSocket.setKeepAlive(true);

        //LOGGER.info("Connected to mysql server at "+this.host+":"+this.port);
        this.mysqlIn = new BufferedInputStream(this.mysqlSocket.getInputStream(), 16384);
        this.mysqlOut = this.mysqlSocket.getOutputStream();


        return this.mysqlIn;


   }

    private void writeServerHandshake(ChannelHandlerContext ctx, ChannelEvent event, InputStream in) throws Exception {

        byte[] packet = Packet.read_packet(in);
        Auth_Challenge authChallenge = Auth_Challenge.loadFromPacket(packet);

        // Remove some flags from the reply
        authChallenge.removeCapabilityFlag(Flags.CLIENT_COMPRESS);
        authChallenge.removeCapabilityFlag(Flags.CLIENT_SSL);
        authChallenge.removeCapabilityFlag(Flags.CLIENT_LOCAL_FILES);

        // Set the default result set creation to the server's character set
        ResultSet.characterSet = authChallenge.characterSet;

        // Set Replace the packet in the buffer
        this.buffer = new ArrayList<byte[]>();
        this.buffer.add(authChallenge.toPacket());

        MysqlNettyChannelBuffer.write(event.getChannel(), this.buffer);
        this.flag = Flags.MODE_READ_AUTH;

    }

    private void handleAuthentication(ChannelHandlerContext ctx, MessageEvent messageEvent) throws Exception{

        this.buffer =  (ArrayList<byte[]>)messageEvent.getMessage();

        Auth_Response authReply = Auth_Response.loadFromPacket(this.buffer.get(0));
        if (!authReply.hasCapabilityFlag(Flags.CLIENT_PROTOCOL_41)) {
            LOGGER.debug("We do not support Protocols under 4.1");
            return;
        }

        authReply.removeCapabilityFlag(Flags.CLIENT_COMPRESS);
        authReply.removeCapabilityFlag(Flags.CLIENT_SSL);
        authReply.removeCapabilityFlag(Flags.CLIENT_LOCAL_FILES);

        /* Adding client auth request buffer to connRefBytes. This object will be forwarded to
           to Mysql proxy to establish the connection and validate the client credentials before forwarding the
           queries to mysql server
        */
        this.connRefBytes.add(this.buffer);

        this.in = executeAuth(ctx, Flags.MODE_SEND_AUTH, this.buffer);
        writeAuthResponse(ctx, messageEvent, this.in);

    }

    private InputStream executeAuth(ChannelHandlerContext ctx, int flag, ArrayList<byte[]> buffer) throws Exception{


        switch (flag){
            case Flags.MODE_SEND_AUTH:
                Packet.write(this.mysqlOut, buffer);
                break;
            default:
                break;

        }

        return this.mysqlIn;

    }

    private void writeAuthResponse(ChannelHandlerContext ctx, MessageEvent messageEvent, InputStream in) throws Exception{

        byte[] packet = Packet.read_packet(in);
        this.buffer = new ArrayList<byte[]>();
        this.buffer.add(packet);

        if (Packet.getType(packet) != Flags.OK) {
            LOGGER.debug("Auth is not okay!");
        }

        MysqlNettyChannelBuffer.write(messageEvent.getChannel(), this.buffer);
        this.flag = Flags.MODE_READ_QUERY;
    }

    private void handleQueries(ChannelHandlerContext ctx, MessageEvent messageEvent) throws Exception{

        this.bufferResultSet = false;
        this.buffer = new ArrayList<byte[]>();
        this.buffer = (ArrayList<byte[]>) messageEvent.getMessage();

        byte[] packet = this.buffer.get(0);
        this.sequenceId = Packet.getSequenceId(packet);

        switch (Packet.getType(packet)) {
            case Flags.COM_QUIT:
                this.halt(messageEvent);
                break;

            // Extract out the new default schema
            case Flags.COM_INIT_DB:
                this.schema = Com_Initdb.loadFromPacket(packet).schema;
                break;

            // Query
            case Flags.COM_QUERY:
                this.query = Com_Query.loadFromPacket(packet).query;
                break;

            default:
                break;

        }

        //No need to write response if Command request packet is for Quit
        if(Packet.getType(packet) == Flags.COM_QUIT){
            executeQueries(ctx,Flags.MODE_SEND_QUERY,this.buffer);
        }else{
            this.in = executeQueries(ctx,Flags.MODE_SEND_QUERY,this.buffer);
            writeQueryResponse(ctx,messageEvent,this.in);
        }

    }

    private InputStream executeQueries(ChannelHandlerContext ctx, int flag, ArrayList<byte[]> buffer) throws Exception{

        if(this.count<7){

            /* Adding client connection queries buffer to connRefBytes. This object will be forwarded to
            to Mysql proxy to establish the connection and validate the client credentials before forwarding the
            queries to mysql server
            */
            this.connRefBytes.add(buffer);
            Packet.write(this.mysqlOut, buffer);
            return this.mysqlIn;


        }else {
            if (this.count == 7) {
            /* closing the local {@link MysqlChannelHandler} mysql socket connection.
             This connection is required to obtain the connection reference keys so that
             the keys and subsequent queries can be forwarded to the mysql proxy.
            */
                closeConnection();
                return forwardRequests(ctx,flag,buffer);
            } else {
                return forwardRequests(ctx,flag,buffer);
            }
        }

    }

    private InputStream forwardRequests(ChannelHandlerContext ctx, int flag, ArrayList<byte[]> buffer) throws Exception{
        MysqlRequestWrapper executorMysqlRequest = new MysqlRequestWrapper();
        executorMysqlRequest.setFlag(flag);
        executorMysqlRequest.setBuffer(buffer);
        executorMysqlRequest.setConnRefBytes(this.connRefBytes);
        this.commandKey = getQueryCommand(this.query);
        executorMysqlRequest.setCommandKey(this.commandKey);

        String proxy = this.proxyMap.get(MysqlChannelHandler.ALL_ROUTES);
        Executor executor = this.repository.getExecutor(proxy,proxy,executorMysqlRequest);
        try{
            this.in = (InputStream) executor.execute();
        }catch (Exception e){
            throw new RuntimeException("Error in reading server Queries Response :" + proxy + ".", e);
        }
        return this.in;

    }



    private void writeQueryResponse(ChannelHandlerContext ctx, MessageEvent messageEvent, InputStream in) throws Exception {

        byte[] packet = Packet.read_packet(in);
        this.buffer = new ArrayList<byte[]>();
        this.buffer.add(packet);

        this.sequenceId = Packet.getSequenceId(packet);

        switch (Packet.getType(packet)) {
            case Flags.OK:
            case Flags.ERR:
                break;

            default:
                this.buffer = MysqlNettyChannelBuffer.readFullResultSet(in, messageEvent, this.buffer, this.bufferResultSet);
                break;
        }


        MysqlNettyChannelBuffer.write(messageEvent.getChannel(), this.buffer);
        this.flag = Flags.MODE_READ_QUERY;

    }

    private void halt(MessageEvent messageEvent) throws Exception{
        //close mysql socket and input/output stream
        closeConnection();

    }

    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent event) throws Exception {
        LOGGER.warn("Exception {} thrown on Channel {}. Disconnect initiated", event, event.getChannel());
        event.getCause().printStackTrace();
        event.getChannel().close();
        closeConnection();
        super.exceptionCaught(ctx, event);
    }

    public String getQueryCommand(String query){
        query = query.trim();
        int i = query.indexOf(' ');
        String command = query.substring(0, i);
        command = command.toUpperCase();
        return command;
    }

    private void closeConnection() throws Exception{

        this.mysqlSocket.close();
        this.mysqlIn.close();
        this.mysqlOut.close();
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
    public String getHost() {
        return host;
    }
    public void setHost(String host) {
        this.host = host;
    }
    public int getPort() {
        return port;
    }
    public void setPort(int port) {
        this.port = port;
    }


    /** End Getter/Setter methods */


}
