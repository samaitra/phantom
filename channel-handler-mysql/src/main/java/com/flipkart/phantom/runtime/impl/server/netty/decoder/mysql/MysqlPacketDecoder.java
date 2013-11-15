package com.flipkart.phantom.runtime.impl.server.netty.decoder.mysql;

import com.github.jmpjct.mysql.proto.Packet;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.InputStream;
import java.util.ArrayList;

/**
 * Created with IntelliJ IDEA.
 * User: saikat
 * Date: 31/10/13
 * Time: 12:51 PM
 * To change this template use File | Settings | File Templates.
 */

/**
 * <code>MysqlPacketDecoder</code> is an extension of the Netty {@link org.jboss.netty.handler.codec.frame.FrameDecoder} that ensures that all mysql protocol bytes have been received
 * before the {@link org.jboss.netty.channel.MessageEvent} is constructed for use by other upstream channel handlers.
 *  This decoder attempts to read the Mysql message from the transport using the protocol. An unsuccessful read indicates that the bytes have not been fully
 * received. This decoder returns a null object in {@link #decode(ChannelHandlerContext, Channel, ChannelBuffer)} in such scenarios and the Netty
 * framework would then call it again when more bytes are received, eventually resulting in all required bytes becoming available.
 * @author Saikat Maitra
 * @version 1.0, 15 November, 2013
 */
public class MysqlPacketDecoder extends FrameDecoder {

    /** Logger for this class*/
    private static final Logger LOGGER = LoggerFactory.getLogger(MysqlPacketDecoder.class);

    /**
     * Interface method implementation. Tries to read the Mysql protocol message. Returns null if unsuccessful, else returns the read byte array
     * @see org.jboss.netty.handler.codec.frame.FrameDecoder#decode(org.jboss.netty.channel.ChannelHandlerContext, org.jboss.netty.channel.Channel, org.jboss.netty.buffer.ChannelBuffer)
     */
    @Override
    protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buf) throws Exception {

        InputStream in = new ChannelBufferInputStream(buf);

        int b=0;
        int size = 0;

        byte[] packet = new byte[3];

        int offset = 0;
        int target = 3;

        do {
            b = in.read(packet, offset, (target - offset));
            if (b == -1) {
              return null;
            }
            offset += b;
        } while (offset != target);

        size = Packet.getSize(packet);

        byte[] packet_tmp = new byte[size+4];
        System.arraycopy(packet, 0, packet_tmp, 0, 3);
        packet = packet_tmp;
        packet_tmp = null;

        target = packet.length;
        do {
            b = in.read(packet, offset, (target - offset));
            if (b == -1) {
                return null;
            }
            offset += b;
        } while (offset != target);
//        LOGGER.info("RECEIVED: MySQL packet (size) = "+size);
        ArrayList<byte[]> buffer = new ArrayList<byte[]>();
        buffer.add(packet);
        return buffer;

    }




}
