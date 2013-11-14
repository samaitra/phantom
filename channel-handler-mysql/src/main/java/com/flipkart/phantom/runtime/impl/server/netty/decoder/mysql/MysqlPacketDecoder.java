package com.flipkart.phantom.runtime.impl.server.netty.decoder.mysql;

import com.flipkart.phantom.mysql.impl.protocol.Packet;
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
public class MysqlPacketDecoder extends FrameDecoder {

    /** Logger for this class*/
    private static final Logger LOGGER = LoggerFactory.getLogger(MysqlPacketDecoder.class);

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
