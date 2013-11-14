package com.flipkart.phantom.mysql.impl.protocol;

import java.util.ArrayList;

public class ComRefresh extends Packet {
    public long flags = 0x00;
    
    public ArrayList<byte[]> getPayload() {
        ArrayList<byte[]> payload = new ArrayList<byte[]>();
        
        payload.add(Proto.build_byte(Flags.COM_REFRESH));
        payload.add(Proto.build_fixed_int(1, this.flags));
        
        return payload;
    }
    
    public static ComRefresh loadFromPacket(byte[] packet) {
        ComRefresh obj = new ComRefresh();
        Proto proto = new Proto(packet, 3);
        
        obj.sequenceId = proto.get_fixed_int(1);
        proto.get_filler(1);
        obj.flags = proto.get_fixed_int(1);
        
        return obj;
    }
}
