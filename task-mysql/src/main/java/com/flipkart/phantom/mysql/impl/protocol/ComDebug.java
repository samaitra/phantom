package com.flipkart.phantom.mysql.impl.protocol;

import java.util.ArrayList;

public class ComDebug extends Packet {
    public ArrayList<byte[]> getPayload() {
        ArrayList<byte[]> payload = new ArrayList<byte[]>();
        
        payload.add(Proto.build_byte(Flags.COM_DEBUG));
        
        return payload;
    }
    
    public static ComDebug loadFromPacket(byte[] packet) {
        ComDebug obj = new ComDebug();
        Proto proto = new Proto(packet, 3);
        
        obj.sequenceId = proto.get_fixed_int(1);
        
        return obj;
    }
}
