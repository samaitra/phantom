package com.flipkart.phantom.mysql.impl.protocol;

import java.util.ArrayList;

public class ComSetoption extends Packet {
    public long operation = 0;
    
    public ArrayList<byte[]> getPayload() {
        ArrayList<byte[]> payload = new ArrayList<byte[]>();
        
        payload.add(Proto.build_byte(Flags.COM_SET_OPTION));
        payload.add(Proto.build_fixed_int(2, this.operation));
        
        return payload;
    }

    public static ComSetoption loadFromPacket(byte[] packet) {
        ComSetoption obj = new ComSetoption();
        Proto proto = new Proto(packet, 3);
        
        obj.sequenceId = proto.get_fixed_int(1);
        proto.get_filler(1);
        obj.operation = proto.get_fixed_int(2);
        
        return obj;
    }
    
}
