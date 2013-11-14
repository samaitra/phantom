package com.flipkart.phantom.mysql.impl.protocol;

import java.util.ArrayList;

public class ComQuery extends Packet {
    public String query = "";
    
    public ArrayList<byte[]> getPayload() {
        ArrayList<byte[]> payload = new ArrayList<byte[]>();
        
        payload.add(Proto.build_byte(Flags.COM_QUERY));
        payload.add(Proto.build_fixed_str(this.query.length(), this.query));
        
        return payload;
    }

    public static ComQuery loadFromPacket(byte[] packet) {
        ComQuery obj = new ComQuery();
        Proto proto = new Proto(packet, 3);
        
        obj.sequenceId = proto.get_fixed_int(1);
        proto.get_filler(1);
        obj.query = proto.get_eop_str();
        
        return obj;
    }
    
}
