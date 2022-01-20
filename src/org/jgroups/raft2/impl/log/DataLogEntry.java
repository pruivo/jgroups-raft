package org.jgroups.raft2.impl.log;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.jgroups.raft.util.Bits2;
import org.jgroups.raft2.LogEntry;
import org.jgroups.util.Bits;
import org.jgroups.util.Util;

/**
 * TODO! document this
 *
 * @author Pedro Ruivo
 * @since 12
 */
public class DataLogEntry extends BaseLogEntry implements LogEntry {

   private int term;
   private int index;
   private byte[] data;
   private int offset;
   private int length;

   public DataLogEntry(int term, int index, byte[] data, int offset, int length) {
      super(term, index);
      this.data = data;
      this.offset = offset;
      this.length = length;
   }

   @Override
   public int serializedSize() {
      return super.serializedSize() + Bits.size(length) + length;
   }

   @Override
   public void writeTo(DataOutput dataOutput) throws IOException {
      super.writeTo(dataOutput);
      Bits2.writeIntCompressed(length, dataOutput);
      dataOutput.write(data, offset, length);
   }

   @Override
   public void readFrom(DataInput dataInput) throws IOException, ClassNotFoundException {
      super.readFrom(dataInput);
      data = new byte[Bits2.readIntCompressed(dataInput)];
      dataInput.readFully(data);
   }
}
