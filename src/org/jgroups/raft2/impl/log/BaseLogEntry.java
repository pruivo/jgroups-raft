package org.jgroups.raft2.impl.log;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.jgroups.Global;
import org.jgroups.raft.util.Bits2;
import org.jgroups.raft2.LogEntry;
import org.jgroups.util.Bits;

/**
 * TODO! document this
 *
 * @author Pedro Ruivo
 * @since 12
 */
public abstract class BaseLogEntry implements LogEntry {

   protected int term;
   protected int index;

   public BaseLogEntry(int term, int index) {
      this.term = term;
      this.index = index;
   }

   @Override
   public final int getTerm() {
      return term;
   }

   @Override
   public final int getLogIndex() {
      return index;
   }

   @Override
   public int serializedSize() {
      return Bits.size(term) + Bits.size(index);
   }

   @Override
   public void writeTo(DataOutput dataOutput) throws IOException {
      Bits2.writeIntCompressed(term, dataOutput);
      Bits2.writeIntCompressed(index, dataOutput);
   }

   @Override
   public void readFrom(DataInput dataInput) throws IOException, ClassNotFoundException {
      term = Bits2.readIntCompressed(dataInput);
      index = Bits2.readIntCompressed(dataInput);
   }
}
