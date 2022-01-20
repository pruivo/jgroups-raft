package org.jgroups.raft2.impl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.jgroups.Header;
import org.jgroups.Message;
import org.jgroups.raft.util.Bits2;
import org.jgroups.raft2.RaftEventHandler;
import org.jgroups.util.Bits;

/**
 * TODO! document this
 *
 * @author Pedro Ruivo
 * @since 12
 */
public abstract class Raft2Header extends Header {

   private int term;

   protected Raft2Header() {
   }

   protected Raft2Header(int term) {
      this.term = term;
   }

   public int getTerm() {
      return term;
   }

   public abstract void execute(RaftEventHandler handler, Message message);

   @Override
   public int serializedSize() {
      return Bits.size(term);
   }

   @Override
   public void writeTo(DataOutput dataOutput) throws IOException {
      Bits2.writeIntCompressed(term, dataOutput);
   }

   @Override
   public void readFrom(DataInput dataInput) throws IOException, ClassNotFoundException {
      term = Bits2.readIntCompressed(dataInput);
   }
}
