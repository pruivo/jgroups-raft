package org.jgroups.raft2.impl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.function.Supplier;

import org.jgroups.Header;
import org.jgroups.Message;
import org.jgroups.raft2.RaftEventHandler;

/**
 * TODO! document this
 *
 * @author Pedro Ruivo
 * @since 12
 */
public class AppendResponseHeader extends Raft2Header{

   private int lastLogTerm;
   private boolean success;

   public AppendResponseHeader() {
   }

   public AppendResponseHeader(int term, int lastLogTerm, boolean success) {
      super(term);
      this.lastLogTerm = lastLogTerm;
      this.success = success;
   }

   public int getLastLogTerm() {
      return lastLogTerm;
   }

   public boolean isSuccess() {
      return success;
   }

   @Override
   public short getMagicId() {
      return RaftConfigurator.APPEND_RSP_ID;
   }

   @Override
   public Supplier<? extends Header> create() {
      return AppendResponseHeader::new;
   }

   @Override
   public int serializedSize() {
      return 0;
   }

   @Override
   public void writeTo(DataOutput dataOutput) throws IOException {

   }

   @Override
   public void readFrom(DataInput dataInput) throws IOException, ClassNotFoundException {

   }

   @Override
   public void execute(RaftEventHandler handler, Message message) {
      handler.onAppendResponse(this);
   }
}
