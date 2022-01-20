package org.jgroups.raft2.impl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.function.Supplier;

import org.jgroups.Address;
import org.jgroups.Header;
import org.jgroups.Message;
import org.jgroups.raft2.RaftEventHandler;

/**
 * TODO! document this
 *
 * @author Pedro Ruivo
 * @since 12
 */
public class AppendRequestHeader extends Raft2Header {

   private RaftId leader;
   private int prevLogIdx;
   private int prevLogTerm;
   private int leaderCommit;


   public AppendRequestHeader() {
      super();
   }

   public AppendRequestHeader(int term, RaftId leader, int prevLogIdx, int prevLogTerm, int leaderCommit) {
      super(term);
      this.leader = leader;
      this.prevLogIdx = prevLogIdx;
      this.prevLogTerm = prevLogTerm;
      this.leaderCommit = leaderCommit;
   }

   @Override
   public short getMagicId() {
      return RaftConfigurator.APPEND_REQ_ID;
   }

   @Override
   public Supplier<? extends Header> create() {
      return AppendRequestHeader::new;
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
      handler.onAppendRequest(this, message.getRawBuffer(), message.getOffset(), message.getLength());
   }

   public RaftId getLeader() {
      return leader;
   }

   public int getPrevLogIdx() {
      return prevLogIdx;
   }

   public int getPrevLogTerm() {
      return prevLogTerm;
   }

   public int getLeaderCommit() {
      return leaderCommit;
   }
}
