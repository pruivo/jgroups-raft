package org.jgroups.raft2.impl.election;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.function.Supplier;

import org.jgroups.Address;
import org.jgroups.Header;
import org.jgroups.Message;
import org.jgroups.raft.util.Bits2;
import org.jgroups.raft2.RaftEventHandler;
import org.jgroups.raft2.impl.Raft2Header;
import org.jgroups.raft2.impl.RaftConfigurator;
import org.jgroups.raft2.impl.RaftId;
import org.jgroups.util.Bits;
import org.jgroups.util.ExtendedUUID;
import org.jgroups.util.Util;

/**
 * TODO! document this
 *
 * @author Pedro Ruivo
 * @since 12
 */
public class VoteRequestHeader extends Raft2Header {

   private RaftId candidateId;
   private int lastLogIdx;
   private int lastLogTerm;

   public VoteRequestHeader() {
   }

   public VoteRequestHeader(int term, RaftId candidateId, int lastLogIdx, int lastLogTerm) {
      super(term);
      this.candidateId = candidateId;
      this.lastLogIdx = lastLogIdx;
      this.lastLogTerm = lastLogTerm;
   }

   @Override
   public short getMagicId() {
      return RaftConfigurator.VOTE_REQ_ID;
   }

   @Override
   public Supplier<? extends Header> create() {
      return VoteRequestHeader::new;
   }

   @Override
   public int serializedSize() {
      return super.serializedSize() + candidateId.serializedSize() + Bits.size(lastLogIdx) + Bits.size(lastLogTerm);
   }

   @Override
   public void writeTo(DataOutput dataOutput) throws IOException {
      super.writeTo(dataOutput);
      Util.writeGenericStreamable(candidateId, dataOutput);
      Bits2.writeIntCompressed(lastLogIdx, dataOutput);
      Bits2.writeIntCompressed(lastLogTerm, dataOutput);
   }

   @Override
   public void readFrom(DataInput dataInput) throws IOException, ClassNotFoundException {
      super.readFrom(dataInput);
      candidateId = Util.readGenericStreamable(dataInput);
      lastLogIdx = Bits2.readIntCompressed(dataInput);
      lastLogTerm = Bits2.readIntCompressed(dataInput);
   }

   @Override
   public void execute(RaftEventHandler handler, Message message) {
      Address src = message.getSrc();
      assert src instanceof ExtendedUUID;
      if (!candidateId.isSameRaftId((ExtendedUUID) src)) {
         //discard
         return;
      }
      handler.onVoteRequest(this);
   }

   public RaftId getCandidateId() {
      return candidateId;
   }

   public int getLastLogIdx() {
      return lastLogIdx;
   }

   public int getLastLogTerm() {
      return lastLogTerm;
   }
}
