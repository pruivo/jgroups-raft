package org.jgroups.raft2.impl.election;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.function.Supplier;

import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.Message;
import org.jgroups.raft2.RaftEventHandler;
import org.jgroups.raft2.impl.Raft2Header;
import org.jgroups.raft2.impl.RaftConfigurator;
import org.jgroups.raft2.impl.RaftId;
import org.jgroups.util.Util;

/**
 * TODO! document this
 *
 * @author Pedro Ruivo
 * @since 12
 */
public class VoteResponseHeader extends Raft2Header {

   private RaftId raftId;
   private boolean voteGranted;

   public VoteResponseHeader() {
   }

   public VoteResponseHeader(int term, RaftId raftId, boolean voteGranted) {
      super(term);
      this.raftId = raftId;
      this.voteGranted = voteGranted;
   }

   @Override
   public short getMagicId() {
      return RaftConfigurator.VOTE_RSP_ID;
   }

   @Override
   public Supplier<? extends Header> create() {
      return VoteResponseHeader::new;
   }

   @Override
   public int serializedSize() {
      return super.serializedSize() + raftId.serializedSize() + Global.BYTE_SIZE;
   }

   @Override
   public void writeTo(DataOutput dataOutput) throws IOException {
      super.writeTo(dataOutput);
      Util.writeGenericStreamable(raftId, dataOutput);
      dataOutput.writeBoolean(voteGranted);
   }

   @Override
   public void readFrom(DataInput dataInput) throws IOException, ClassNotFoundException {
      super.readFrom(dataInput);
      raftId = Util.readGenericStreamable(dataInput);
      voteGranted = dataInput.readBoolean();
   }

   public RaftId getRaftId() {
      return raftId;
   }

   public boolean isVoteGranted() {
      return voteGranted;
   }

   @Override
   public void execute(RaftEventHandler handler, Message message) {
      handler.onVoteResponse(this);
   }
}
