package org.jgroups.protocols.raft2;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.annotations.Property;
import org.jgroups.raft2.ElectionTimeoutTask;
import org.jgroups.raft2.RaftEventHandler;
import org.jgroups.raft2.impl.MessageSender;
import org.jgroups.raft2.impl.Raft2Header;
import org.jgroups.raft2.impl.RaftConfigurator;
import org.jgroups.raft2.impl.RaftEventHandlerImpl;
import org.jgroups.raft2.impl.RaftId;
import org.jgroups.raft2.impl.cluster.ClusterConfiguration;
import org.jgroups.raft2.impl.election.ElectionManager;
import org.jgroups.raft2.impl.election.ElectionManagerImpl;
import org.jgroups.raft2.impl.election.ElectionTimeoutTaskImpl;
import org.jgroups.raft2.impl.log.RaftLog;
import org.jgroups.raft2.impl.state.RaftState;
import org.jgroups.raft2.impl.state.RaftStateImpl;
import org.jgroups.stack.Protocol;
import org.jgroups.util.ExtendedUUID;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.Util;

/**
 * TODO! document this
 *
 * @author Pedro Ruivo
 * @since 12
 */
public class RAFT2 extends Protocol implements MessageSender {

   static {
      RaftConfigurator.configure();
   }

   @Property(description = "The identifier of this node. Needs to be unique and an element of members. Must not be null",
         writable = false)
   protected String raftId;
   @Property(description = "Interval (in ms) at which a leader sends out heartbeats")
   protected long heartbeat_interval = 30;

   @Property(description = "Min election interval (ms)")
   protected long election_min_interval = 150;

   @Property(description = "Max election interval (ms). The actual election interval is computed as a random value in " +
         "range [election_min_interval..election_max_interval]")
   protected long election_max_interval = 300;


   /**
    * Initial set of members defining the Raft cluster
    */
   protected Collection<String> members = Collections.emptyList();

   private ElectionTimeoutTask electionTimeoutTask;
   private RaftEventHandler raftEventHandler;
   private ClusterConfiguration cluster;

   public RAFT2 raftId(String raftId) {
      this.raftId = Objects.requireNonNull(raftId);
      return this;
   }

   public RAFT2 members(Collection<String> members) {
      this.members = new ArrayList<>(Objects.requireNonNull(members));
      return this;
   }

   @Override
   public void init() throws Exception {
      super.init();
      // values are validated inside create()
      electionTimeoutTask = ElectionTimeoutTaskImpl.create(election_min_interval, election_max_interval);

      RaftState state = new RaftStateImpl(raftId);
      RaftLog raftLog = null;
      ElectionManager em = ElectionManagerImpl.create(getTransport().getInternalThreadPool(), state, raftLog, cluster, this);

      raftEventHandler = new RaftEventHandlerImpl(em, cluster, state, this);

      electionTimeoutTask.add(em);
   }

   @Override
   public void start() throws Exception {
      super.start();
      raftEventHandler.start();
   }

   @Override
   public void stop() {
      super.stop();
   }

   @Override
   public void destroy() {
      super.destroy();
   }

   @Override
   public Object down(Event evt) {
      switch (evt.getType()) {
         case Event.CONNECT:
         case Event.CONNECT_USE_FLUSH:
         case Event.CONNECT_WITH_STATE_TRANSFER:
         case Event.CONNECT_WITH_STATE_TRANSFER_USE_FLUSH:
            Object retVal = down_prot.down(evt); // connect first
            electionTimeoutTask.startTimer(getTransport().getTimer());
            return retVal;
         case Event.DISCONNECT:
            raftEventHandler.disconnect();
            electionTimeoutTask.stopTimer();
            break;
         case Event.SET_LOCAL_ADDRESS:
            Address localAddress = evt.getArg();
            if (!(localAddress instanceof ExtendedUUID)) {
               throw new IllegalStateException();
            }
            if (!RaftConfigurator.isSameRaftId(Util.stringToBytes(raftId), (ExtendedUUID) localAddress)) {
               throw new IllegalStateException();
            }
            break;
         case Event.VIEW_CHANGE:
            handleView(evt.getArg());
            break;
      }
      return down_prot.down(evt);
   }

   @Override
   public Object down(Message msg) {
      return super.down(msg);
   }

   @Override
   public Object up(Event evt) {
      if (evt.getType() == Event.VIEW_CHANGE)
         handleView(evt.getArg());
      return up_prot.up(evt);
   }

   @Override
   public Object up(Message msg) {
      Raft2Header hdr = msg.getHeader(id);
      if (hdr != null) {
         handleRaftEvent(msg, hdr);
         return null;
      }
      return up_prot.up(msg);
   }

   @Override
   public void up(MessageBatch batch) {
      for (Message msg : batch) {
         Raft2Header hdr = msg.getHeader(id);
         if (hdr != null) {
            batch.remove(msg);
            handleRaftEvent(msg, hdr);
         }
      }
      if (!batch.isEmpty())
         up_prot.up(batch);
   }

   private void handleView(View newView) {
      cluster.handleView(newView);
   }

   private void handleRaftEvent(Message message, Raft2Header header) {
      Address src = message.getSrc();
      if (!(src instanceof ExtendedUUID)) {
         //discard
         return;
      }
      electionTimeoutTask.onMessageReceived();
      header.execute(raftEventHandler, message);
   }

   @Override
   public void sendTo(RaftId dst, Raft2Header header) {

   }

   @Override
   public void sendToAll(Raft2Header header) {

   }
}
