package org.jgroups.raft2.impl;

import java.util.Collection;
import java.util.Objects;

import org.jgroups.protocols.raft2.RAFT2;
import org.jgroups.raft2.RaftEventHandler;
import org.jgroups.raft2.impl.cluster.ClusterConfiguration;
import org.jgroups.raft2.impl.election.ElectionManager;
import org.jgroups.raft2.impl.election.VoteResponseHeader;
import org.jgroups.raft2.impl.election.VoteRequestHeader;
import org.jgroups.raft2.impl.eventloop.RaftRoleEventLoop;
import org.jgroups.raft2.impl.state.RaftState;

/**
 * TODO! document this
 *
 * @author Pedro Ruivo
 * @since 12
 */
public class RaftEventHandlerImpl implements RaftEventHandler {

   private final ElectionManager electionManager;
   private final ClusterConfiguration clusterConfiguration;
   private final RaftState state;
   private final MessageSender messageSender;

   private volatile RaftRoleEventLoop raftRoleEventLoop;

   public RaftEventHandlerImpl(ElectionManager electionManager, ClusterConfiguration clusterConfiguration, RaftState state, MessageSender messageSender) {
      this.electionManager = electionManager;
      this.clusterConfiguration = Objects.requireNonNull(clusterConfiguration);
      this.state = Objects.requireNonNull(state);
      this.messageSender = messageSender;
   }

   @Override
   public void init(RAFT2 protocol, Collection<String> initialMembers) {
      raftRoleEventLoop = new RaftRoleEventLoop(null, messageSender);
   }

   @Override
   public void start() {

   }

   @Override
   public void stop() {

   }

   @Override
   public void disconnect() {

   }

   @Override
   public void onAppendRequest(AppendRequestHeader header, byte[] data, int offset, int length) {

   }

   @Override
   public void onAppendResponse(AppendResponseHeader header) {

   }

   @Override
   public void onVoteRequest(VoteRequestHeader header) {
      electionManager.onVoteRequest(header);
   }

   @Override
   public void onVoteResponse(VoteResponseHeader header) {
      electionManager.onVoteResponse(header);
   }


}
