package org.jgroups.raft2.impl.eventloop;

import java.util.Collection;
import java.util.concurrent.Executor;

import org.jgroups.Address;
import org.jgroups.raft2.impl.AbstractEventLoop;
import org.jgroups.raft2.impl.MessageSender;
import org.jgroups.raft2.impl.RaftRole;
import org.jgroups.raft2.impl.election.VoteRequestHeader;
import org.jgroups.raft2.impl.election.VoteResponseHeader;
import org.jgroups.raft2.impl.state.RaftStateEvent;
import org.jgroups.raft2.impl.state.RaftStateListener;

/**
 * TODO! document this
 *
 * @author Pedro Ruivo
 * @since 12
 */
public class RaftRoleEventLoop extends AbstractEventLoop implements RaftStateListener {


   private final MessageSender messageSender;

   private volatile RaftRole role = RaftRole.FOLLOWER;


   public RaftRoleEventLoop(Executor executor, MessageSender messageSender) {
      super(executor);
      this.messageSender = messageSender;
   }

   @Override
   protected void onEvent() {

   }

   @Override
   public void onStateChangeEvent(RaftStateEvent event) {
      if (event.isRoleChanged()) {
         this.role = event.getNewRole();
      }
   }


}
