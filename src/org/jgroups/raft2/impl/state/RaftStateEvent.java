package org.jgroups.raft2.impl.state;

import java.util.Objects;

import org.jgroups.raft2.impl.RaftId;
import org.jgroups.raft2.impl.RaftRole;

/**
 * TODO! document this
 *
 * @author Pedro Ruivo
 * @since 12
 */
public interface RaftStateEvent {

   int getOldTerm();

   int getNewTerm();

   default boolean isTermChanged() {
      return getOldTerm() != getNewTerm();
   }

   RaftRole getOldRole();

   RaftRole getNewRole();

   default boolean isRoleChanged() {
      return getOldRole() != getNewRole();
   }

   RaftId getOldLeader();

   RaftId getNewLeader();

   default boolean isLeaderChanged() {
      return Objects.equals(getOldLeader(), getNewLeader());
   }

}
