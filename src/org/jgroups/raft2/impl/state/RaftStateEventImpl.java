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
class RaftStateEventImpl implements RaftStateEvent {

   private final int oldTerm;
   private final int newTerm;
   private final RaftRole oldRole;
   private final RaftRole newRole;
   private final RaftId oldLeader;
   private final RaftId newLeader;

   RaftStateEventImpl(int oldTerm, int newTerm, RaftRole oldRole, RaftRole newRole, RaftId oldLeader, RaftId newLeader) {
      this.oldTerm = oldTerm;
      this.newTerm = newTerm;
      this.oldRole = Objects.requireNonNull(oldRole);
      this.newRole = Objects.requireNonNull(newRole);
      this.oldLeader = oldLeader;
      this.newLeader = newLeader;
   }

   @Override
   public int getOldTerm() {
      return oldTerm;
   }

   @Override
   public int getNewTerm() {
      return newTerm;
   }

   @Override
   public RaftRole getOldRole() {
      return oldRole;
   }

   @Override
   public RaftRole getNewRole() {
      return newRole;
   }

   @Override
   public RaftId getOldLeader() {
      return oldLeader;
   }

   @Override
   public RaftId getNewLeader() {
      return newLeader;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof RaftStateEventImpl)) return false;

      RaftStateEventImpl that = (RaftStateEventImpl) o;

      if (getOldTerm() != that.getOldTerm()) return false;
      if (getNewTerm() != that.getNewTerm()) return false;
      if (getOldRole() != that.getOldRole()) return false;
      if (getNewRole() != that.getNewRole()) return false;
      if (getOldLeader() != null ? !getOldLeader().equals(that.getOldLeader()) : that.getOldLeader() != null)
         return false;
      return getNewLeader() != null ? getNewLeader().equals(that.getNewLeader()) : that.getNewLeader() == null;
   }

   @Override
   public int hashCode() {
      int result = getOldTerm();
      result = 31 * result + getNewTerm();
      result = 31 * result + getOldRole().hashCode();
      result = 31 * result + getNewRole().hashCode();
      result = 31 * result + (getOldLeader() != null ? getOldLeader().hashCode() : 0);
      result = 31 * result + (getNewLeader() != null ? getNewLeader().hashCode() : 0);
      return result;
   }

   @Override
   public String toString() {
      return "RaftStateEventImpl{" +
            "oldTerm=" + oldTerm +
            ", newTerm=" + newTerm +
            ", oldRole=" + oldRole +
            ", newRole=" + newRole +
            ", oldLeader=" + oldLeader +
            ", newLeader=" + newLeader +
            '}';
   }
}
