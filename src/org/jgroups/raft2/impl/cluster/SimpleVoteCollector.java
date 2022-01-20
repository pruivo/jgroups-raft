package org.jgroups.raft2.impl.cluster;

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.jgroups.raft2.impl.RaftId;

/**
 * TODO! document this
 *
 * @author Pedro Ruivo
 * @since 12
 */
public class SimpleVoteCollector implements VoteCollector{


   private final int term;
   private final Collection<RaftId> cluster;
   private final int majority;
   private final Collection<RaftId> acks = ConcurrentHashMap.newKeySet();

   public SimpleVoteCollector(int term, Collection<RaftId> cluster) {
      this.term = term;
      this.cluster = Objects.requireNonNull(cluster);
      this.majority = ClusterConfiguration.majority(cluster.size());
   }


   @Override
   public boolean addVote(RaftId member) {
      if (cluster.contains(member) && acks.add(member)) {
         return acks.size() >= majority;
      }
      return false;
   }

   @Override
   public int getTerm() {
      return term;
   }

   @Override
   public String toString() {
      return "SimpleVoteCollector{" +
            "term=" + term +
            ", majority=" + majority +
            ", acks=" + acks +
            '}';
   }
}
