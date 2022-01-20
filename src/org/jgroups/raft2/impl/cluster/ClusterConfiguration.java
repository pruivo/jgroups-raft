package org.jgroups.raft2.impl.cluster;

import java.util.Collection;

import org.jgroups.View;
import org.jgroups.raft2.impl.RaftId;

/**
 * TODO! document this
 *
 * @author Pedro Ruivo
 * @since 12
 */
public interface ClusterConfiguration {

   VoteCollector newVoteCollector(int term);

   void handleView(View newView);

   Collection<RaftId> cluster();

   static int majority(int clusterSize) {
      return (clusterSize / 2) + 1;
   }
}
