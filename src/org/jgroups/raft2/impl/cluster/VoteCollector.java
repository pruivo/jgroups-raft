package org.jgroups.raft2.impl.cluster;

import java.util.Collection;

import org.jgroups.Address;
import org.jgroups.raft2.impl.RaftId;

/**
 * TODO! document this
 *
 * @author Pedro Ruivo
 * @since 12
 */
public interface VoteCollector {

   boolean addVote(RaftId member);

   int getTerm();

}
