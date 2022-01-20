package org.jgroups.raft2.impl.state;

import org.jgroups.raft2.impl.RaftRole;

/**
 * TODO! document this
 *
 * @author Pedro Ruivo
 * @since 12
 */
public interface RaftStateListener {

   void onStateChangeEvent(RaftStateEvent event);

}
