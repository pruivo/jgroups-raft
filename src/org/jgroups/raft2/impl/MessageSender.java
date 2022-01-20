package org.jgroups.raft2.impl;

import java.util.Collection;

import org.jgroups.Address;

/**
 * TODO! document this
 *
 * @author Pedro Ruivo
 * @since 12
 */
public interface MessageSender { ;

   void sendTo(RaftId dst, Raft2Header header);

   void sendToAll(Raft2Header header);
}
