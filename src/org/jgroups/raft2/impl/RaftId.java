package org.jgroups.raft2.impl;

import org.jgroups.Constructable;
import org.jgroups.util.ExtendedUUID;
import org.jgroups.util.SizeStreamable;

/**
 * TODO! document this
 *
 * @author Pedro Ruivo
 * @since 12
 */
public interface RaftId extends SizeStreamable, Constructable {

   boolean isSameRaftId(ExtendedUUID addr);

}
