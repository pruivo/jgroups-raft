package org.jgroups.raft2.impl.log;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.jgroups.raft2.LogEntry;

/**
 * TODO! document this
 *
 * @author Pedro Ruivo
 * @since 12
 */
public enum InitialLogEntry implements LogEntry {
   INSTANCE;

   @Override
   public int getTerm() {
      return 0;
   }

   @Override
   public int getLogIndex() {
      return 0;
   }

   @Override
   public int serializedSize() {
      return 0;
   }

   @Override
   public void writeTo(DataOutput dataOutput) throws IOException {

   }

   @Override
   public void readFrom(DataInput dataInput) throws IOException, ClassNotFoundException {

   }
}
