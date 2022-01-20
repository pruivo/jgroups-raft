package org.jgroups.raft2.impl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Supplier;

import org.jgroups.util.ExtendedUUID;
import org.jgroups.util.Util;

/**
 * TODO! document this
 *
 * @author Pedro Ruivo
 * @since 12
 */
public class RaftIdImpl implements RaftId {

   private byte[] raftId;

   private RaftIdImpl() {
   }

   public RaftIdImpl(byte[] raftId) {
      this.raftId = Objects.requireNonNull(raftId);
   }

   public RaftIdImpl(String raftId) {
      this(Util.stringToBytes(raftId));
   }

   @Override
   public boolean isSameRaftId(ExtendedUUID addr) {
      return RaftConfigurator.isSameRaftId(raftId, addr);
   }

   @Override
   public int serializedSize() {
      return Util.size(raftId);
   }

   @Override
   public void writeTo(DataOutput dataOutput) throws IOException {
      Util.writeByteBuffer(raftId, dataOutput);
   }

   @Override
   public void readFrom(DataInput dataInput) throws IOException, ClassNotFoundException {
      raftId = Util.readByteBuffer(dataInput);
   }

   @Override
   public Supplier<RaftIdImpl> create() {
      return RaftIdImpl::new;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof RaftIdImpl)) return false;

      RaftIdImpl raftId1 = (RaftIdImpl) o;

      return Arrays.equals(raftId, raftId1.raftId);
   }

   @Override
   public int hashCode() {
      return Arrays.hashCode(raftId);
   }

   @Override
   public String toString() {
      return "RaftIdImpl{" +
            "raftId=" + Util.bytesToString(raftId) +
            '}';
   }
}
