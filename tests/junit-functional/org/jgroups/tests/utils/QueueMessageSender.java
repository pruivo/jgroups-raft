package org.jgroups.tests.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.jgroups.raft2.impl.MessageSender;
import org.jgroups.raft2.impl.Raft2Header;
import org.jgroups.raft2.impl.RaftId;

/**
 * TODO! document this
 *
 * @author Pedro Ruivo
 * @since 12
 */
public class QueueMessageSender implements MessageSender {

   private final BlockingDeque<QueuedMessage> messages = new LinkedBlockingDeque<>();

   @Override
   public void sendTo(RaftId dst, Raft2Header header) {
      messages.add(new QueuedMessage(dst, header));
   }

   @Override
   public void sendToAll(Raft2Header header) {
      messages.add(new QueuedMessage(null, header));
   }

   public QueuedMessage poll(long timeout, TimeUnit timeUnit) throws InterruptedException {
      return messages.poll(timeout, timeUnit);
   }

   public static class QueuedMessage {
      private final RaftId dest;
      private final Raft2Header header;

      public QueuedMessage(RaftId dest, Raft2Header header) {
         this.dest = dest;
         this.header = header;
      }

      public RaftId getDest() {
         return dest;
      }

      public Raft2Header getHeader() {
         return header;
      }
   }

}
