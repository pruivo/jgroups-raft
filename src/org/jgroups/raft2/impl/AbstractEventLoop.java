package org.jgroups.raft2.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * TODO! document this
 *
 * @author Pedro Ruivo
 * @since 12
 */
public abstract class AbstractEventLoop implements Runnable {

   private final Executor executor;
   private volatile CompletionStage<Void> lastRunnable;
   private final AtomicBoolean eventTriggered = new AtomicBoolean(false);

   protected AbstractEventLoop(Executor executor) {
      this.executor = executor;
   }

   @Override
   public void run() {
      eventTriggered.set(false);
      try {
         onEvent();
      } catch (Throwable t) {
         //log
      }
   }

   protected void triggerEvent() {
      if (eventTriggered.compareAndSet(false, true)) {
         if (lastRunnable == null) {
            lastRunnable = CompletableFuture.runAsync(this, executor);
         } else {
            //noinspection NonAtomicOperationOnVolatileField
            lastRunnable = lastRunnable.thenRunAsync(this, executor);
         }
      }
   }

   protected abstract void onEvent();
}
