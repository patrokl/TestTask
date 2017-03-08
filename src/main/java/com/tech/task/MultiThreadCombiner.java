package com.tech.task;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by Illia on 2017-03-06.
 */
public class MultiThreadCombiner<T> extends Combiner<T> {

    protected List<Worker> workers;

    public MultiThreadCombiner(SynchronousQueue<T> outputQueue) {
        super(outputQueue);
        workers = new ArrayList<Worker>();
    }

    public void addInputQueue(BlockingQueue<T> queue, double priority, long isEmptyTimeout, TimeUnit timeUnit) throws CombinerException {
        //validateInputData all input params
        validateInputData(queue, priority, isEmptyTimeout, timeUnit);
        //create new worker
        Worker worker = new Worker(queue, priority, isEmptyTimeout, timeUnit);
        workers.add(worker);
        //notify all workers to update sleepTimeout for all workers
        notifyAllWorkers();
        //Start new thread
        Thread thread = new Thread(worker);
        thread.setName("Prior " + priority + ": ");
        thread.start();
    }

    public void removeInputQueue(BlockingQueue<T> queue) throws CombinerException {
        //Create fake worker to use in workers collection
        Worker worker = new Worker(queue);
        int i = workers.indexOf(worker);
        //Throw worker wasn't found throw exception
        if (i == -1) throw new CombinerException("Queue can't be removed. It is absent in Combiner.");
        //Get real worker. Stop it and notify other
        worker = workers.get(i);
        worker.isInterrupted = true;
        workers.remove(worker);
        notifyAllWorkers();
    }

    public boolean hasInputQueue(BlockingQueue<T> queue) {
        return workers.contains(new Worker(queue));
    }

    protected void notifyAllWorkers() {
        double totalPrior = workers.stream().mapToDouble(w -> w.priority).sum();
        workers.forEach(w -> w.notifyWithNewTotalPrior(totalPrior));
    }

    protected void validateInputData(BlockingQueue<T> queue, double priority, long isEmptyTimeout, TimeUnit timeUnit) throws CombinerException {
        if (queue == null) throw new CombinerException("Input queue can't be null");
        if (priority <= 0) throw new CombinerException("Priority queue can't have negative value or 0");
        if (isEmptyTimeout <= 0) throw new CombinerException("Empty timeout can't have negative value or 0");
        if (timeUnit == null) throw new CombinerException("timeUnit can't be null");
    }

    protected class Worker implements Runnable {
        static final long TIME_INTERVAL = 1000;
        BlockingQueue<T> queue;
        double priority;
        long emptyTimeout;
        TimeUnit timeUnit;
        volatile long sleepTimeout;
        volatile boolean isInterrupted;

        protected Worker(BlockingQueue<T> queue, double priority, long emptyTimeout, TimeUnit timeUnit) {
            this.queue = queue;
            this.priority = priority;
            this.emptyTimeout = emptyTimeout;
            this.timeUnit = timeUnit;
        }

        protected Worker(BlockingQueue<T> queue) {
            this.queue = queue;
        }


        protected void notifyWithNewTotalPrior(double totalPrior) {
            sleepTimeout = (long) (TIME_INTERVAL / ((priority / totalPrior) * 100));
        }

        public void run() {
            while (true) {
                T message;
                try {
                    Thread.sleep(sleepTimeout);
                    if (isInterrupted) throw new InterruptedException();
                    message = queue.poll(emptyTimeout, timeUnit);
                    if(message == null) throw new InterruptedException();
                    System.out.println(Thread.currentThread().getName() + message);
                    outputQueue.put(message);
                } catch (InterruptedException e) {
                    System.out.println(Thread.currentThread().getName() + " was interrupted");
                    try {
                        removeInputQueue(queue);
                    } catch (CombinerException e1) {
                        e1.printStackTrace();
                    }
                    break;
                }
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Worker worker = (Worker) o;

            return queue != null ? queue.equals(worker.queue) : worker.queue == null;

        }

        @Override
        public int hashCode() {
            return queue != null ? queue.hashCode() : 0;
        }
    }


}
