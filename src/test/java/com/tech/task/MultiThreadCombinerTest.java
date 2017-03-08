package com.tech.task;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.*;
import static org.junit.Assert.*;

/**
 * Created by Illia on 2017-03-07.
 */
public class MultiThreadCombinerTest {

    private MultiThreadCombiner<Integer> combiner;
    private SynchronousQueue<Integer> strings;

    @Before
    public void setUp() {
        strings = new SynchronousQueue<>();
        combiner = new MultiThreadCombiner<>(strings);
    }

    @Test(expected = Combiner.CombinerException.class)
    public void validateInputData_Should_ThrowException_When_QueueNull() throws Combiner.CombinerException {
        combiner.validateInputData(null, 1, 1, MILLISECONDS);
    }

    @Test(expected = Combiner.CombinerException.class)
    public void validateInputData_Should_ThrowException_When_PriorityLess0() throws Combiner.CombinerException {
        combiner.validateInputData(new SynchronousQueue<>(), -1, 1, MILLISECONDS);
    }

    @Test(expected = Combiner.CombinerException.class)
    public void validateInputData_Should_ThrowException_When_Priority0() throws Combiner.CombinerException {
        combiner.validateInputData(new SynchronousQueue<>(), 0, 1, MILLISECONDS);
    }

    @Test(expected = Combiner.CombinerException.class)
    public void validateInputData_Should_ThrowException_When_EmptyTimeoutLess0() throws Combiner.CombinerException {
        combiner.validateInputData(new SynchronousQueue<>(), 1, -1, MILLISECONDS);
    }

    @Test(expected = Combiner.CombinerException.class)
    public void validateInputData_Should_ThrowException_When_EmptyTimeout0() throws Combiner.CombinerException {
        combiner.validateInputData(new SynchronousQueue<>(), 1, 0, MILLISECONDS);
    }

    @Test(expected = Combiner.CombinerException.class)
    public void validateInputData_Should_ThrowException_When_TimeUnitNull() throws Combiner.CombinerException {
        combiner.validateInputData(new SynchronousQueue<>(), 1, 1, null);
    }

    @Test
    public void notifyAllWorkers_Should_ChangeSleepTimeout() throws Combiner.CombinerException {
        MultiThreadCombiner<Integer>.Worker worker20 = combiner.new Worker(new SynchronousQueue<>(), 20, 1, MINUTES);
        MultiThreadCombiner<Integer>.Worker worker40 = combiner.new Worker(new SynchronousQueue<>(), 40, 1, MINUTES);

        combiner.workers.add(worker20);
        combiner.workers.add(worker40);

        assertEquals(0, worker20.sleepTimeout);
        assertEquals(0, worker40.sleepTimeout);

        combiner.notifyAllWorkers();

        assertTrue(worker20.sleepTimeout > 0);
        assertTrue(worker40.sleepTimeout > 0);
    }

    @Test
    public void hasInputQueue_Should_ReturnTrue_When_QueuePresent() {
        SynchronousQueue<Integer> queue = new SynchronousQueue<>();
        MultiThreadCombiner<Integer>.Worker worker = combiner.new Worker(queue, 10, 1, MINUTES);
        combiner.workers.add(worker);

        assertTrue(combiner.hasInputQueue(queue));
    }

    @Test
    public void hasInputQueue_Should_ReturnFalse_When_QueuePresent() {
        SynchronousQueue<Integer> queue = new SynchronousQueue<>();
        MultiThreadCombiner<Integer>.Worker worker = combiner.new Worker(queue, 10, 1, MINUTES);
        combiner.workers.add(worker);

        SynchronousQueue<Integer> newQueue = new SynchronousQueue<>();
        assertFalse(combiner.hasInputQueue(newQueue));
    }

    @Test
    public void addInputQueue_Should_StartWorkOfCombiner_When_AddedNewQueue() throws Combiner.CombinerException, InterruptedException {
        int total = 10;


        List<Integer> recorder = new ArrayList<>(total);
        startReadFromOutputQueue(recorder,total);

        BlockingQueue<Integer> queue = new ArrayBlockingQueue<>(total);
        combiner.addInputQueue(queue, 10, 1, SECONDS);


        for (int i = 0; i < total; i++) {
            queue.offer(i);
        }

        Thread.sleep(2000);

        String resStr = recorder.stream().map(Object::toString).collect(Collectors.joining());
        assertEquals("0123456789", resStr);
    }

    @Test
    public void addInputQueue_Should_StartAnotherWorker_When_SecondQueueAdded() throws Combiner.CombinerException, InterruptedException {
        int total = 10;

        List<Integer> recorder = new ArrayList<>(total);
        startReadFromOutputQueue(recorder,total);

        BlockingQueue<Integer> queue1 = new ArrayBlockingQueue<>(total/2);
        combiner.addInputQueue(queue1, 10, 1, SECONDS);

        BlockingQueue<Integer> queue2 = new ArrayBlockingQueue<>(total/2);
        combiner.addInputQueue(queue2, 10, 1, SECONDS);


        for (int i = 0; i < total/2; i++) {
            queue1.offer(i);
            queue2.offer(i+5);
        }

        Thread.sleep(2000);

        assertEquals(total, recorder.size());
        for (int i = 0; i < total; i++) {
            assertTrue(recorder.contains(i));
        }
    }

    @Test
    public void Worker_Should_BeDependOnPriority_When_FewQueueAdded() throws Combiner.CombinerException, InterruptedException {
        int total = 100;

        List<Integer> recorder = new ArrayList<>(total);
        startReadFromOutputQueue(recorder,total);

        BlockingQueue<Integer> queue1 = new ArrayBlockingQueue<>(total);
        int priority1 = 1;
        combiner.addInputQueue(queue1, priority1, 1, SECONDS);

        BlockingQueue<Integer> queue3 = new ArrayBlockingQueue<>(total);
        int priority3 = 3;
        combiner.addInputQueue(queue3, priority3, 1, SECONDS);

        BlockingQueue<Integer> queue7 = new ArrayBlockingQueue<>(total);
        int priority7 = 7;
        combiner.addInputQueue(queue7, priority7, 1, SECONDS);


        for (int i = 0; i < total; i++) {
            queue1.offer(priority1);
            queue3.offer(priority3);
            queue7.offer(priority7);
        }

        Thread.sleep(5000);

        assertEquals(total, recorder.size());
        long counter1 = recorder.stream().filter(s -> s.equals(priority1)).count();
        long counter3 = recorder.stream().filter(s -> s.equals(priority3)).count();
        long counter7 = recorder.stream().filter(s -> s.equals(priority7)).count();
        assertTrue(counter7>counter3 && counter3>counter1);
    }


    @Test
    public void Worker_Should_StopWork_When_QueueIsEmptyLongerThenTimeout() throws Combiner.CombinerException, InterruptedException {
        int total = 5;

        List<Integer> recorder = new ArrayList<>(total);
        startReadFromOutputQueue(recorder,total);

        BlockingQueue<Integer> queue1 = new ArrayBlockingQueue<>(total);
        combiner.addInputQueue(queue1, 1, 5, SECONDS);


        for (int i = 0; i < total-1; i++) {
            queue1.offer(i);
        }

        Thread.sleep(7000);

        assertEquals(total-1, recorder.size());
        assertTrue(combiner.workers.size() == 0);
    }

    @Test(expected = Combiner.CombinerException.class)
    public void removeInputQueue_Should_ThrowException_When_RemovingNotExistingQueue() throws Combiner.CombinerException {
        SynchronousQueue<Integer> queue = new SynchronousQueue<>();
        MultiThreadCombiner<Integer>.Worker worker = combiner.new Worker(queue, 10, 1, MINUTES);
        combiner.workers.add(worker);

        SynchronousQueue<Integer> notExistingQueue = new SynchronousQueue<>();
        combiner.removeInputQueue(notExistingQueue);
    }

    @Test
    public void removeInputQueue_Should_RemoveQueue() throws Combiner.CombinerException {
        SynchronousQueue<Integer> queue = new SynchronousQueue<>();
        MultiThreadCombiner<Integer>.Worker worker = combiner.new Worker(queue, 10, 1, MINUTES);
        combiner.workers.add(worker);

        combiner.removeInputQueue(queue);

        assertTrue(worker.isInterrupted);
        assertTrue(combiner.workers.isEmpty());
    }


    private void startReadFromOutputQueue(List<Integer> recorder, int expected){
        Runnable runnable = () -> {
            try {
                while (true) {
                    Integer message = strings.take();
                    if (message != null) recorder.add(message);
                    if (recorder.size() == expected) break;
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        };
        new Thread(runnable).start();
    }


}
