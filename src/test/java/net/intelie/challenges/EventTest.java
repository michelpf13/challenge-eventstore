package net.intelie.challenges;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/**
 * In all tests myEvents is the expected result array for each test.
 *
 * For the concurrency tests a ExecutorService is used. ExecutorService is a JDK API that simplifies running tasks in asynchronous mode.
 * ExecutorService automatically provides a pool of threads and an API for assigning tasks to it.
 */
public class EventTest {
    private Event event1 = new Event("some_type", 123L);
    private Event event2 = new Event("some_type5", 123L);
    private Event event3 = new Event("some_type7", 123L);
    private Event event4 = new Event("some_type2", 123L);
    private Event event5 = new Event("some_type", 312L);

    /**
     * Insert Test.
     */
    @Test
    public void insertTest () throws Exception{
        Solution solution = new Solution();

        List<Event> myEvents = new ArrayList<Event>() {{
            add(event1);
            add(event1);
            add(event1);
            add(event2);
            add(event3);
            add(event3);
            add(event5);
            add(event4);
        }};

        myEvents.sort(Comparator.comparing(Event::type).thenComparing(Event::timestamp));

        solution.insert(event1);
        solution.insert(event1);
        solution.insert(event1);
        solution.insert(event2);
        solution.insert(event3);
        solution.insert(event3);
        solution.insert(event5);
        solution.insert(event4);

        assertEquals(solution.result(), myEvents);
    }

    /**
     * Remove All Test
     */

    @Test
    public void removeAllTest() throws Exception{
        Solution solution = new Solution();

        List<Event> myEvents = new ArrayList<Event>() {{
            add(event1);
            add(event1);
            add(event1);
            add(event2);
            add(event5);
            add(event4);
        }};

        myEvents.sort(Comparator.comparing(Event::type).thenComparing(Event::timestamp));

        solution.insert(event1);
        solution.insert(event1);
        solution.insert(event1);
        solution.insert(event2);
        solution.insert(event3);
        solution.insert(event3);
        solution.insert(event5);
        solution.insert(event4);

        solution.removeAll("some_type7");

        assertEquals(solution.result(), myEvents);
    }

    /**
     * Query Test
     */

    @Test
    public void queryTest() throws Exception{
        Solution solution = new Solution();

        List<Event> myEvents = new ArrayList<Event>() {{
            add(event1);
            add(event1);
            add(event1);
            add(event2);
            add(event3);
            add(event3);
            add(event4);
        }};

        myEvents.sort(Comparator.comparing(Event::type).thenComparing(Event::timestamp));

        solution.insert(event1);
        solution.insert(event1);
        solution.insert(event1);
        solution.insert(event2);
        solution.insert(event3);
        solution.insert(event3);
        solution.insert(event5);
        solution.insert(event4);

        EventIterator query = solution.query("some_type",124L, 313L);

        query.moveNext();

        query.remove();

        assertEquals(solution.result(), myEvents);
    }


    /**
     * Concurrent Insert Test
     */

    @Test
    public void concurrentInsertTest() throws Exception {
        Solution solution = new Solution();

        List<Event> myEvents = new ArrayList<Event>() {{
            add(event1);
            add(event1);
            add(event1);
            add(event2);
            add(event3);
            add(event3);
            add(event5);
            add(event4);
        }};

        myEvents.sort(Comparator.comparing(Event::type).thenComparing(Event::timestamp));

        ExecutorService service = Executors.newFixedThreadPool(10);

        service.execute(() -> {
            solution.insert(event1);
            solution.insert(event1);
            solution.insert(event1);
            solution.insert(event2);
            solution.insert(event3);
            solution.insert(event3);
            solution.insert(event5);
            solution.insert(event4);
        });

        service.shutdown();

        service.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

        assertEquals(solution.result(), myEvents);
    }

    /**
     * Concurrent Remove All Test
     */

    @Test
    public void concurrentRemoveAllTest() throws Exception {
        Solution solution = new Solution();

        List<Event> myEvents = new ArrayList<Event>() {{
            add(event1);
            add(event2);
            add(event5);
            add(event4);
            add(event3);
            add(event3);
            add(event3);
            add(event3);
            add(event3);
        }};

        myEvents.sort(Comparator.comparing(Event::type).thenComparing(Event::timestamp));

        ExecutorService service = Executors.newFixedThreadPool(10);

        service.execute(() -> {
            solution.insert(event1);
            solution.insert(event2);
            solution.insert(event3);
            solution.insert(event5);
            solution.insert(event4);
            solution.removeAll("some_type7");
            solution.insert(event3);
            solution.insert(event3);
            solution.insert(event3);
            solution.insert(event3);
            solution.insert(event3);
        });

        service.shutdown();

        service.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

        assertEquals(solution.result(), myEvents);
    }

    /**
     * Concurrent Query Test
     */

    @Test
    public void concurrentQueryTest() throws InterruptedException {
        Solution solution = new Solution();

        List<Event> myEvents = new ArrayList<Event>() {{
            add(event1);
            add(event1);
            add(event1);
            add(event2);
            add(event5);
            add(event3);
            add(event3);
        }};

        myEvents.sort(Comparator.comparing(Event::type).thenComparing(Event::timestamp));

        solution.insert(event1);
        solution.insert(event1);
        solution.insert(event1);
        solution.insert(event2);
        solution.insert(event3);
        solution.insert(event3);
        solution.insert(event5);
        solution.insert(event4);

        ExecutorService service = Executors.newFixedThreadPool(10);

        service.execute(() -> {
            EventIterator query = solution.query("some_type2", 120L, 315L);
            query.moveNext();
            query.remove();
        });

        service.shutdown();

        service.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

        assertEquals(solution.result(), myEvents);
    }
}