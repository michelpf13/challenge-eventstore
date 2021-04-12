package net.intelie.challenges;


import java.util.*;
import java.util.function.Predicate;

/**
 *  An alternative option for the Synchronized List would be to use the Hashmap (or ConcurrentHashMap) class. In this case, the type would be used as the key and an Event Arraylist would be used as the value.
 *  So removal would be made easier. However, for each type a new ArrayList would need to be created for insertion.
 *
 *  Therefore, the Synchronized List was chosen for the task, as it is easily implemented and simple to understand, in addition to working well for the problem. It is only necessary
 *  to ensure that the reference of the list does not change during execution.
 */


public class Solution implements EventStore {
    private List<Event> events;

    /**
     * Collections.synchronizedList      Decorates another List to synchronize its behaviour for a multi-threaded environment. Methods are synchronized, then forwarded to the decorated list. https://commons.apache.org/proper/commons-collections/javadocs/api-3.2.2/org/apache/commons/collections/list/SynchronizedList.html
     */

    Solution(){
        events = new ArrayList<>();
        events = Collections.synchronizedList(events);
    }

    /**
     *
     * @param event         Event object.
     *
     * Synchronizes the add method of the event list.
     */

    @Override
    public void insert(Event event) {
        synchronized (events) {
            events.add(event);
        }
    }

    /**
     *
     * @param type       Removes all events of the same type.
     *
     * Creates an auxiliary list to store the events that will be deleted. That way, if there is an interruption during execution, the status of the main list will not be partially modified.
     */

    @Override
    public void removeAll(String type) {
        List<Event> removed = new ArrayList<>();
        synchronized (events){
            for (Event event : events) {
                if (event.type().equals(type)) removed.add(event);
            }
            if (removed.size()>0) {
                events.removeAll(removed);
            }
        }

    }

    /**
     *
     * @param type      The type we are querying for.
     * @param startTime Start timestamp (inclusive).
     * @param endTime   End timestamp (exclusive).
     * @return The query result. An inner class that implements the EventIterator interface.
     *
     * The predicate returns elements with the type and time interval passed as parameter. Then creates an iterator over the list of events filtered according to the predicate condition.
     *
     */

    @Override
    public EventIterator query(String type, long startTime, long endTime) {
        Predicate<Event> condition = event -> type.equals(event.type()) && (event.timestamp() >= startTime && event.timestamp() < endTime);
        Iterator<Event> iterator = events.stream().filter(condition).iterator();

        return new MyIterator(iterator);
    }

    /**
     *
     * @return Just a print formatted method.
     */

    @Override
    public String toString() {
        StringBuilder print = new StringBuilder();

        events.sort(Comparator.comparing(Event::type).thenComparing(Event::timestamp));

        for (Event event : events) {
            print.append(event.type());
            print.append(", ");
            print.append(event.timestamp());
            print.append("\n");
        }
        return print.toString();
    }

    /**
     *
     * @return Result method for comparison in tests.
     */

    List<Event> result(){
        events.sort(Comparator.comparing(Event::type).thenComparing(Event::timestamp));

        return events;
    }

    /**
     * Inner class that implements the EventIterator interface. Implements methods for traversing, returning and removing events within a specific query.
     *
     * Event variable to store the current event and boolean for controlling the call to the moveNext() function
     */

    public class MyIterator implements EventIterator {
        private Iterator<Event> queryIterator;
        private Event currentEvent;
        private boolean moveState;

        MyIterator(Iterator<Event> eventIterator) {
            queryIterator = eventIterator;
            currentEvent = null;
            moveState = false;
        }

        /**
         *
         * @return True or False. Moves the iterator to the next query element, if any.
         */

        @Override
        public boolean moveNext() {
            if (queryIterator.hasNext()) {
                moveState = true;
                currentEvent = queryIterator.next();
                return true;
            }
            else {
                moveState = false;
                currentEvent = null;
                return false;
            }
        }

        /**
         *
         * @return Returns the current event that the iterator is pointing to.
         * If moveNext has never been called or your last result was false, throw an exception.
         */

        @Override
        public Event current() {
            if (!moveState) throw new IllegalStateException();
            return currentEvent;
        }

        /**
         * Removes the current event from the query. Synchronizes access to the event list to avoid concurrency problems.
         * If moveNext has never been called or your last result was false, throw an exception.
         */

        @Override
        public void remove() {
            if (!moveState) throw new IllegalStateException();
            synchronized (events){
                events.remove(currentEvent);
            }
        }

        @Override
        public void close() throws Exception {
            // No implementation required.
        }
    }

}
