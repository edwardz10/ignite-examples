package org.apache.ignite.datagrid;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.transactions.Transaction;

import java.util.concurrent.locks.Lock;

public class DataGrid {

    private static void putAndGet(IgniteCache<Integer, String> cache) {
        for (int i = 0; i < 10; i++) {
            cache.put(i, Integer.toString(i));
        }

        for (int i = 0; i < 10; i++)
            System.out.println("Got [key=" + i + ", val=" + cache.get(i) + ']');
    }

    private static void atomicOperations(IgniteCache<String, Integer> cache) {
        // Put-if-absent which returns previous value.
        Integer oldVal = cache.getAndPutIfAbsent("Hello", 11);

        // Put-if-absent which returns boolean success flag.
        boolean success = cache.putIfAbsent("World", 22);

        // Replace-if-exists operation (opposite of getAndPutIfAbsent), returns previous value.
        oldVal = cache.getAndReplace("Hello", 11);

        // Replace-if-exists operation (opposite of putIfAbsent), returns boolean success flag.
        success = cache.replace("World", 22);

        // Replace-if-matches operation.
        success = cache.replace("World", 2, 22);

        // Remove-if-matches operation.
        success = cache.remove("Hello", 1);
    }

    private static void transactions(Ignite ignite, IgniteCache<String, Integer> cache) {
        try (Transaction tx = ignite.transactions().txStart()) {
            Integer hello = cache.get("Hello");

            if (hello == 1)
                cache.put("Hello", 11);

            cache.put("World", 22);

            tx.commit();
        }
    }

    private static void distributedLocks(IgniteCache<String, Integer> cache) {
        // Lock cache key "Hello".
        Lock lock = cache.lock("Hello");

        lock.lock();

        try {
            cache.put("Hello", 11);
            cache.put("World", 22);
        }
        finally {
            lock.unlock();
        }
    }

    public static void main(String[] args) {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            IgniteCache<Integer, String> integerStringCache = ignite.getOrCreateCache("IntegerStringCache");
            IgniteCache<String, Integer> stringIntegerCache = ignite.getOrCreateCache("StringIntegerCache");

            putAndGet(integerStringCache);
            atomicOperations(stringIntegerCache);
            transactions(ignite, stringIntegerCache);
            distributedLocks(stringIntegerCache);
        }
    }
}
