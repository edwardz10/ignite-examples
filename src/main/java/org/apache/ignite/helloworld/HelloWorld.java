package org.apache.ignite.helloworld;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;

public class HelloWorld {

    public static void main(String[] args) throws IgniteException {
        try (Ignite ignite = Ignition.start("examples\\config\\example-ignite.xml")) {
            final IgniteCache<Integer, String> cache = ignite.getOrCreateCache("myCache");

            cache.put(1, "Hello");
            cache.put(2, "World");

            ignite.compute().broadcast(() -> System.out.println(cache.get(1) + " " + cache.get(2)));
        }
    }
}
