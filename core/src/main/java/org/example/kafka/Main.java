package org.example.kafka;

public class Main {

    public static void main(String[] args) {


        MyProducer producer = new MyProducer();
        producer.transaction();

        Thread thread = new Thread(new MyConsumer());
        thread.start();
    }
}
