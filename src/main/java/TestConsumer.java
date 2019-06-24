import config.ConfigurationKafka;
import utils.KakfaConsumer;

public class TestConsumer {

    private static final int NUM_CONSUMERS = 3;


    public static void main (String args[]) {


        KakfaConsumer consumer = new KakfaConsumer(-1, ConfigurationKafka.TOPIC);
        System.out.println("=== Listing topics === ");
        consumer.listTopics();
        System.out.println("=== === === === === === ");

        for (int i = 0; i < NUM_CONSUMERS; i++) {
            Thread c = new Thread(new KakfaConsumer(i, ConfigurationKafka.TOPIC));
            c.start();
        }
    }

}
