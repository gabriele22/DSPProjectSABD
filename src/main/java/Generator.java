import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import com.google.gson.Gson;
import config.ConfigurationKafka;
import utils.Comment;
import utils.KakfaProducer;


public class Generator implements Runnable{

    /*
     * This datasource reads data from the debs2015 grand challenge
     * and sends them trying to preserve inter-arrival times.
     *
     * We define as real time, the time experienced by the data source
     * while replying the data set. We define as event time, the time
     * associated with each event stored in the data set, which should
     * cover the whole 2013.
     *
     * We read the dataset with a granularity of TIMESPAN minutes at once,
     * i.e., we read all the events in TIMESPAN minutes of the simulated
     * time at once.
     *
     * Relying on TIMESPAN (expressed in minutes), it is possible to
     * accelerate the dataset by reading multiple events at once.
     *
     * Relying on SPEEDUP (expressed in milliseconds), it is possible to
     * define the (real) time interval between two consecutive minutes of
     * the simulated time. Using -1 as value, the dataset is reproduced
     * with waiting times.
     *
     */
//ad esempio se timespan è 15 minuti e speedup è 1000ms è come se 15 minuti diventassero un secondo
    private static final int TIMESPAN = 15; 		// expressed in mins
    private static final int SPEEDUP = 1000; 	// expressed in ms

    private String filename;
    private Gson gson;

    public Generator(String filename){
        this.filename = filename;
        this.gson = new Gson();
    }

    @Override
    public void run() {



        BufferedReader br = null;
        LinesBatch linesBatch = null;

        try {
            System.out.println("Initializing... ");
            br = new BufferedReader(new FileReader(filename));
            String header = br.readLine();
            System.out.println("HEADER: "+ header);

            String line = br.readLine();
            linesBatch = new LinesBatch();
            long batchInitialTime 	= roundToCompletedMinute(getDropoffDatatime(line));
            long batchFinalTime 	= computeBatchFinalTime(batchInitialTime);
            long latestSendingTime 	= System.currentTimeMillis();
            System.out.println(" batch init  " + new Date(batchInitialTime).toString());
            System.out.println(" batch final " + new Date(batchFinalTime).toString());

            System.out.println("Read: " + line);
            linesBatch.addLine(line);

            while ((line = br.readLine()) != null) {

                long eventTime = getDropoffDatatime(line);

                if (eventTime < batchFinalTime){
                    linesBatch.addLine(line);
                    continue;
                }

                System.out.println("Sending " + linesBatch.getLines().size() + " lines");

                /* batch is concluded and has to be sent */
                send(linesBatch);

                /* sleep if needed */
                if (SPEEDUP != -1){
                    long nextBatchInitTime = roundToCompletedMinute(eventTime);
                    long completeIntervalToSkip = SPEEDUP * (int) Math.floor(((double) (nextBatchInitTime - batchFinalTime) / (TIMESPAN * 60 * 1000)));
                    long deltaIntervalToSkip 	= SPEEDUP - (System.currentTimeMillis() - latestSendingTime);

                    System.out.println(" sleep for d:" + deltaIntervalToSkip + " + c:" +completeIntervalToSkip);

/*                    if (deltaIntervalToSkip < 0){
                        System.out.println("WARNING: consumer is slower than source. A backpressure mechanism has been activated.");
                        deltaIntervalToSkip = 0;
                    }*/

                    try {
                        Thread.sleep(deltaIntervalToSkip + completeIntervalToSkip);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                }

                /* update batch */
                linesBatch = new LinesBatch();
                linesBatch.addLine(line);
                batchInitialTime = roundToCompletedMinute(eventTime);
                batchFinalTime 	= computeBatchFinalTime(batchInitialTime);
                latestSendingTime 	= System.currentTimeMillis();

                System.out.println(" batch init  " + new Date(batchInitialTime));
                System.out.println(" batch final " + new Date(batchFinalTime));

            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (br != null){
            try {
                br.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    private void send(LinesBatch linesBatch) {
        KakfaProducer producer = new KakfaProducer(ConfigurationKafka.TOPIC);

        try {
            for (int i = 0; i < linesBatch.getLines().size(); i++) {
                String[] tokens	=	linesBatch.getLines().get(i).split(",");

                Comment comment = new Comment(tokens[0],tokens[1],tokens[2],tokens[3],
                        tokens[4],tokens[5],tokens[6],tokens[7],tokens[8],tokens[9],
                        tokens[10],tokens[11],tokens[12],tokens[13],tokens[14]);

                String jsonString = gson.toJson(comment);

                producer.produce(tokens[3], jsonString);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }

    }

    private long getDropoffDatatime(String line){

        String[] tokens	=	line.split(",");
        //METTERE 5!
        long ts = Long.valueOf(tokens[5])*1000;
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        Date d= new Date(ts);
        return d.getTime();

    }

    private long roundToCompletedMinute(long timestamp) {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        Date d = new Date(timestamp);
        Calendar date = new GregorianCalendar();
        date.setTime(d);
        date.set(Calendar.SECOND, 0);
        date.set(Calendar.MILLISECOND, 0);

        return date.getTime().getTime();

    }

    private long computeBatchFinalTime(long initialTime){

        return initialTime + TIMESPAN * 60 * 1000;
    }


    /**
     * This component reads data from the debs dataset
     * and feeds the Storm topology by publishing data
     * on Redis.
     *
     * @param args
     */
    public static void main(String[] args) {

        /* TODO:
         * Usage:
         * java -jar debs2015gc-1.0.jar it.uniroma2.debs2016gc.DataSource [debs dataset] [redis ip]
         */

        String file = args[0];
      //  String file = "/home/gabriele/Scrivania/comment.csv";
        Generator fill = new Generator (file);
        Thread th1 = new Thread(fill);
        th1.start();
    }
}

