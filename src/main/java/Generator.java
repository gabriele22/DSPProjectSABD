import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import config.ConfigurationKafka;
import utils.entity.Comment;
import utils.KakfaCommentProducer;

/**
 * this thread reads data from the file csv (comments.csv) and replies one comment at a time
 * after waiting for a number of milliseconds equal to the number of minutes
 * elapsed between a comment and the next
 *
 */

public class Generator implements Runnable{

    private String filename;

    public Generator(String filename){
        this.filename = filename;
    }

    @Override
    public void run() {

        BufferedReader br = null;

        try {
            br = new BufferedReader(new FileReader(filename));
            String header = br.readLine();
            System.out.println("HEADER: "+ header);

            String line = br.readLine();
            long previousTime = getDateTime(line);
            send(line);

            while ((line = br.readLine()) != null) {

                long nextTime = getDateTime(line);
                long sleepTime = (int) Math.floor(((double) (nextTime - previousTime ) / (60*1000)));

                if(sleepTime>0) {

                    System.out.println(" sleep for :" + sleepTime + " ms");

                    try {
                        Thread.sleep(sleepTime);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                send(line);
                previousTime = nextTime;

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


    //send with kafkaCommentProducer
    private void send(String line) {
        KakfaCommentProducer producer = new KakfaCommentProducer(ConfigurationKafka.TOPIC);

        try {
            String[] tokensUnchecked = line.split(",",-1);
            String[] tokens = controlFieldWithString(tokensUnchecked);

            Comment comment = new Comment(tokens[0], tokens[1], tokens[2], tokens[3],
                                    tokens[4], tokens[5], tokens[6], tokens[7], tokens[8], tokens[9],
                                    tokens[10], tokens[11], tokens[12], tokens[13], tokens[14]);

            String commentId = tokens[3];

            //send
            producer.produce(String.valueOf(commentId), comment);


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }

    }

    //get createDate present in dataset
    private long getDateTime(String line){
        String[] tokens	=	line.split(",",-1);
        long ts = Long.valueOf(tokens[5])*1000;
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        Date d= new Date(ts);
        return d.getTime();
    }


    //control field with string and return compliant fields
    private String[] controlFieldWithString(String[] fields){
        ArrayList<String> temp =  new ArrayList<>();

        for(int i=0; i<fields.length;i++) {
            if (fields[i].startsWith("\042")) {  //  \042 = "
                StringBuilder u = new StringBuilder(fields[i].substring(1));
                while(!(fields[i+1].endsWith("\042"))) {
                    u.append(", ").append(fields[i+1]);
                    i=i+1;
                }

                u.append(", ").append(fields[i+1].substring(0, fields[i + 1].length() - 1));
                i = i+1;

                temp.add(u.toString());
            }else {
                temp.add(fields[i]);
            }
        }

        return temp.toArray(new String[15]);

    }


    public static void main(String[] args) {

        /* TODO:
         * Usage:
         * java -jar debs2015gc-1.0.jar it.uniroma2.debs2016gc.DataSource [debs dataset] [redis ip]
         */

        String fileCSV = args[0];
        Generator fill = new Generator (fileCSV);
        Thread th1 = new Thread(fill);
        th1.start();
    }
}

