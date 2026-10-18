package de.mastery.jvm06;

/** Produce the given values to a topic, consume them back, and print a tally.
 *  Replace the TODO. */
public class KafkaRoundTrip {

    public static void main(String[] args) {
        // args[0]=bootstrap, args[1]=topic, args[2..]=values
        // TODO: produce (acks=all) then consume from the beginning (poll with a deadline),
        //       then print "TALLY" followed by "<value>=<count>" lines.
    }
}
