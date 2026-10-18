import de.commerce.Enricher;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public class EnrichTest {

    private TopologyTestDriver driver;
    private TestInputTopic<String, String> orders;
    private TestInputTopic<String, String> customers;
    private TestOutputTopic<String, String> enriched;

    @BeforeEach
    void setUp() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kfk08-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                  Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                  Serdes.String().getClass());

        Topology topology = Enricher.build();
        driver = new TopologyTestDriver(topology, props);

        orders = driver.createInputTopic("orders",
                new StringSerializer(), new StringSerializer());
        customers = driver.createInputTopic("customers",
                new StringSerializer(), new StringSerializer());
        enriched = driver.createOutputTopic("enriched",
                new StringDeserializer(), new StringDeserializer());
    }

    @AfterEach
    void tearDown() {
        if (driver != null) {
            driver.close();
        }
    }

    @Test
    void enrichesOrderWithCurrentCustomerName() {
        customers.pipeInput("c1", "Acme");
        orders.pipeInput("c1", "10.0");

        assertThat(enriched.readKeyValue().value).isEqualTo("Acme|10.0");
        assertThat(enriched.isEmpty()).isTrue();
    }

    @Test
    void orderBeforeCustomerExistsProducesNothing() {
        // inner KStream-KTable join: no table entry yet -> no output
        orders.pipeInput("c9", "5.0");
        assertThat(enriched.isEmpty()).isTrue();
    }

    @Test
    void laterOrderNotRetroactiveAfterCustomerArrives() {
        orders.pipeInput("c2", "1.0");          // no customer yet -> dropped
        customers.pipeInput("c2", "Globex");     // table change: no output
        orders.pipeInput("c2", "2.0");          // now enriched

        assertThat(enriched.readKeyValue().value).isEqualTo("Globex|2.0");
        assertThat(enriched.isEmpty()).isTrue();
    }

    @Test
    void customerUpdateChangesLaterEnrichment() {
        customers.pipeInput("c3", "Initech");
        orders.pipeInput("c3", "7.0");
        customers.pipeInput("c3", "Initech LLC"); // dimension update
        orders.pipeInput("c3", "8.0");

        assertThat(enriched.readKeyValue().value).isEqualTo("Initech|7.0");
        assertThat(enriched.readKeyValue().value).isEqualTo("Initech LLC|8.0");
        assertThat(enriched.isEmpty()).isTrue();
    }

    @Test
    void keyIsPreservedOnOutput() {
        customers.pipeInput("c4", "Umbrella");
        orders.pipeInput("c4", "3.5");
        var kv = enriched.readKeyValue();
        assertThat(kv.key).isEqualTo("c4");
        assertThat(kv.value).isEqualTo("Umbrella|3.5");
    }
}
