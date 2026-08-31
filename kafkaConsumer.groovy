package kafka

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.common.serialization.StringDeserializer
import com.kms.katalon.core.annotation.Keyword
import com.kms.katalon.core.util.KeywordUtil

public class KafkaConsumerKeyword {

    @Keyword
    public String consumeLatestMessage(String bootstrapServers, String topic, String groupId) {
        Properties props = new Properties()
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)
        consumer.subscribe(Collections.singletonList(topic))

        ConsumerRecords<String, String> records = consumer.poll(java.time.Duration.ofSeconds(5))
        String lastMessage = ""
        
        for (record in records) {
            lastMessage = record.value()
            KeywordUtil.logInfo("Consumed Message: " + lastMessage)
        }
        
        consumer.close()
        return lastMessage
    }
}