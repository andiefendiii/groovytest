import com.kms.katalon.core.util.KeywordUtil

String server = "localhost:9092"
String topic = "user-registration"
String group = "katalon-consumer-group"

String message = CustomKeywords.'kafka.KafkaConsumerKeyword.consumeLatestMessage'(server, topic, group)

// Assertion
assert message != null && !message.isEmpty()
KeywordUtil.markPassed("Berhasil mengonsumsi pesan dari Kafka: " + message)