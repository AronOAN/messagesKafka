package cl.messages.config;

import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConsumerErrorConfiguration {
    // Agregar solo cuando exista @KafkaListener que procesa mensajes.
    @Bean
    DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> kafkaTemplate) {
        var recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (record, error) -> new TopicPartition("emailss.DLT", -1));
        recoverer.setFailIfSendResultIsError(true);
        // 1 intento inicial + hasta 3 reintentos; despues se publica en emailss.DLT.
        return new DefaultErrorHandler(recoverer, new FixedBackOff(1_000L, 3L));
    }
}
