package cl.messages.controller;

import cl.messages.dto.RequestMessageDTO;
import cl.messages.service.MessageService;
import lombok.extern.log4j.Log4j2;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("/messages")
@Log4j2
public class MessageController {
    private final MessageService service;
    public MessageController(MessageService service) { this.service = service; }

    @PostMapping
    public CompletableFuture<ResponseEntity<Map<String, Object>>> sendMessage(
            @RequestBody RequestMessageDTO request) {
        try {
            return service.sendMessage(request).handle((metadata, error) -> {
                if (error != null) {
                    log.error("Kafka no confirmo el envio", error);
                    if (error.getCause() instanceof IllegalArgumentException) {
                        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(
                                Map.<String, Object>of("status", "invalid_message"));
                    }
                    return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(
                            Map.<String, Object>of("status", "unavailable", "message", "Kafka no confirmo el envio"));
                }
                return ResponseEntity.status(HttpStatus.CREATED).body(Map.<String, Object>of(
                        "status", "published", "topic", metadata.topic(),
                        "partition", metadata.partition(), "offset", metadata.offset(),
                        "message", "Mensaje publicado correctamente"
                    ));
            });
        } catch (RuntimeException e) {
            log.error("Fallo inmediato al iniciar publicacion en Kafka", e);
            return CompletableFuture.completedFuture(
                    ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(
                            Map.<String, Object>of("status", "unavailable")));
        }
    }
}
