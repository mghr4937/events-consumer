package com.kafka.eventsconsumer.intg.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.eventsconsumer.consumer.EventsConsumer;
import com.kafka.eventsconsumer.entity.Book;
import com.kafka.eventsconsumer.entity.LibraryEvent;
import com.kafka.eventsconsumer.entity.enums.EventType;
import com.kafka.eventsconsumer.jpa.LibraryEventsRepository;
import com.kafka.eventsconsumer.service.LibraryEventsService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@SpringBootTest
@EmbeddedKafka(topics = "library-events", partitions = 3)
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}"})
public class LibraryEventsConsumerIntegrationTest {

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    KafkaListenerEndpointRegistry endpointRegistry;

    @Autowired
    LibraryEventsRepository libraryEventsRepository;

    @SpyBean
    EventsConsumer libraryEventsConsumerSpy;

    @SpyBean
    LibraryEventsService libraryEventsServiceSpy;

    @Autowired
    ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {

        for (MessageListenerContainer messageListenerContainer : endpointRegistry.getAllListenerContainers()) {
            ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafkaBroker.getPartitionsPerTopic());
        }
    }

    @AfterEach
    void tearDown() {
        libraryEventsRepository.deleteAll();
    }

    @Test
    void publishNewLibraryEvent() throws ExecutionException, InterruptedException, JsonProcessingException {
        //given
        String json = "{\"id\":null,\"eventType\":\"NEW\",\"book\":{\"id\":123,\"name\":\"Kafka Using Spring Boot\",\"author\":\"Dummy\"}}";
        kafkaTemplate.sendDefault(json).get();

        //when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        //then
        verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));

        List<LibraryEvent> libraryEventList = (List<LibraryEvent>) libraryEventsRepository.findAll();
        assert libraryEventList.size() == 1;
        libraryEventList.forEach(libraryEvent -> {
            assert libraryEvent.getId() != null;
            assertEquals(123, libraryEvent.getBook().getId());
        });
    }

    @Test
    void publishUpdateLibraryEvent() throws JsonProcessingException, ExecutionException, InterruptedException {
        //given
        String json = "{\"id\":null,\"eventType\":\"NEW\",\"book\":{\"id\":123,\"name\":\"Kafka Using Spring Boot\",\"author\":\"Dummy\"}}";
        LibraryEvent libraryEvent = objectMapper.readValue(json, LibraryEvent.class);
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventsRepository.save(libraryEvent);

        //publish the update library event
        Book updatedBook = Book.builder()
                .id(123)
                .name("Another spring kafka book")
                .author("Willy")
                .build();

        libraryEvent.setEventType(EventType.UPDATE);
        libraryEvent.setBook(updatedBook);
        String updatedJson = objectMapper.writeValueAsString(libraryEvent);
        kafkaTemplate.sendDefault(libraryEvent.getId(), updatedJson).get();

        //when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        //then
        LibraryEvent persistedLibraryEvent = libraryEventsRepository.findById(libraryEvent.getId()).get();

        assertEquals("Another spring kafka book", persistedLibraryEvent.getBook().getName());

    }

    @Test
    void publishModifyLibraryEvent_NotValidEventId() throws JsonProcessingException, InterruptedException, ExecutionException {

        //given
        Integer eventId = 888;
        String json = "{\"id\":" + eventId + ",\"eventType\":\"UPDATE\",\"book\":{\"id\":456,\"name\":\"Kafka Using Spring Boot\",\"athor\":\"Dilip\"}}";
        System.out.println(json);
        kafkaTemplate.sendDefault(eventId, json).get();

        //when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));

        Optional<LibraryEvent> libraryEventOptional = libraryEventsRepository.findById(eventId);
        assertTrue(libraryEventOptional.isEmpty());
    }

    @Test
    void publishModifyLibraryEvent_NullEventId() throws JsonProcessingException, InterruptedException, ExecutionException {
        //given
        Integer eventId = null;
        String json = "{\"id\":" + eventId + ",\"eventType\":\"UPDATE\",\"book\":{\"id\":456,\"name\":\"Kafka Using Spring Boot\",\"author\":\"Dilip\"}}";
        kafkaTemplate.sendDefault(eventId, json).get();
        //when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);


        verify(libraryEventsConsumerSpy, times(5)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(5)).processLibraryEvent(isA(ConsumerRecord.class));
    }

    @Test
    void publishModifyLibraryEvent_000EventId() throws JsonProcessingException, InterruptedException, ExecutionException {
        //given
        Integer eventId = 000;
        String json = "{\"id\":" + eventId + ",\"eventType\":\"UPDATE\",\"book\":{\"id\":456,\"name\":\"Kafka Using Spring Boot\",\"author\":\"Dilip\"}}";
        kafkaTemplate.sendDefault(eventId, json).get();
        //when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);


        verify(libraryEventsServiceSpy, times(4)).processLibraryEvent(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(1)).handleRecovery(isA(ConsumerRecord.class));
    }
}
