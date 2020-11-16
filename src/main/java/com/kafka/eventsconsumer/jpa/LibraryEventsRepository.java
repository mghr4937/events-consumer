package com.kafka.eventsconsumer.jpa;

import com.kafka.eventsconsumer.entity.LibraryEvent;
import org.springframework.data.repository.CrudRepository;

public interface LibraryEventsRepository extends CrudRepository<LibraryEvent, Integer> {


}
