/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.fixtures;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.zeebe.client.TopicClient;
import io.zeebe.client.event.*;
import org.junit.rules.ExternalResource;

public class TopicEventRecorder extends ExternalResource
{
    private static final String SUBSCRIPTION_NAME = "event-recorder";

    private final List<ReceivedTaskEvent> taskEvents = new CopyOnWriteArrayList<>();
    private final List<ReceivedWorkflowInstanceEvent> wfInstanceEvents = new CopyOnWriteArrayList<>();

    private final ClientRule clientRule;

    private final String topicName;
    private final int partitionId;

    protected TopicSubscription subscription;

    public TopicEventRecorder(
            final ClientRule clientRule,
            final String topicName,
            final int partitionId)
    {
        this.clientRule = clientRule;
        this.topicName = topicName;
        this.partitionId = partitionId;
    }

    @Override
    protected void before() throws Throwable
    {
        startRecordingEvents();
    }

    @Override
    protected void after()
    {
        stopRecordingEvents();
    }

    private void startRecordingEvents()
    {
        final TopicClient client = clientRule.getClient().topic(topicName, partitionId);

        subscription = client.newSubscription()
            .name(SUBSCRIPTION_NAME)
            .taskEventHandler((m, e) -> taskEvents.add(new ReceivedTaskEvent(m, e)))
            .workflowInstanceEventHandler((m, e) -> wfInstanceEvents.add(new ReceivedWorkflowInstanceEvent(m, e)))
            .open();
    }

    private void stopRecordingEvents()
    {
        subscription.close();
        subscription = null;
    }

    private <T> Optional<T> getLastEvent(Stream<T> eventStream)
    {
        final List<T> events = eventStream.collect(Collectors.toList());

        if (events.isEmpty())
        {
            return Optional.empty();
        }
        else
        {
            return Optional.of(events.get(events.size() - 1));
        }
    }

    public List<ReceivedWorkflowInstanceEvent> getWorkflowInstanceEvents(final Predicate<ReceivedWorkflowInstanceEvent> matcher)
    {
        return wfInstanceEvents.stream().filter(matcher).collect(Collectors.toList());
    }

    public ReceivedWorkflowInstanceEvent getLastWorkflowInstanceEvent(final Predicate<ReceivedWorkflowInstanceEvent> matcher)
    {
        return getLastEvent(wfInstanceEvents.stream().filter(matcher)).orElseThrow(() -> new AssertionError("no event found"));
    }

    public List<ReceivedTaskEvent> getTaskEvents(final Predicate<ReceivedTaskEvent> matcher)
    {
        return taskEvents.stream().filter(matcher).collect(Collectors.toList());
    }

    public ReceivedTaskEvent getLastTaskEvent(final Predicate<ReceivedTaskEvent> matcher)
    {
        return getLastEvent(taskEvents.stream().filter(matcher)).orElseThrow(() -> new AssertionError("no event found"));
    }

    public static Predicate<ReceivedWorkflowInstanceEvent> wfInstance(final String eventType)
    {
        return e -> e.getEvent().getEventType().equals(eventType);
    }

    public static Predicate<ReceivedWorkflowInstanceEvent> wfInstanceKey(final long key)
    {
        return e -> e.getEvent().getWorkflowInstanceKey() == key;
    }

    public static Predicate<ReceivedTaskEvent> taskKey(final long key)
    {
        return e -> e.getMetadata().getEventKey() == key;
    }

    public static Predicate<ReceivedTaskEvent> taskType(final String type)
    {
        return e -> e.getEvent().getType().equals(type);
    }

    public class ReceivedTaskEvent
    {
        private final EventMetadata metadata;
        private final TaskEvent event;

        ReceivedTaskEvent(EventMetadata metadata, TaskEvent event)
        {
            this.metadata = metadata;
            this.event = event;
        }

        public EventMetadata getMetadata()
        {
            return metadata;
        }

        public TaskEvent getEvent()
        {
            return event;
        }
    }

    public class ReceivedWorkflowInstanceEvent
    {
        private final EventMetadata metadata;
        private final WorkflowInstanceEvent event;

        ReceivedWorkflowInstanceEvent(EventMetadata metadata, WorkflowInstanceEvent event)
        {
            this.metadata = metadata;
            this.event = event;
        }

        public EventMetadata getMetadata()
        {
            return metadata;
        }

        public WorkflowInstanceEvent getEvent()
        {
            return event;
        }
    }

}
