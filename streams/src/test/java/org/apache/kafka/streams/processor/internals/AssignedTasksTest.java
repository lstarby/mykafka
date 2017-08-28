/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.processor.TaskId;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AssignedTasksTest {

    private final Task t1 = EasyMock.createMock(Task.class);
    private final Task t2 = EasyMock.createMock(Task.class);
    private final TopicPartition tp1 = new TopicPartition("t1", 0);
    private final TopicPartition tp2 = new TopicPartition("t2", 0);
    private final TopicPartition changeLog1 = new TopicPartition("cl1", 0);
    private final TopicPartition changeLog2 = new TopicPartition("cl2", 0);
    private final TaskId taskId1 = new TaskId(0, 0);
    private final TaskId taskId2 = new TaskId(1, 0);
    private AssignedTasks assignedTasks;

    @Before
    public void before() {
        assignedTasks = new AssignedTasks("log", "task");
        EasyMock.expect(t1.id()).andReturn(taskId1).anyTimes();
        EasyMock.expect(t2.id()).andReturn(taskId2).anyTimes();
    }

    @Test
    public void shouldGetPartitionsFromNewTasksThatHaveStateStores() {
        EasyMock.expect(t1.hasStateStores()).andReturn(true);
        EasyMock.expect(t2.hasStateStores()).andReturn(true);
        EasyMock.expect(t1.partitions()).andReturn(Collections.singleton(tp1));
        EasyMock.expect(t2.partitions()).andReturn(Collections.singleton(tp2));
        EasyMock.replay(t1, t2);

        assignedTasks.addNewTask(t1);
        assignedTasks.addNewTask(t2);

        final Set<TopicPartition> partitions = assignedTasks.uninitializedPartitions();
        assertThat(partitions, equalTo(Utils.mkSet(tp1, tp2)));
        EasyMock.verify(t1, t2);
    }

    @Test
    public void shouldNotGetPartitionsFromNewTasksWithoutStateStores() {
        EasyMock.expect(t1.hasStateStores()).andReturn(false);
        EasyMock.expect(t2.hasStateStores()).andReturn(false);
        EasyMock.replay(t1, t2);

        assignedTasks.addNewTask(t1);
        assignedTasks.addNewTask(t2);

        final Set<TopicPartition> partitions = assignedTasks.uninitializedPartitions();
        assertTrue(partitions.isEmpty());
        EasyMock.verify(t1, t2);
    }

    @Test
    public void shouldInitializeNewTasks() {
        EasyMock.expect(t1.initialize()).andReturn(false);
        EasyMock.replay(t1);

        addAndInitTask();

        EasyMock.verify(t1);
    }

    @Test
    public void shouldMoveInitializedTasksNeedingRestoreToRestoring() {
        EasyMock.expect(t1.initialize()).andReturn(false);
        EasyMock.expect(t2.initialize()).andReturn(true);
        EasyMock.expect(t2.partitions()).andReturn(Collections.singleton(tp2));
        EasyMock.expect(t2.changelogPartitions()).andReturn(Collections.<TopicPartition>emptyList());

        EasyMock.replay(t1, t2);

        assignedTasks.addNewTask(t1);
        assignedTasks.addNewTask(t2);

        assignedTasks.initializeNewTasks();

        Collection<Task> restoring = assignedTasks.restoringTasks();
        assertThat(restoring.size(), equalTo(1));
        assertSame(restoring.iterator().next(), t1);
    }

    @Test
    public void shouldMoveInitializedTasksThatDontNeedRestoringToRunning() {
        EasyMock.expect(t2.initialize()).andReturn(true);
        EasyMock.expect(t2.partitions()).andReturn(Collections.singleton(tp2));
        EasyMock.expect(t2.changelogPartitions()).andReturn(Collections.<TopicPartition>emptyList());

        EasyMock.replay(t2);

        assignedTasks.addNewTask(t2);
        assignedTasks.initializeNewTasks();

        assertThat(assignedTasks.runningTaskIds(), equalTo(Collections.singleton(taskId2)));
    }

    @Test
    public void shouldTransitionFullyRestoredTasksToRunning() {
        final Set<TopicPartition> task1Partitions = Utils.mkSet(tp1);
        EasyMock.expect(t1.initialize()).andReturn(false);
        EasyMock.expect(t1.partitions()).andReturn(task1Partitions).anyTimes();
        EasyMock.expect(t1.changelogPartitions()).andReturn(Utils.mkSet(changeLog1, changeLog2)).anyTimes();
        EasyMock.replay(t1);

        addAndInitTask();

        assertTrue(assignedTasks.updateRestored(Utils.mkSet(changeLog1)).isEmpty());
        Set<TopicPartition> partitions = assignedTasks.updateRestored(Utils.mkSet(changeLog2));
        assertThat(partitions, equalTo(task1Partitions));
        assertThat(assignedTasks.runningTaskIds(), equalTo(Collections.singleton(taskId1)));
    }

    @Test
    public void shouldSuspendRunningTasks() {
        mockRunningTaskSuspension();
        EasyMock.replay(t1);

        suspendTask();

        assertThat(assignedTasks.previousTaskIds(), equalTo(Collections.singleton(taskId1)));
        EasyMock.verify(t1);
    }

    @Test
    public void shouldCloseRestoringTasks() {
        EasyMock.expect(t1.initialize()).andReturn(false);
        t1.close(false);
        EasyMock.expectLastCall();
        EasyMock.replay(t1);

        suspendTask();
        EasyMock.verify(t1);
    }

    @Test
    public void shouldClosedUnInitializedTasksOnSuspend() {
        t1.close(false);
        EasyMock.expectLastCall();
        EasyMock.replay(t1);

        assignedTasks.addNewTask(t1);
        assignedTasks.suspend();

        EasyMock.verify(t1);
    }

    @Test
    public void shouldNotSuspendSuspendedTasks() {
        mockRunningTaskSuspension();
        EasyMock.replay(t1);

        suspendTask();
        assignedTasks.suspend();
        EasyMock.verify(t1);
    }


    @Test
    public void shouldCloseTaskOnSuspendWhenRuntimeException() {
        mockTaskInitialization();
        t1.suspend();
        EasyMock.expectLastCall().andThrow(new RuntimeException("KABOOM!"));
        t1.close(false);
        EasyMock.expectLastCall();
        EasyMock.replay(t1);

        assertThat(suspendTask(), not(nullValue()));
        assertThat(assignedTasks.previousTaskIds(), equalTo(Collections.singleton(taskId1)));
        EasyMock.verify(t1);
    }

    @Test
    public void shouldCloseTaskOnSuspendWhenProducerFencedException() {
        mockTaskInitialization();
        t1.suspend();
        EasyMock.expectLastCall().andThrow(new ProducerFencedException("KABOOM!"));
        t1.close(false);
        EasyMock.expectLastCall();
        EasyMock.replay(t1);


        assertThat(suspendTask(), nullValue());
        assertTrue(assignedTasks.previousTaskIds().isEmpty());
        EasyMock.verify(t1);
    }

    @Test
    public void shouldResumeMatchingSuspendedTasks() {
        mockRunningTaskSuspension();
        t1.resume();
        EasyMock.expectLastCall();
        EasyMock.replay(t1);

        suspendTask();

        assertTrue(assignedTasks.maybeResumeSuspendedTask(taskId1, Collections.singleton(tp1)));
        assertThat(assignedTasks.runningTaskIds(), equalTo(Collections.singleton(taskId1)));
        EasyMock.verify(t1);
    }


    private void mockTaskInitialization() {
        EasyMock.expect(t1.initialize()).andReturn(true);
        EasyMock.expect(t1.partitions()).andReturn(Collections.singleton(tp1));
        EasyMock.expect(t1.changelogPartitions()).andReturn(Collections.<TopicPartition>emptyList());
    }

    @Test
    public void shouldCommitRunningTasks() {
        mockTaskInitialization();
        t1.commit();
        EasyMock.expectLastCall();
        EasyMock.replay(t1);

        addAndInitTask();

        assignedTasks.commit();
        EasyMock.verify(t1);
    }

    @Test
    public void shouldCloseTaskOnCommitIfProduceFencedException() {
        mockTaskInitialization();
        t1.commit();
        EasyMock.expectLastCall().andThrow(new ProducerFencedException(""));
        t1.close(false);
        EasyMock.expectLastCall();
        EasyMock.replay(t1);
        addAndInitTask();

        assignedTasks.commit();
        EasyMock.verify(t1);
    }

    @Test
    public void shouldNotThrowCommitFailedExceptionOnCommit() {
        mockTaskInitialization();
        t1.commit();
        EasyMock.expectLastCall().andThrow(new CommitFailedException());
        EasyMock.replay(t1);
        addAndInitTask();

        assignedTasks.commit();
        assertThat(assignedTasks.runningTaskIds(), equalTo(Collections.singleton(taskId1)));
        EasyMock.verify(t1);
    }

    @Test
    public void shouldThrowExceptionOnCommitWhenNotCommitFailedOrProducerFenced() {
        mockTaskInitialization();
        t1.commit();
        EasyMock.expectLastCall().andThrow(new RuntimeException(""));
        EasyMock.replay(t1);
        addAndInitTask();

        try {
            assignedTasks.commit();
            fail("Should have thrown exception");
        } catch (Exception e) {
            // ok
        }
        assertThat(assignedTasks.runningTaskIds(), equalTo(Collections.singleton(taskId1)));
        EasyMock.verify(t1);
    }

    @Test
    public void shouldCommitRunningTasksIfNeeded() {
        mockTaskInitialization();
        EasyMock.expect(t1.commitNeeded()).andReturn(true);
        t1.commit();
        EasyMock.expectLastCall();
        EasyMock.replay(t1);

        addAndInitTask();

        assertThat(assignedTasks.maybeCommit(), equalTo(1));
        EasyMock.verify(t1);
    }

    @Test
    public void shouldCloseTaskOnMaybeCommitIfProduceFencedException() {
        mockTaskInitialization();
        EasyMock.expect(t1.commitNeeded()).andReturn(true);
        t1.commit();
        EasyMock.expectLastCall().andThrow(new ProducerFencedException(""));
        t1.close(false);
        EasyMock.expectLastCall();
        EasyMock.replay(t1);
        addAndInitTask();

        assignedTasks.maybeCommit();
        EasyMock.verify(t1);
    }

    @Test
    public void shouldNotThrowCommitFailedExceptionOnMaybeCommit() {
        mockTaskInitialization();
        EasyMock.expect(t1.commitNeeded()).andReturn(true);
        t1.commit();
        EasyMock.expectLastCall().andThrow(new CommitFailedException());
        EasyMock.replay(t1);
        addAndInitTask();

        assignedTasks.maybeCommit();
        assertThat(assignedTasks.runningTaskIds(), equalTo(Collections.singleton(taskId1)));
        EasyMock.verify(t1);
    }

    @Test
    public void shouldThrowExceptionOnMaybeCommitWhenNotCommitFailedOrProducerFenced() {
        mockTaskInitialization();
        EasyMock.expect(t1.commitNeeded()).andReturn(true);
        t1.commit();
        EasyMock.expectLastCall().andThrow(new RuntimeException(""));
        EasyMock.replay(t1);

        addAndInitTask();

        try {
            assignedTasks.maybeCommit();
            fail("Should have thrown exception");
        } catch (Exception e) {
            // ok
        }
        assertThat(assignedTasks.runningTaskIds(), equalTo(Collections.singleton(taskId1)));
        EasyMock.verify(t1);
    }



    @Test
    public void shouldPunctuateRunningTasks() {
        mockTaskInitialization();
        EasyMock.expect(t1.maybePunctuateStreamTime()).andReturn(true);
        EasyMock.expect(t1.maybePunctuateSystemTime()).andReturn(true);
        EasyMock.replay(t1);

        addAndInitTask();

        assertThat(assignedTasks.punctuate(), equalTo(2));
        EasyMock.verify(t1);
    }

    @Test
    public void shouldReturnNumberOfPunctuations() {
        mockTaskInitialization();
        EasyMock.expect(t1.maybePunctuateStreamTime()).andReturn(true);
        EasyMock.expect(t1.maybePunctuateSystemTime()).andReturn(false);
        EasyMock.replay(t1);

        addAndInitTask();

        assertThat(assignedTasks.punctuate(), equalTo(1));
        EasyMock.verify(t1);
    }

    private void addAndInitTask() {
        assignedTasks.addNewTask(t1);
        assignedTasks.initializeNewTasks();
    }

    private RuntimeException suspendTask() {
        addAndInitTask();
        return assignedTasks.suspend();
    }

    private void mockRunningTaskSuspension() {
        EasyMock.expect(t1.initialize()).andReturn(true);
        EasyMock.expect(t1.partitions()).andReturn(Collections.singleton(tp1)).anyTimes();
        EasyMock.expect(t1.changelogPartitions()).andReturn(Collections.<TopicPartition>emptyList()).anyTimes();
        t1.suspend();
        EasyMock.expectLastCall();
    }


}