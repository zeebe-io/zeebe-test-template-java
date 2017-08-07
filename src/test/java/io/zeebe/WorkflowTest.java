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
package io.zeebe;

import static io.zeebe.fixtures.ZeebeTestRule.DEFAULT_PARTITION;
import static io.zeebe.fixtures.ZeebeTestRule.DEFAULT_TOPIC;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import io.zeebe.client.ZeebeClient;
import io.zeebe.client.workflow.cmd.DeploymentResult;
import io.zeebe.client.workflow.cmd.WorkflowInstance;
import io.zeebe.fixtures.ZeebeTestRule;
import org.junit.*;

public class WorkflowTest
{
    @Rule
    public ZeebeTestRule testRule = new ZeebeTestRule();

    private ZeebeClient client;

    @Before
    public void deploy()
    {
        client = testRule.getClient();

        final DeploymentResult deploymentResult = client.workflowTopic(DEFAULT_TOPIC, DEFAULT_PARTITION).deploy()
                .resourceFromClasspath("process.bpmn")
                .execute();

        assertThat(deploymentResult.isDeployed())
            .withFailMessage("Deployment failed: %s", deploymentResult.getErrorMessage())
            .isTrue();
    }

    @Test
    public void shouldCompleteWorkflowInstance()
    {
        final WorkflowInstance workflowInstance = client.workflowTopic(DEFAULT_TOPIC, DEFAULT_PARTITION).create()
            .bpmnProcessId("process")
            .latestVersion()
            .execute();

        client.taskTopic(DEFAULT_TOPIC, DEFAULT_PARTITION).newTaskSubscription()
            .taskType("task")
            .lockOwner("test")
            .lockTime(Duration.ofSeconds(30))
            .handler(task ->
            {
                task.complete();
            })
            .open();

        testRule.waitUntilWorklowInstanceCompleted(workflowInstance.getWorkflowInstanceKey());
    }

}
