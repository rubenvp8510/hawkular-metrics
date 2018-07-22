/*
 * Copyright 2018 Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.hawkular.metrics.core.jobs;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.hawkular.metrics.core.service.BaseITest;
import org.hawkular.metrics.core.service.DataAccess;
import org.hawkular.metrics.core.service.MetricsServiceImpl;
import org.hawkular.metrics.core.service.TestDataAccessFactory;
import org.hawkular.metrics.scheduler.api.RepeatingTrigger;
import org.hawkular.metrics.scheduler.api.Trigger;
import org.hawkular.metrics.scheduler.impl.JobDetailsImpl;
import org.hawkular.metrics.sysconfig.ConfigurationService;
import org.joda.time.DateTime;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @author jsanda
 */
public class TempTableCreatorITest extends BaseITest {

    private DataAccess dataAccess;

    private MetricsServiceImpl metricsService;

    private ConfigurationService configurationService;

    @BeforeClass
    public void initClass() throws Exception {
        configurationService = new ConfigurationService();
        configurationService.init(rxSession);

        dataAccess = TestDataAccessFactory.newInstance(session);

        metricsService = new MetricsServiceImpl();
        metricsService.setDataAccess(dataAccess);
        metricsService.setConfigurationService(configurationService);
        metricsService.startUp(session, getKeyspace(), true, metricRegistry);
    }

    @Test
    public void runJob() throws Exception {
        long start = new DateTime(2018, 7, 20, 15, 0).getMillis();
        UUID jobId = UUID.randomUUID();
        Trigger trigger = new RepeatingTrigger.Builder()
                .withTriggerTime(start)
                .withInterval(2, TimeUnit.HOURS)
                .build();
        JobDetailsImpl jobDetails = new JobDetailsImpl(jobId, TempTableCreator.JOB_NAME, TempTableCreator.JOB_NAME,
                null, trigger);
        TempTableCreator job = new TempTableCreator(metricsService, configurationService);

        job.call(jobDetails).await();
    }
}
