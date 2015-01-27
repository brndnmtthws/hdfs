package org.apache.mesos.hdfs;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.hdfs.config.SchedulerConf;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;

public class TestScheduler {

  @Mock
  SchedulerDriver driver;

  @Captor
  ArgumentCaptor<Collection<Protos.TaskInfo>> taskInfosCapture;

  private final Protos.OfferID offerId1 = Protos.OfferID.newBuilder().setValue("offer1").build();
  private final Protos.Offer firstOffer = Protos.Offer.newBuilder()
      .setId(offerId1)
      .setFrameworkId(Protos.FrameworkID.newBuilder().setValue("framework1").build())
      .setSlaveId(Protos.SlaveID.newBuilder().setValue("slave1").build())
      .setHostname("hostname")
      .build();

  @Test
  public void acceptsAllTheResourceOffersItCanUntilItHasEnoughToStart() {
    Scheduler scheduler = new Scheduler(new SchedulerConf(new Configuration(), 0));

    scheduler.resourceOffers(driver, Lists.newArrayList(firstOffer));

    verify(driver).launchTasks(eq(Lists.newArrayList(offerId1)), taskInfosCapture.capture());
    Collection<Protos.TaskInfo> taskInfos = taskInfosCapture.getValue();
    assertEquals(1, taskInfos.size());
    Protos.TaskInfo taskInfo = taskInfos.iterator().next();
    assertEquals("slave1", taskInfo.getSlaveId().getValue());
  }

  @Before
  public void initializeMocks() {
    MockitoAnnotations.initMocks(this);
  }
}
