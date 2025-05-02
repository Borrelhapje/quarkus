package io.quarkus.deployment.steps;

import java.util.Optional;

import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.annotations.ExecutionTime;
import io.quarkus.deployment.annotations.Record;
import io.quarkus.deployment.builditem.ContextHandlerBuildItem;
import io.quarkus.deployment.builditem.ExecutorBuildItem;
import io.quarkus.deployment.builditem.LaunchModeBuildItem;
import io.quarkus.deployment.builditem.ShutdownContextBuildItem;
import io.quarkus.deployment.builditem.ThreadFactoryBuildItem;
import io.quarkus.deployment.builditem.nativeimage.RuntimeInitializedClassBuildItem;
import io.quarkus.deployment.metrics.MetricsCapabilityBuildItem;
import io.quarkus.deployment.metrics.MetricsFactoryConsumerBuildItem;
import io.quarkus.runtime.ExecutorRecorder;
import io.quarkus.runtime.metrics.MetricsFactory;

/**
 *
 */
public class ThreadPoolSetup {

    @BuildStep()
    @Record(value = ExecutionTime.RUNTIME_INIT)
    public void createExecutorMetricsProxy(ExecutorRecorder recorder,
            BuildProducer<MetricsFactoryConsumerBuildItem> metricsProducer,
            BuildProducer<MetricsExecutorBuildItem> executorMetricsProducer,
            MetricsCapabilityBuildItem metricsCapabilityBuildItem) {
        if (!metricsCapabilityBuildItem.metricsSupported(MetricsFactory.MICROMETER)
                && !metricsCapabilityBuildItem.metricsSupported(MetricsFactory.MP_METRICS)) {
            return;
        }
        final MetricsExecutor metricsDelegate = executor.createMetricsDelegate();
        metricsProducer.produce(new MetricsFactoryConsumerBuildItem(metricsDelegate));
        executorMetricsProducer.produce(new MetricsExecutorBuildItem(metricsDelegate));
    }

    @BuildStep
    @Record(value = ExecutionTime.RUNTIME_INIT)
    public ExecutorBuildItem createExecutor(ExecutorRecorder recorder, ShutdownContextBuildItem shutdownContextBuildItem,
            LaunchModeBuildItem launchModeBuildItem,
            Optional<ThreadFactoryBuildItem> threadFactoryBuildItem,
            Optional<ContextHandlerBuildItem> contextBuildItem,
            Optional<MetricsExecutorBuildItem> executorMetricsBuildItem) {
        return new ExecutorBuildItem(
                recorder.setupRunTime(shutdownContextBuildItem, launchModeBuildItem.getLaunchMode(),
                        threadFactoryBuildItem.map(ThreadFactoryBuildItem::getThreadFactory).orElse(null),
                        contextBuildItem.map(ContextHandlerBuildItem::contextHandler).orElse(null),
                        executorMetricsBuildItem.map(MetricsExecutorBuildItem::getMetricsExecutor).orElse(null)));
    }

    @BuildStep
    RuntimeInitializedClassBuildItem registerClasses() {
        // make sure that the config provider gets initialized only at run time
        return new RuntimeInitializedClassBuildItem(ExecutorRecorder.class.getName());
    }
}
