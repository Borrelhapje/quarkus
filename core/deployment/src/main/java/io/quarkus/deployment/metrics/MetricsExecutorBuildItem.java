package io.quarkus.deployment.metrics;

import io.quarkus.runtime.ExecutorRecorder.MetricsExecutor;

public class MetricsExecutorBuildItem extends SimpleBuildItem {

    private final MetricsExecutor metricsExecutor;

    public MetricsExecutorBuildItem(MetricsExecutor metricsExecutor) {
        this.metricsExecutor = metricsExecutor;
    }

    public MetricsExecutor getMetricsExecutor() {
        return metricsExecutor;
    }
}
