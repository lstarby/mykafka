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
package org.apache.kafka.common.metrics;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Configuration values for metrics
 * 配置值得衡量指标
 */
public class MetricConfig {

    private Quota quota;
    /**
     * 用于维护衡量指标的样本数
     */
    private int samples;
    private long eventWindow;
    /**
     * metrics系统维护可配置的样本数量，在一个可修正的window size。这项配置配置了窗口大小，例如。我们可能在30s的期间维护两个样本。当一个窗口推出后，我们会擦除并重写最老的窗口
     */
    private long timeWindowMs;
    /**
     * clinet.id 键值对集合
     */
    private Map<String, String> tags;
    private Sensor.RecordingLevel recordingLevel;

    public MetricConfig() {
        super();
        this.quota = null;
        this.samples = 2;
        this.eventWindow = Long.MAX_VALUE;
        this.timeWindowMs = TimeUnit.MILLISECONDS.convert(30, TimeUnit.SECONDS);
        this.tags = new LinkedHashMap<>();
        this.recordingLevel = Sensor.RecordingLevel.INFO;
    }

    public Quota quota() {
        return this.quota;
    }

    public MetricConfig quota(Quota quota) {
        this.quota = quota;
        return this;
    }

    public long eventWindow() {
        return eventWindow;
    }

    public MetricConfig eventWindow(long window) {
        this.eventWindow = window;
        return this;
    }

    public long timeWindowMs() {
        return timeWindowMs;
    }

    public MetricConfig timeWindow(long window, TimeUnit unit) {
        this.timeWindowMs = TimeUnit.MILLISECONDS.convert(window, unit);
        return this;
    }

    public Map<String, String> tags() {
        return this.tags;
    }

    public MetricConfig tags(Map<String, String> tags) {
        this.tags = tags;
        return this;
    }

    public int samples() {
        return this.samples;
    }

    public MetricConfig samples(int samples) {
        if (samples < 1)
            throw new IllegalArgumentException("The number of samples must be at least 1.");
        this.samples = samples;
        return this;
    }

    public Sensor.RecordingLevel recordLevel() {
        return this.recordingLevel;
    }

    public MetricConfig recordLevel(Sensor.RecordingLevel recordingLevel) {
        this.recordingLevel = recordingLevel;
        return this;
    }

	@Override
	public String toString() {
		return "MetricConfig [quota=" + quota + ", samples=" + samples + ", eventWindow=" + eventWindow
				+ ", timeWindowMs=" + timeWindowMs + ", tags=" + tags + ", recordingLevel=" + recordingLevel
				+ ", getClass()=" + getClass() + ", hashCode()=" + hashCode() + ", toString()=" + super.toString()
				+ "]";
	}


    
}
