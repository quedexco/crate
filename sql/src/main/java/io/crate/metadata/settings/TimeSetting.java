/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.metadata.settings;

import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;

import javax.annotation.Nonnull;

public abstract class TimeSetting extends Setting<TimeValue, String> {

    @Override
    public TimeValue defaultValue() {
        return new TimeValue(60_000);
    }

    public TimeValue maxValue() {
        return new TimeValue(Long.MAX_VALUE);
    }

    public TimeValue minValue() {
        return new TimeValue(0);
    }

    @Override
    public DataType dataType() {
        return DataTypes.STRING;
    }

    @Override
    public String extract(Settings settings) {
        return extractTimeValue(settings).toString();
    }

    @Override
    public String extract(Settings settings, @Nonnull TimeValue defaultValue) {
        return extractTimeValue(settings, defaultValue).toString();
    }

    public long extractMillis(Settings settings) {
        return extractTimeValue(settings).millis();
    }

    public TimeValue extractTimeValue(Settings settings) {
        return settings.getAsTime(settingName(), defaultValue());
    }

    private TimeValue extractTimeValue(Settings settings, @Nonnull TimeValue defaultValue) {
        return settings.getAsTime(settingName(), defaultValue);
    }
}
