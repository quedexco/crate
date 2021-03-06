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

import javax.annotation.Nonnull;

public abstract class FloatSetting extends Setting<Float, Float> {

    public Float maxValue() {
        return Float.MAX_VALUE;
    }

    public Float minValue() {
        return Float.MIN_VALUE;
    }

    @Override
    public DataType dataType() {
        return DataTypes.FLOAT;
    }

    @Override
    public Float extract(Settings settings) {
        return settings.getAsFloat(settingName(), defaultValue());
    }

    @Override
    public Float extract(Settings settings, @Nonnull Float defaultValue) {
        return settings.getAsFloat(settingName(), defaultValue);
    }
}
