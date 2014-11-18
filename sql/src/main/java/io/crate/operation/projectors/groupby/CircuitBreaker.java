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

package io.crate.operation.projectors.groupby;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.util.Locale;

public class CircuitBreaker {

    private final ESLogger logger = Loggers.getLogger(getClass());
    private final long limit;
    private final double overheadConstant;

    private long used = 0;

    public CircuitBreaker(long limit, double overheadConstant) {
        this.limit = limit;
        this.overheadConstant = overheadConstant;
    }

    public void addEstimateAndMaybeBreak(long bytes) throws CircuitBreakingException {
        used += bytes;
        logger.error("Adding {} to used bytes [new used: {}, limit: {}]", bytes, used, limit);
        long newWithOverhead = (long) (used * overheadConstant);
        if (newWithOverhead > limit) {
            throw new CircuitBreakingException(String.format(Locale.ENGLISH,
                    "Circuit breaker: using %d bytes; limit is %d", used, limit));
        }
    }
}
