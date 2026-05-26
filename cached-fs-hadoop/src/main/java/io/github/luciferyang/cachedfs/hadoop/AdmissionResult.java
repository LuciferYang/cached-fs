/*
 * Copyright (c) 2026 The cached-fs Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.luciferyang.cachedfs.hadoop;

/**
 * Typed result of the Phase 5c prefetch admission gate. Three static-final singletons cover all
 * cases; the gate ALWAYS returns one of them so no allocation happens in the hot read path.
 *
 * <p>Reason strings line up with the {@code prefetchSkipped(reason)} reason map: {@code "budget"},
 * {@code "heap_pressure"}. {@code ADMIT.reason()} is the empty string by convention.
 */
final class AdmissionResult {

  static final AdmissionResult ADMIT = new AdmissionResult(true, "");
  static final AdmissionResult BUDGET_REJECT = new AdmissionResult(false, "budget");
  static final AdmissionResult HEAP_REJECT = new AdmissionResult(false, "heap_pressure");

  private final boolean admit;
  private final String reason;

  private AdmissionResult(boolean admit, String reason) {
    this.admit = admit;
    this.reason = reason;
  }

  boolean admit() {
    return admit;
  }

  String reason() {
    return reason;
  }
}
