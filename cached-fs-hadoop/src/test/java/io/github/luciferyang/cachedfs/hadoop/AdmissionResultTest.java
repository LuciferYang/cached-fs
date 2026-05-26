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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class AdmissionResultTest {

  @Test
  @DisplayName("ADMIT singleton: admit=true, reason is empty")
  void admitSingleton() {
    assertThat(AdmissionResult.ADMIT.admit()).isTrue();
    assertThat(AdmissionResult.ADMIT.reason()).isEmpty();
  }

  @Test
  @DisplayName("BUDGET_REJECT singleton: admit=false, reason=\"budget\"")
  void budgetRejectSingleton() {
    assertThat(AdmissionResult.BUDGET_REJECT.admit()).isFalse();
    assertThat(AdmissionResult.BUDGET_REJECT.reason()).isEqualTo("budget");
  }

  @Test
  @DisplayName("HEAP_REJECT singleton: admit=false, reason=\"heap_pressure\"")
  void heapRejectSingleton() {
    assertThat(AdmissionResult.HEAP_REJECT.admit()).isFalse();
    assertThat(AdmissionResult.HEAP_REJECT.reason()).isEqualTo("heap_pressure");
  }

  @Test
  @DisplayName("singletons are identity-stable (flyweight contract)")
  void singletonsAreFlyweights() {
    assertThat(AdmissionResult.ADMIT).isSameAs(AdmissionResult.ADMIT);
    assertThat(AdmissionResult.BUDGET_REJECT).isSameAs(AdmissionResult.BUDGET_REJECT);
    assertThat(AdmissionResult.HEAP_REJECT).isSameAs(AdmissionResult.HEAP_REJECT);
    // Reason strings line up with IoStatistics.PREFETCH_SKIPPED_REASONS so caller can route
    // `incPrefetchSkipped(adm.reason(), chunkSize)` without translation.
    assertThat(AdmissionResult.BUDGET_REJECT.reason())
        .isNotEqualTo(AdmissionResult.HEAP_REJECT.reason());
  }
}
