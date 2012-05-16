/**
 * Copyright (C) 2010-2012 LShift Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.lshift.diffa.kernel.util.client;

import net.lshift.diffa.kernel.util.cache.KeyPredicate;

public class TestKeyPredicate implements KeyPredicate<TestCacheKey> {

  private String predicateValue;

  public TestKeyPredicate() {
  }

  public TestKeyPredicate(String predicateValue) {
    this.predicateValue = predicateValue;
  }

  public String getPredicateValue() {
    return predicateValue;
  }

  public void setPredicateValue(String predicateValue) {
    this.predicateValue = predicateValue;
  }

  @Override
  public boolean constrain(TestCacheKey key) {
    return key.getFirstValue().equals(predicateValue);
  }
}
