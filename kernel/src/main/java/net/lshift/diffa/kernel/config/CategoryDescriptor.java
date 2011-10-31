/**
 * Copyright (C) 2010-2011 LShift Ltd.
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

package net.lshift.diffa.kernel.config;

import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;

/**
 * This provides various endpoint-specific attributes of a category that are necessary for the kernel
 * to be able auto-narrow a category.
 */
@JsonTypeInfo(use=JsonTypeInfo.Id.NAME, include= JsonTypeInfo.As.PROPERTY, property="@type")
@JsonSubTypes({
  @JsonSubTypes.Type(value = RangeCategoryDescriptor.class, name = "range"),
  @JsonSubTypes.Type(value = SetCategoryDescriptor.class, name = "set"),
  @JsonSubTypes.Type(value = PrefixCategoryDescriptor.class, name = "prefix")
})
abstract public class CategoryDescriptor {

  protected CategoryDescriptor() {
  }

  private int id;

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  /**
   * Determines whether the given other category descriptor is a refinement of this category descriptor. This allows
   * for validation of views - ensuring that they don't specify configuration that isn't achievable.
   * @param other the other category descriptor to validate.
   * @return true - the provided other descriptor is a refinement; false - the other descriptor is outside the bounds of
   *      this descriptor.
   */
  public abstract boolean isRefinement(CategoryDescriptor other);
}
