/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Cloudera, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.labs.envelope.component;

/**
 * An Envelope component is a class that can insert functionality into an Envelope pipeline, such
 * as an input, or a deriver, or a task.
 * Envelope exposes interface APIs for components to be implemented with. Those interfaces will
 * extend this Component interface.
 * Envelope provides some built-in components. Pipelines can also provide their own plugins
 * that implement the corresponding component APIs.
 */
public interface Component {

}