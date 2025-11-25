/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.devlive.connector.dameng.logminer.buffered.infinispan.marshalling;

import java.lang.annotation.*;

/**
 * Indicates that visibility of the annotated element is raised for the purposes of marshalling
 * (e.g. public instead of package-private). Uses of methods annotated with this indicator are
 * discouraged and should not be used in runtime code.
 *
 * @author Chris Cranford
 */
@Documented
@Target({ ElementType.CONSTRUCTOR, ElementType.METHOD })
@Retention(RetentionPolicy.SOURCE)
public @interface VisibleForMarshalling {
}
