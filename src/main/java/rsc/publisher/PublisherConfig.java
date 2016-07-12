
/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package rsc.publisher;

/**
 * A component that has introspection options
 */
public interface PublisherConfig {

    /**
     * A component that is meant to be introspectable on finest logging level
     */
    int TRACE_ONLY = 0b00000001;

    /**
     * A component that is meant to be embedded or gating linked upstream(s) and/or downstream(s) components
     */
    int INNER = 0b00000010;

    /**
     * A component that is intended to build others
     */
    int FACTORY = 0b00000100;

    /**
     * Defined identifier or null if not available
     * @return defined identifier or null if not available
     */
    default Object getId() {
        return null;
    }

    /**
     * Flags determining the nature of this {@link PublisherConfig}, can be a combination of those, e.g. :
     * <pre>
     *     int mode = PublisherConfig.LOGGING | PublisherConfig.FACTORY
     * @return the introspection mode, see constants
     */
    default int getMode(){
        return 0;
    }


    /**
     * The name of the component
     * @return the name of the component
     */
    default String getName() {
        return getClass().getSimpleName();
    }

    /**
     * The prefetch configuration of the component
     * @return the prefetch configuration of the component
     */
    default long getPrefetch() {
        return -1L;
    }
}
