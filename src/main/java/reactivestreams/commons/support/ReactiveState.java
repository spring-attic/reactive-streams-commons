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

package reactivestreams.commons.support;

import java.util.Iterator;

/**
 * A component that supports extra state peeking and access for reactive components: buffers, capacity, names,
 * connected upstream/downstreams...
 *
 * The state read accuracy (volatility) is implementation-dependant and implementors MAY return cached value for a
 * given state.
 */
public interface ReactiveState {


	/**
	 * A capacity aware component
	 */
	interface Bounded extends ReactiveState {

		/**
		 * Return defined element capacity
		 * @return long capacity
		 */
		long getCapacity();
	}

	/**
	 * A storing component
	 */
	interface Buffering extends Bounded {

		/**
		 * Return current used space in buffer
		 * @return long capacity
		 */
		long pending();
	}

	/**
	 * A component that is linked to a source producer.
	 */
	interface Upstream extends ReactiveState {

		/**
		 * Return the direct source of data, Supports reference
		 */
		Object upstream();
	}

	/**
	 * A component that is linked to N upstreams producers.
	 */
	interface LinkedUpstreams extends ReactiveState {

		/**
		 * Return the connected sources of data
		 */
		Iterator<?> upstreams();

		/**
		 * @return the number of upstreams
		 */
		long upstreamsCount();
	}

	/**
	 * An upstream producer tracker
	 */
	interface UpstreamDemand extends ReactiveState {

		/**
		 * @return expected number of events to be produced to this component
		 */
		long expectedFromUpstream();
	}

	/**
	 * A threshold aware component
	 */
	interface UpstreamPrefetch extends UpstreamDemand {

		/**
		 * @return a given limit threshold to replenish outstanding upstream request
		 */
		long limit();
	}


	/**
	 * A component that will emit events to a downstream.
	 */
	interface Downstream extends ReactiveState {

		/**
		 * Return the direct data receiver
		 */
		Object downstream();
	}

	/**
	 * A component that will emit events to N downstreams.
	 */
	interface LinkedDownstreams extends ReactiveState {

		/**
		 * @return the connected data receivers
		 */
		Iterator<?> downstreams();

		/**
		 * @return
		 */
		long downstreamsCount();

	}

	/**
	 * A request aware component
	 */
	interface DownstreamDemand extends ReactiveState {

		/**
		 * Return defined element capacity, used to drive new {@link org.reactivestreams.Subscription} request needs.
		 * This is the maximum in-flight data allowed to transit to this elements.
		 * @return long capacity
		 */
		long requestedFromDownstream();
	}

	/**
	 * A lifecycle backed upstream
	 */
	interface ActiveUpstream extends ReactiveState {

		/**
		 * @return has this upstream started or "onSubscribed" ?
		 */
		boolean isStarted();

		/**
		 *
		 * @return has this upstream finished or "completed" / "failed" ?
		 */
		boolean isTerminated();
	}

	/**
	 * A lifecycle backed downstream
	 */
	interface ActiveDownstream extends ReactiveState {

		/**
		 *
		 * @return has the downstream "cancelled" and interrupted its consuming ?
		 */
		boolean isCancelled();
	}

	/**
	 * A component that is meant to be introspectable on finest logging level
	 */
	interface Trace extends ReactiveState {

	}

	/**
	 * A component that is meant to be embedded or gating linked upstream(s) and/or downstream(s) components
	 */
	interface Inner extends ReactiveState {

	}

	/**
	 * A component that holds a failure state if any
	 */
	interface FailState extends ReactiveState {

		Throwable getError();
	}


	/**
	 * A component that is forking to a sub-flow given a delegate input and that is consuming from a given delegate
	 * output
	 */
	interface FeedbackLoop extends ReactiveState {

		Object delegateInput();

		Object delegateOutput();
	}

	/**
	 * A component that is intended to build others
	 */
	interface Factory extends ReactiveState {

	}
}
