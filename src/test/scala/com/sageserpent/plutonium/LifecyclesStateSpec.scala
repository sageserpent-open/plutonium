package com.sageserpent.plutonium

import org.scalacheck.{ShrinkLowPriority => NoShrinking}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

class LifecyclesStateSpec
    extends FlatSpec
    with Matchers
    with GeneratorDrivenPropertyChecks
    with NoShrinking {
  // TODO - presumably there would be some kind of compatibility test with 'PatchRecorder'?

  // TODO - so if one 'LifecyclesState' was mutated into another by a series of revisions, it would make the same cumulative update plan
  // as if all the revisions were rolled together as one - or at least it would have the same effect. Hmm, need to think about that more precisely.

  // 1. If I feed in new events that refer to a bunch of item ids, then I know that the updates generated will only refer to a subset of those ids plus those booked in vai prior revisions.

  // 2. Something to do with working out the type of a lifecycle's item.

  // 3. Preservation of event ordering in the update plan.

  // 4. Events must be a subset of the ones booked in.

  // 5. Something to do with measurements.

  // 6. Something to do with annihilations.

  // 7. Start with a bunch of events at various times, some of which collide. Scramble them up and revise an empty lifecycles state. The update plan that results
  // should refer to patches that come from the events, and these should be correctly ordered wrt to the the events that made them.

  // 8. Similar to #7, but intersperse obsolete events - all of the obsolete events must either be explicitly annulled or knocked out by a subsequent final
  // update. What gets through should have the same effect as if oen big revision was made with the final events.
}
