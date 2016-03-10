package com.sageserpent.plutonium

/**
 * Created by Gerard on 21/09/2015.
 */
abstract class History extends Identified {
  override def hashCode = super.hashCode

  var isConsistent = true

  private val _datums = scala.collection.mutable.MutableList.empty[Any]

  override def checkInvariant: Bitemporal[() => Unit] = {
    for {checkInvariantOnSuper <- super.checkInvariant
    } yield () => {
      checkInvariantOnSuper()
      if (!isConsistent) throw WorldSpecSupport.changeError
    }
  }

  // Subclasses should define properties whose updates call this method to log the update in the history.
  // Building up the history in this way causes the tests using the functionality in 'WorldSpec' along with
  // expectations that call 'datums' on a history to indirectly test a precondition on a change's spore. This
  // is the guarantee by the spore's caller that when a new revision is defined that contains changes, when
  // each change's spore begins execution, it will see the state of the world at the event time of the
  // change, with all of the history present due to prior events that are themselves not ruled out in the
  // revision being defined that will contain the change. The change's spore will also see the effects of
  // any other changes that come before the change in the revision, as defined by the event id to event map
  // passed to 'World.revise'.
  protected def recordDatum(datum: Any): Unit = {
    _datums += datum
  }

  val datums: scala.collection.Seq[Any] = _datums

  var shouldBeUnchanged: Boolean = true

  val propertyAllowingSecondOrderMutation = scala.collection.mutable.MutableList.empty[Any]
}
