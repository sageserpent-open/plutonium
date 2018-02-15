package com.sageserpent.plutonium

abstract class History {
  type Id
  val id: Id

  override def toString: String = s"id: $id, proxied class: ${getClass.getSuperclass}, datums: $datums"

  override def hashCode(): Int =
    id.hashCode() // Use a stable hash code - remember that the rest of the state will update.

  override def equals(that: scala.Any): Boolean = {
    val thisClazz = getClass
    val thatClazz = that.getClass

    if (thisClazz.isAssignableFrom(thatClazz)) {
      val thatHistory = that.asInstanceOf[History]
      id == thatHistory.id && datums == thatHistory.datums
    } else thatClazz.isAssignableFrom(thisClazz) && that.equals(this)
  }

  def checkInvariant(): Unit = {
    if (invariantBreakageScheduled) {
      // NOTE: breakage of a bitemporal invariant is *not* a logic error; we expect
      // to be asked to try to record events that could potentially make the world
      // inconsistent - so we don't use an assertion here.
      throw WorldSpecSupport.changeError
    }
    try {
      shouldBeUnchanged = false
      // An invariant should not be able to modify its item. End of story, no if or buts.
      assert(false)
    } catch {
      case _: RuntimeException =>
    }
  }

  private val _datums = scala.collection.mutable.MutableList.empty[Any]

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

  private var invariantBreakageScheduled = false

  def forceInvariantBreakage() = {
    invariantBreakageScheduled = true
  }

  val propertyAllowingSecondOrderMutation =
    scala.collection.mutable.MutableList.empty[Any]
}
