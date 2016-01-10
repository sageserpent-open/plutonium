package com.sageserpent.plutonium

import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.prop.Checkers

/**
  * Created by Gerard on 10/01/2016.
  */

class PatchRecorderSpec extends FlatSpec with Matchers with Checkers {
  "Recording a patch" should "be reflected in the property 'whenEventPertainedToByLastRecordingTookPlace'" in {

  }

  it should "not immediately apply the patch" in {

  }

  it should "ensure that the patch is considered as a candidate for the best related patch at some point" in {

  }

  it should "ensure that patches are only ever applied in a subsequence of the sequence they were recorded" in {

  }

  it should "ensure a patch is only ever applied before any annihilations recorded after its recording" in {

  }

  "Candidates for the best related patch" should "only be submitted once" in {

  }

  they should "be submitted in chunks that when concatenated together form a subsequence of the sequence they were recorded" in {
    
  }

  "The best related patch" should "be applied" in {

  }

  "Recording a patch from an event" should "submit related patches taken from those recorded previously as candidates for the best related patch" in {
  }

  "Recording an annihilation" should "submit related patches taken from those recorded previously as candidates for the best related patch" in {

  }

  it should "immediately carry out the annihilation" in {

  }

  "Noting that recording has ended" should "submit related patches taken from those recorded previously as candidates for the best related patch" in {

  }

  it should "be reflected in the property 'allRecordingsAreCaptured'" in {

  }

  "A patch that is applied" should "not be applied again" in {

  }
}
