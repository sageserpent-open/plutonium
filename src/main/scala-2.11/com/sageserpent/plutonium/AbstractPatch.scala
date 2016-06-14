package com.sageserpent.plutonium

import java.io.{ObjectInputStream, ObjectOutputStream}
import java.lang.reflect.Method

/**
  * Created by Gerard on 10/01/2016.
  */

object AbstractPatch {
  def patchesAreRelated(lhs: AbstractPatch, rhs: AbstractPatch): Boolean = {
    val bothReferToTheSameItem = lhs.targetId == rhs.targetId && (lhs.targetTypeTag.tpe <:< rhs.targetTypeTag.tpe || rhs.targetTypeTag.tpe <:< lhs.targetTypeTag.tpe)
    val bothReferToTheSameMethod = WorldImplementationCodeFactoring.firstMethodIsOverrideCompatibleWithSecond(lhs.method, rhs.method) ||
      WorldImplementationCodeFactoring.firstMethodIsOverrideCompatibleWithSecond(rhs.method, lhs.method)
    bothReferToTheSameItem && bothReferToTheSameMethod
  }
}

abstract class AbstractPatch(val method: Method) extends java.io.Serializable {
  val targetReconstitutionData: Recorder#ItemReconstitutionData[_ <: Identified]
  val argumentReconstitutionDatums: Seq[Recorder#ItemReconstitutionData[_ <: Identified]]
  lazy val (targetId, targetTypeTag) = targetReconstitutionData
  def apply(identifiedItemAccess: IdentifiedItemAccess): Unit
  def checkInvariant(identifiedItemAccess: IdentifiedItemAccess): Unit

  private def writeObject(stream: ObjectOutputStream): Unit = {
    stream.writeUnshared("Albert")
  }

  private def readObject(stream: ObjectInputStream): Unit = {
    println(stream.readUnshared())
  }
}


