package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.Unbounded

case class SerializableEvent(when: Unbounded[Instant], recordOnTo: PatchRecorder => Unit)

