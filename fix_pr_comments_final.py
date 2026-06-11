import sys

with open('src/test/scala/com/sageserpent/plutonium/WorldSpecSupportAmericium.scala', 'r') as f:
    content = f.read()

# 1. Remove seedTrials
content = content.replace('  def seedTrials: Trials[Long] = api.longs', '')

# 2. Use api.instants
content = content.replace('  def instantTrials: Trials[Instant] = api.longs.map(Instant.ofEpochMilli)', '  def instantTrials: Trials[Instant] = api.instants')

# 3. Variadic parameters for dataSamplesForAnIdTrials
# Old signature:
#   def dataSamplesForAnIdTrials[AHistory <: History: TypeTag](
#       historyIdTrials: Trials[AHistory#Id],
#       dataSampleTrials: Seq[Trials[
#         (_, (Unbounded[Instant], AHistory#Id) => Event)
#       ]]
#   ): Trials[(
#
# New signature:
#   def dataSamplesForAnIdTrials[AHistory <: History: TypeTag](
#       historyIdTrials: Trials[AHistory#Id],
#       dataSampleTrials: Trials[
#         (_, (Unbounded[Instant], AHistory#Id) => Event)
#       ]*
#   ): Trials[(

content = content.replace(
    'dataSampleTrials: Seq[Trials[\n        (_, (Unbounded[Instant], AHistory#Id) => Event)\n      ]]',
    'dataSampleTrials: Trials[\n        (_, (Unbounded[Instant], AHistory#Id) => Event)\n      ]*'
)

# 4. Remove guard in shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen
old_shuffle_block = r"""    if (shuffledEventsPerItemTrials.isEmpty) {
        api.only(Vector.empty)
    } else {
        api
          .sequences(shuffledEventsPerItemTrials)
          .flatMap(shuffledEventsPerItem =>
            api.pickAlternatelyFrom(
              shrinkToRoundRobin = true,
              shuffledEventsPerItem: _*
            )
          )
    }"""

new_shuffle_block = r"""    api
      .sequences(shuffledEventsPerItemTrials)
      .flatMap(shuffledEventsPerItem =>
        api.pickAlternatelyFrom(
          shrinkToRoundRobin = true,
          shuffledEventsPerItem: _*
        )
      )"""

content = content.replace(old_shuffle_block, new_shuffle_block)

# 5. Revert match in events to be partial
old_match = r"""                        changes :+ annihilationFor(eventWhens.last match {
                          case Finite(definiteWhen) => definiteWhen
                          case _ => Instant.now() // Should not happen given require
                        })"""

new_match = r"""                        changes :+ annihilationFor(eventWhens.last match {
                          case Finite(definiteWhen) => definiteWhen
                        })"""

content = content.replace(old_match, new_match)

# 6. Prefer .lists and filter over manual size generation
# Part A: dataSamplesGenerator in dataSamplesForAnIdTrials
old_ds_gen = r"""    val dataSamplesGenerator: Trials[Seq[(Int, (Any, (Unbounded[Instant], AHistory#Id) => Event))]] =
      api.integers(1, 5).flatMap(size =>
        api
          .alternateWithWeights(dataSampleTrials.zipWithIndex map {
              case (trials, index) => 1 -> (trials map (sample => index -> sample))
            })
          .listsOfSize(size))"""

new_ds_gen = r"""    val dataSamplesGenerator: Trials[Seq[(Int, (Any, (Unbounded[Instant], AHistory#Id) => Event))]] =
      api
        .alternateWithWeights(dataSampleTrials.zipWithIndex map {
          case (trials, index) => 1 -> (trials map (sample => index -> sample))
        })
        .lists
        .filter(_.nonEmpty)"""

content = content.replace(old_ds_gen, new_ds_gen)

# Part B: final generation in recordingsGroupedByIdTrials_
old_recordings_gen = r"api.integers(1, 3).flatMap(recordingsForAnIdTrials.listsOfSize).filter(idsAreNotRepeated)"
new_recordings_gen = r"recordingsForAnIdTrials.lists.filter(_.nonEmpty).filter(idsAreNotRepeated)"

content = content.replace(old_recordings_gen, new_recordings_gen)

with open('src/test/scala/com/sageserpent/plutonium/WorldSpecSupportAmericium.scala', 'w') as f:
    f.write(content)
