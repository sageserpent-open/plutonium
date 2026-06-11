import sys

with open('src/test/scala/com/sageserpent/plutonium/WorldSpecSupportAmericium.scala', 'r') as f:
    content = f.read()

# Fix 1: Remove seedTrials
content = content.replace('  def seedTrials: Trials[Long] = api.longs', '')

# Fix 2: Use api.instants
content = content.replace('  def instantTrials: Trials[Instant] = api.longs.map(Instant.ofEpochMilli)', '  def instantTrials: Trials[Instant] = api.instants')

# Fix 3: Remove redundant guard in shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen
old_shuffle_code = r"""    if (shuffledEventsPerItemTrials.isEmpty) {
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

new_shuffle_code = r"""    api
      .sequences(shuffledEventsPerItemTrials)
      .flatMap(shuffledEventsPerItem =>
        api.pickAlternatelyFrom(
          shrinkToRoundRobin = true,
          shuffledEventsPerItem: _*
        )
      )"""

content = content.replace(old_shuffle_code, new_shuffle_code)

# Fix 4: dataSamplesGenerator in dataSamplesForAnIdTrials - use lists and filter
old_ds_gen = r"""    val dataSamplesGenerator: Trials[Seq[(Int, (Any, (Unbounded[Instant], AHistory#Id) => Event))]] =
      api
        .alternateWithWeights(dataSampleTrials.zipWithIndex map {
          case (trials, index) => 1 -> (trials map (sample => index -> sample))
        })
        .lists
        .filter(_.nonEmpty)"""

# I need to check the line number to make sure I'm replacing the right one.
# It seems fine as it is unique enough.

# Fix 5: final generation in recordingsGroupedByIdTrials_ - use lists and filter
old_recordings_gen = r"api.integers(1, 3).flatMap(recordingsForAnIdTrials.listsOfSize).filter(idsAreNotRepeated)"
new_recordings_gen = r"recordingsForAnIdTrials.lists.filter(_.nonEmpty).filter(idsAreNotRepeated)"
content = content.replace(old_recordings_gen, new_recordings_gen)

with open('src/test/scala/com/sageserpent/plutonium/WorldSpecSupportAmericium.scala', 'w') as f:
    f.write(content)
