import sys

with open('src/test/scala/com/sageserpent/plutonium/WorldSpecSupportAmericium.scala', 'r') as f:
    content = f.read()

# Revert to a more predictable size range for lists of recordings to avoid NoValidTrialsException
# and stay constructive.

# 1. In recordingsGroupedByIdTrials_
old_recordings_gen = r"recordingsForAnIdTrials.lists.filter(_.nonEmpty).filter(idsAreNotRepeated)"
new_recordings_gen = r"api.integers(1, 3).flatMap(recordingsForAnIdTrials.listsOfSize).filter(idsAreNotRepeated)"
content = content.replace(old_recordings_gen, new_recordings_gen)

# 2. In dataSamplesForAnIdTrials
old_ds_gen = r"""    val dataSamplesGenerator: Trials[Seq[(Int, (Any, (Unbounded[Instant], AHistory#Id) => Event))]] =
      api
        .alternateWithWeights(dataSampleTrials.zipWithIndex map {
          case (trials, index) => 1 -> (trials map (sample => index -> sample))
        })
        .lists
        .filter(_.nonEmpty)"""

new_ds_gen = r"""    val dataSamplesGenerator: Trials[Seq[(Int, (Any, (Unbounded[Instant], AHistory#Id) => Event))]] =
      api.integers(1, 5).flatMap(size =>
        api
          .alternateWithWeights(dataSampleTrials.zipWithIndex map {
              case (trials, index) => 1 -> (trials map (sample => index -> sample))
            })
          .listsOfSize(size))"""

content = content.replace(old_ds_gen, new_ds_gen)

with open('src/test/scala/com/sageserpent/plutonium/WorldSpecSupportAmericium.scala', 'w') as f:
    f.write(content)
