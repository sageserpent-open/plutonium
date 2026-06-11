import sys

with open('src/test/scala/com/sageserpent/plutonium/intersperseObsoleteEventsAmericium.scala', 'r') as f:
    content = f.read()

# Replace explicit Option in chunkKeepingEventIdsUniquePerChunk
old_chunk_sig = r"""  def chunkKeepingEventIdsUniquePerChunk[EventRelatedThing](
      eventIdPieces: Seq[(Option[EventRelatedThing], EventId)]
  ): Trials[Seq[Seq[(Option[EventRelatedThing], EventId)]]] = {"""

new_chunk_sig = r"""  def chunkKeepingEventIdsUniquePerChunk[EventRelatedThing](
      eventIdPieces: Seq[(EventRelatedThing, EventId)]
  ): Trials[Seq[Seq[(EventRelatedThing, EventId)]]] = {"""

content = content.replace(old_chunk_sig, new_chunk_sig)

with open('src/test/scala/com/sageserpent/plutonium/intersperseObsoleteEventsAmericium.scala', 'w') as f:
    f.write(content)
