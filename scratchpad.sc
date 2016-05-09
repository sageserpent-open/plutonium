

import scala.collection.mutable


implicit val ttbConfig = mutable.SortedBagConfiguration.keepAll[Int]
val ttb = mutable.TreeBag.empty[Int]
ttb.add(1, 2)
ttb.setMultiplicity(7, 6)
ttb += 2
ttb.getBucket(2)
ttb += 1
ttb +=(2, 2)
ttb.getBucket(2)
ttb += 3
ttb.size
ttb(1)
ttb(2)
ttb(7)
ttb

ttb drop 2

ttb dropWhile (_ < 7)



implicit val htbConfig = mutable.HashedBagConfiguration.keepAll[Int]
val htb = mutable.HashBag.empty[Int]
htb.add(1, 2)
htb.setMultiplicity(7, 6)
htb += 2
htb.getBucket(2)
htb += 1
htb +=(2, 2)
htb.getBucket(2)
htb += 3
htb.size
htb(1)
htb(2)
htb(7)
htb

htb drop 2

htb dropWhile (_ < 7)
