import com.sageserpent.americium.Trials
import com.sageserpent.americium.Trials.api

val trials: Trials[(Boolean, Int)] = api.only((true, 1))
val collected = trials.collect {
  case (true, value) => value
}
