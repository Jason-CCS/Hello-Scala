package hello.world.scala.collection.function

import scala.collection.mutable.ListBuffer

object FoldLeft {

  case class MachineAction(machine: String, state: String, epochTime: Long)

  case class StateChangeHistory(machine: String, startState: String, endState: String, startEpochTime: Long, endEpochTime: Long)

  def calculate(logs: List[MachineAction]): List[StateChangeHistory] = {
    val resultList = ListBuffer[StateChangeHistory]()

    logs.foldLeft(Map.empty[String, (StateChangeHistory, Int)]) { case (map, record) =>
      val currentState = record.state
      if (!map.contains(record.machine)) {
        val newHistory = StateChangeHistory(record.machine, currentState, "", record.epochTime, 0)
        resultList += newHistory
        map + (record.machine -> (newHistory, resultList.size - 1))
      } else {
        val oldHistory = map(record.machine)._1
        val oldIndex = map(record.machine)._2
        if (oldHistory.endState.isEmpty) { // if endState is empty, update the existing history record
          if (oldHistory.startState != currentState) {
            val newHistory = oldHistory.copy(endState = record.state, endEpochTime = record.epochTime)
            resultList(oldIndex) = newHistory
            val placeholderHistory = newHistory.copy(startState = newHistory.endState, endState = "", startEpochTime = newHistory.endEpochTime, endEpochTime = 0)
            resultList += placeholderHistory
            map.updated(record.machine, (placeholderHistory, resultList.size - 1))
          } else map
        } else { // if endState is not empty, add one more history record
          if (oldHistory.endState != currentState) {
            val newHistory = oldHistory.copy(startState = oldHistory.endState, endState = record.state, startEpochTime = oldHistory.endEpochTime, endEpochTime = record.epochTime) // clone
            resultList += newHistory
            val placeholderHistory = newHistory.copy(startState = newHistory.endState, endState = "", startEpochTime = newHistory.endEpochTime, endEpochTime = 0)
            resultList += placeholderHistory
            map.updated(record.machine, (placeholderHistory, resultList.size - 1))
          } else map
        }
      }
    }
    resultList.toList
  }

  def main(args: Array[String]): Unit = {
    val logs = List(
      MachineAction("M1", "IDLE", 1800),
      MachineAction("M2", "IDLE", 1801),
      MachineAction("M2", "Running", 1802),
      MachineAction("M3", "IDLE", 1803),
      MachineAction("M4", "IDLE", 1804),
      MachineAction("M5", "IDLE", 1805),
      MachineAction("M1", "RUNNING", 1806),
      MachineAction("M2", "Stopping", 1807),
      MachineAction("M3", "RUNNING", 1808),
      MachineAction("M4", "IDLE", 1809),
      MachineAction("M5", "RUNNING", 1810)
    )
    calculate(logs).foreach(println)
  }

}
