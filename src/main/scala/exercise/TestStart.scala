package exercise

object TestStart {

  def main(args: Array[String]): Unit = {
    //partition
    normal
    //println(List("本年利润","bbb","ccc","本年利润aaa").find(_.contains("本年利润")).toList)
  }


  def normal = {
    val producer = new KafkaWorkCountProducer("TopicC");
    new Thread(producer).start()
  }

  def partition = {
    val producer = new KafkaWorkCountKeyProducer("TopicOne");
    new Thread(producer).start()
  }
}
