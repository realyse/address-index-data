
object Realias extends App {

  val config = ConfigFactory.load()

  val opts = new ScallopConf(args) {
    banner(
      """
Realias script.

Automatically updates all of the ES cluster indexes to match the specified epoch, if it exists.

Example: sbt "realias 65"

For usage see below:
      """)

    val epoch: ScallopOption[String] = opt("epoch", noshort = true, descr = "Epoch to update aliases to")
    verify()
  }

  val nodes = config.getString("addressindex.elasticsearch.nodes")
  val port = config.getString("addressindex.elasticsearch.port")

  val url = s"http://$nodes:$port/"

  val get_response: HttpResponse[String] = Http(url + "_alias/*").get().asString
  if (get_response.code != 200) throw new Exception(s"Could not get aliases using GET: code ${get_response.code} body ${get_response.body}")

  val post_response: HttpResponse[String] = Http(url + "_alias")
    .post()
    .header("Content-type", "application/json")
    .asString
  if (post_response.code != 200) throw new Exception(s"Could not create aliases using POST: code ${post_response.code} body ${post_response.body}")
}
