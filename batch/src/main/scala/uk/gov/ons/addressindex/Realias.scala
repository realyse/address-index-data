import com.typesafe.config.ConfigFactory
import org.rogach.scallop.{ScallopConf, ScallopOption}
import scalaj.http.{Http, HttpResponse}
import play.api.libs.json._

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

  val get_response: HttpResponse[String] = Http(url + "_alias/*").asString
  if (get_response.code != 200) throw new Exception(s"Could not get aliases using GET: code ${get_response.code} body ${get_response.body}")

  val get_json = Json.parse(get_response.body).as[JsObject]

  val target_indexes_regex = s"^hybrid(-historical)?(-skinny)?_${opts.epoch}_".r
  val target_indexes = get_json.fields.filter({case (target_indexes_regex(_), _) => true})

  val target_aliases_regex = s"beta(_full|_skinny)?(_hist|_nohist)?_${opts.epoch}".r
//  val target_aliases = get_json.fields.map((_, v) => v.filter({case (target_aliases_regex(_), _) => true}))
  val target_aliases = get_json.fields.map({
    case (_, v) => (v \ "aliases").as[JsObject].keys.filter({case target_aliases_regex() => true})
  })

  // beta_full_hist_65          -> hybrid-historical_65_......_.............
  // beta_full_hist_current     -> hybrid-historical_65_......_.............
  // beta_full_nohist_65        -> hybrid_65_......_.............
  // beta_full_nohist_current   -> hybrid_65_......_.............
  // beta_skinny_hist_65        -> hybrid-historical-skinny_65_......_.............
  // beta_skinny_hist_current   -> hybrid-historical-skinny_65_......_.............
  // beta_skinny_nohist_65      -> hybrid-skinny_65_......_.............
  // beta_skinny_nohist_current -> hybrid-skinny_65_......_.............

  val data: JsValue = Json.obj(
    "actions" -> Json.arr(
      Json.obj("add" -> Json.obj("index" -> false, "alias" -> false)),
      Json.obj("remove" -> Json.obj("index" -> false, "alias" -> false))
    )
  )

  val post_response: HttpResponse[String] = Http(url + "_alias")
    .postData(data)
    .header("Content-type", "application/json")
    .asString
  if (post_response.code != 200) throw new Exception(s"Could not create aliases using POST: code ${post_response.code} body ${post_response.body}")
}
