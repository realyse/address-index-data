package uk.gov.ons.addressindex

import com.typesafe.config.ConfigFactory
import org.rogach.scallop.{ScallopConf, ScallopOption}
import play.api.libs.functional.syntax._
import play.api.libs.json._
import scalaj.http.{Http, HttpResponse}

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

    val epoch_opt: ScallopOption[String] = opt(
      "epoch", descr = "Epoch to update aliases to",
      noshort = true, required = true)
    val prefix_opt: ScallopOption[String] = opt(
      "prefix", descr = "Prefix to place before the index names",
      default = Some("index"), noshort = true)
    verify()
    val epoch: String = epoch_opt.toOption.get
    val prefix: String = prefix_opt.toOption.get
  }

  // Class definitions, only used here, mostly for parsing JSON

  sealed trait AliasCommand

  case class RemoveAlias(index: String, alias: String) extends AliasCommand

  case class AddAlias(index: String, alias: String) extends AliasCommand

  // The JSON document posted to /_aliases
  case class PostAliases(actions: Seq[AliasCommand])

  // The inner element of the JSON document received from /_aliases/*
  // We can't describe the entire document as a class, because it uses dynamic keys
  case class IndexAliases(index: String, aliases: Seq[String])

  // Classes for the POST to /_aliases

  implicit val writeRemoveAlias: Writes[RemoveAlias] = (
    (__ \ "remove" \ "index").write[String] and
      (__ \ "remove" \ "alias").write[String]
    ) (unlift(RemoveAlias.unapply))

  implicit val writeAddAlias: Writes[AddAlias] = (
    (__ \ "add" \ "index").write[String] and
      (__ \ "add" \ "alias").write[String]
    ) (unlift(AddAlias.unapply))

  implicit val writeAliasCommand: Writes[AliasCommand] = new Writes[AliasCommand] {
    def writes(ac: AliasCommand): JsValue = {
      ac match {
        case c: RemoveAlias => writeRemoveAlias.writes(c)
        case c: AddAlias => writeAddAlias.writes(c)
      }
    }
  }

  implicit val writePostAliases: Writes[PostAliases] =
    (__ \ "actions").write[Seq[AliasCommand]].contramap[PostAliases](_.actions)

  // Classes for the GET from /_aliases/*

  implicit val readIndexAliases: Reads[Seq[IndexAliases]] = new Reads[Seq[IndexAliases]] {
    def reads(j: JsValue): JsResult[Seq[IndexAliases]] = {
      JsSuccess(j.as[JsObject].fields.map(
        t => IndexAliases(t._1, (t._2 \ "aliases").as[JsObject].keys.toSeq)
      ))
    }
  }

  // Main code start

  val nodes = config.getString("addressindex.elasticsearch.nodes")
  val port = config.getString("addressindex.elasticsearch.port")

  val url = s"http://$nodes:$port/"

  val get_response: HttpResponse[String] = Http(url + "_alias/*").asString
  if (get_response.code != 200) throw new Exception(s"Could not get aliases using GET: code ${get_response.code} body ${get_response.body}")

  val index_aliases = Json.parse(get_response.body).as[Seq[IndexAliases]]

  val target_indexes_regex = raw"^hybrid(-historical)?(-skinny)?_${opts.epoch}".r
  val target_indexes = index_aliases.filter(i => target_indexes_regex.findFirstIn(i.index).isDefined).map(_.index)
  println(s"Current indexes: ${target_indexes.mkString(",")}")

  val target_aliases_regex = raw"^${opts.prefix}_(full|skinny)_(hist|nohist)_(${opts.epoch}|current)".r
  val target_aliases = index_aliases.flatMap(_.aliases).filter(target_aliases_regex.findFirstIn(_).isDefined)
  println(s"Current aliases: ${target_aliases.mkString(",")}")

  // create an alias from an index name
  def make_aliases(index_name: String): Seq[String] = {
    target_indexes_regex.findFirstMatchIn(index_name) match {
      case Some(m) =>
        val hist = if (m.group(1) == "-historical") "hist" else "nohist"
        val size = if (m.group(2) == "-skinny") "skinny" else "full"

        Seq(s"${opts.prefix}_${size}_${hist}_current", s"${opts.prefix}_${size}_${hist}_${opts.epoch}")
    }
  }

  // replace an alias only if we have an index for it to point at
  val alias_additions = target_indexes.flatMap(i => make_aliases(i).map((i, _))).map(t => AddAlias(t._1, t._2))

  // delete an alias only if we're going to replace it, and it exists currently
  val alias_deletions = alias_additions.filter(c => target_aliases.contains(c.alias)).map(c => RemoveAlias(c.index, c.alias))

  // beta_full_hist_65          -> hybrid-historical_65_......_.............
  // beta_full_hist_current     -> hybrid-historical_65_......_.............
  // beta_full_nohist_65        -> hybrid_65_......_.............
  // beta_full_nohist_current   -> hybrid_65_......_.............
  // beta_skinny_hist_65        -> hybrid-historical-skinny_65_......_.............
  // beta_skinny_hist_current   -> hybrid-historical-skinny_65_......_.............
  // beta_skinny_nohist_65      -> hybrid-skinny_65_......_.............
  // beta_skinny_nohist_current -> hybrid-skinny_65_......_.............

  val data = PostAliases(alias_deletions ++ alias_additions)

  println(Json.toJson[PostAliases](data).toString)

  //  val post_response: HttpResponse[String] = Http(url + "_alias")
  //    .postData(Json.toJson[PostAliases](data).toString())
  //    .header("Content-type", "application/json")
  //    .asString
  //  if (post_response.code != 200) throw new Exception(s"Could not create aliases using POST: code ${post_response.code} body ${post_response.body}")
}
