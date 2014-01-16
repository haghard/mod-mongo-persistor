package org.vertx.mods

import com.mongodb.BasicDBObject
import org.vertx.mods.MQLSyntax._
import scala.util.parsing.combinator.{PackratParsers, JavaTokenParsers}
import scala.annotation.tailrec
import com.mongodb.MongoException

object MQLSyntax {

  trait MQLExpression

  trait MQLOperation extends MQLExpression {
    def symbol: String
  }

  case class MQLgtOperation(val symbol: String = "$gt") extends MQLOperation
  case class MQLgteOperation(val symbol: String = "$gt") extends MQLOperation
  case class MQLltOperation(val symbol: String = "$lt") extends MQLOperation
  case class MQLlteOperation(val symbol: String = "$lte") extends MQLOperation
  case class MQLeqOperation(val symbol: String = "$eq") extends MQLOperation
  case class MQLneOperation(val symbol: String = "$ne") extends MQLOperation
  case class MQLinOperation(val symbol: String = "$in") extends MQLOperation
  case class MQLninOperation(val symbol: String = "$nin") extends MQLOperation

  sealed trait MQLValue extends MQLExpression {
    type T <: AnyRef
    def value: T
  }

  case class MQLString(val value: String) extends MQLValue {
    type T = String
  }

  case class MQLDouble(val value: java.lang.Double) extends MQLValue {
    type T = java.lang.Double
  }

  case class MQLInt(val value: Integer) extends MQLValue {
    type T = java.lang.Integer
  }

  case class MQLIntArray(val value: Array[Int]) extends MQLValue {
    type T = Array[Int]
  }

  case class MQLFloatArray(val value: Array[Double]) extends MQLValue {
    type T = Array[Double]
  }

  case class MQLStringArray(val value: Array[String]) extends MQLValue {
    type T = Array[String]
  }

  case class MQLRight(val symbol: String, val value: MQLValue) extends MQLExpression
}

/**
 * Companion object
 *
 */
object MQLParser {

  def apply() = new MQLParser()
}

class MQLParser extends JavaTokenParsers with PackratParsers {

  type P[T] = PackratParser[T]

  lazy val operation: P[MQLOperation] = ("$gt" | "$gte" | "$ne" | "$nin" | "$lt" | "$lte" | "$eq" | "$in" ) ^^ {
    case "$gt" => MQLgtOperation()
    case "$gte" => MQLgteOperation()
    case "$ne" => MQLneOperation()
    case "$lt" => MQLltOperation()
    case "$lte" => MQLlteOperation()
    case "$eq" => MQLeqOperation()
    case "$in" => MQLinOperation()
    case "$nin" => MQLninOperation()
    case f => throw new UnsupportedOperationException(s"unsupported field ${f} ")
  }

  implicit class StringExt(str: String) {
    def clean = { str.replace("\"","") }
  }

  lazy val strArray: P[MQLStringArray] = "{" ~> stringLiteral ~ rep("," ~> stringLiteral) <~ "}" ^^ {
    case start ~ other => {
      if (other.isEmpty) {
        MQLStringArray(Array(start.clean))
      } else {
        MQLStringArray(other.map({ _.clean }).::(start.clean).toArray)
      }
    }
  }

  lazy val intArray: P[MQLIntArray] = "{" ~> wholeNumber ~ rep("," ~> wholeNumber) <~ "}" ^^ {
    case start ~ other => {
      if (other.isEmpty) {
        MQLIntArray(Array(start.toInt))
      } else {
        MQLIntArray(other.map({ _.toInt }).::(start.toInt).toArray)
      }
    }
  }

  lazy val doubleArray: P[MQLFloatArray] = "{" ~> floatingPointNumber ~ rep("," ~> floatingPointNumber) <~ "}" ^^ {
    case start ~ other => {
      if (other.isEmpty) {
        MQLFloatArray(Array(start.toDouble))
      } else {
        MQLFloatArray(other.map({ _.toDouble }).::(start.toDouble).toArray)
      }
    }
  }

  lazy val array: P[MQLValue] = intArray | doubleArray | strArray

  lazy val predicate: P[MQLRight] = operation ~ (floatingPointNumber | wholeNumber | stringLiteral | array) ^^ {
    case operation ~ value => {
      value match {
        case v: String => {
          if (v.contains(".")) {
            MQLRight(operation.symbol, MQLDouble(v.toDouble))
          } else {
            try {
              MQLRight(operation.symbol, MQLInt(v.toInt))
            } catch {
              case _ : NumberFormatException => MQLRight(operation.symbol, MQLString(v.clean))
            }
          }
        }

        case v: MQLValue => MQLRight(operation.symbol, v)
      }
    }
  }

  lazy val field : P[String] = ident ~ rep("." ~> ident)  ^^ {
    case start ~ other => other match {
      case Nil => start
      case _ => start + "." + other.mkString(".")
    }
  }

  lazy val separateFieldConditions: P[(String, BasicDBObject)] =  field ~ rep1(predicate) ^^ {
    case field ~ preds => {
      val start = preds.head
      val dbObject = new BasicDBObject(start.symbol, start.value.value)
      val anonymousDbObject = loop(preds.tail, dbObject)
      (field -> anonymousDbObject)
    }
  }

  @tailrec
  private def loop(conds: List[MQLRight], startDbObj: BasicDBObject): BasicDBObject = {
    conds match {
      case Nil => startDbObj
      case h :: tail =>
        loop(tail, startDbObj.append(h.symbol, h.value.value))
    }
  }

  def createDbObject(p :(String, BasicDBObject)): BasicDBObject = {
    if (p._2.get("$eq") != null ) {
      new BasicDBObject(p._1, p._2.get("$eq"))
    } else {
      new BasicDBObject(p._1, p._2)
    }
  }

  lazy val querySelectors: P[BasicDBObject] = separateFieldConditions ~ rep(separateFieldConditions) ^^ {
    case first ~ others => {
      if (others.isEmpty) {
        createDbObject(first)
      } else {
        val javaMap: java.util.Map[String, AnyRef] = {
          (first :: others).foldLeft(new java.util.HashMap[String, AnyRef]()) {
            (acc: java.util.HashMap[String, AnyRef], current: (String, BasicDBObject)) => {
              if (current._2.get("$eq") != null) {
                acc.put(current._1, current._2.get("$eq") )
              } else {
                acc.put(current._1, current._2)
              }
              acc
            }
          }
        }
        new BasicDBObject(javaMap)
      }
    }
  }

  lazy val logical: P[String] = ("$or" | "$and" | "$not" | "$nor") ^^ { case v => v }


  lazy val conditionChainArray: P[java.util.List[BasicDBObject]] = "{" ~> querySelectors ~ "}" ~ rep("," ~ "{" ~> querySelectors <~ "}") ^^ {
    case first ~ sep ~ other => {
      if (other.isEmpty)
        java.util.Arrays.asList(first)
      else {
        import scala.collection.JavaConversions._
        val others: java.util.List[BasicDBObject] = first :: other
        others
      }
    }
  }

  lazy val logicalQuerySelectors: P[BasicDBObject] = logical ~ ":" ~ "[" ~ conditionChainArray <~ "]" ^^ {
    case l ~ sep0 ~ sep2 ~ c => new BasicDBObject(l, c)
  }

  lazy val q: P[BasicDBObject] = logicalQuerySelectors | querySelectors

  def parse(source: String): BasicDBObject = {
    val parser: PackratParser[BasicDBObject] = phrase(q)
    import scala.util.parsing.input.CharSequenceReader;
    parser.apply(new PackratReader(new CharSequenceReader(source.toCharArray))) match {
      case Success(dbObject, _) => println(dbObject.toString); dbObject;
      case Error(msg, in) => throw new MongoException("query parse error:" + msg)
      case Failure(msg, _) => throw new MongoException("query parse error:" + msg)
    }
  }
}