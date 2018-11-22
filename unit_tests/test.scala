import org.scalatest._
import Matchers._

// Each test checks the objects produced by a function belong to a required class

class StringToUberRideSpec extends FlatSpec {
  val s="\"4/1/2014 0:33:00\",40.7594,-73.9722,\"B02512\""

  val testUberRide = stringToUberRide(s)

  testUberRide shouldBe an [UberRide]
}

class StringToPOISpec extends FlatSpec {
  val s = "36.979368,-122.020727,bank,Chase"

  val testPOI = stringToPOI(s)

  testPOI shouldBe a [POI]
}

class StringToDateTimeSpec extends FlatSpec {
  val s = "4/1/2014 0:21:00"

  val testDateTime = getLocalDateTime(s)

  testDateTime shouldBe a [DateTime]
}
