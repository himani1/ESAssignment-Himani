import org.scalatest.FunSuite

class OperationsTest extends FunSuite{

  val cl=Operations.client()

  test("add a record") {
    val res=Operations.add("2","himani","9873215276","delhi",cl)
    assert(res.getId=="2")
  }

  test("read total counts") {
    val res=Operations.readCount(cl)
    assert(res==3)
  }

  test("update a record") {
    val res=Operations.update("1","city","noida",cl)
    assert(res.getVersion==9)
  }

  test("delete a record") {
    val res=Operations.delete("2",cl)
    assert(res.getTotalDeleted==1)
  }

  test("read from a json file"){
    val res=Operations.readJson("inputJson.json",cl)
    assert(res!=null)
  }

  test("write to a file"){
    val res=Operations.writeJson(cl)
    assert(res!=null)
  }

}
