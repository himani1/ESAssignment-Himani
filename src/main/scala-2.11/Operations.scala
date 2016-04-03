import java.net.InetAddress

import org.elasticsearch.action.deletebyquery.DeleteByQueryAction
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.action.update.UpdateResponse
import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.plugin.deletebyquery.DeleteByQueryPlugin
import java.io.{File, PrintWriter}
import scala.io.Source
import scala.util.Random

object Operations {

  def client():Client = {

    val client: Client = TransportClient.builder().addPlugin(classOf[DeleteByQueryPlugin]).build()
      .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300))
    client
  }

  def add(id:String,name:String,mobile:String,city:String,client: Client):IndexResponse = {

    val jsonStr=
      s"""{
         "name":"$name",
         "mobile":"$mobile",
         "city":"$city",
         "id":"$id"
         }"""
    client.prepareIndex("company","employee",id).setSource(jsonStr).get()
  }

  def readCount(client:Client):Long={
    client.prepareSearch("company").execute().actionGet().getHits.totalHits()
  }

  def update(id:String,field:String,value:Any,client:Client):UpdateResponse={
    client.prepareUpdate("company","employee",id).setDoc(field,value).get()
  }

  def delete(id:String,client: Client)={
    val res=DeleteByQueryAction.INSTANCE.newRequestBuilder(client).setIndices("company").setQuery(QueryBuilders.termsQuery("_id",id)).get()
    res
  }

  def readJson(file:String,client:Client)={
    val data=Source.fromFile(file).getLines().toList
    val res=client.prepareBulk()
    data.map{
      json => res.add(client.prepareIndex("socialmedia","twitter").setSource(json))
    }
    res.execute().actionGet()
  }

  def writeJson(client:Client)={
    val data=client.prepareSearch("socialmedia").setTypes("twitter").execute().get()
    val res=new PrintWriter(new File("/home/himani/output.json"))
    res.write(data.toString)
  }
}

