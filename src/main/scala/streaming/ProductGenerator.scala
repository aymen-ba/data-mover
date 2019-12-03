package streaming

import java.util.Random

object ProductGenerator {

  val rd: Random = new Random

  def generateProduct(): Product = {
     val productId = rd.nextInt(10000) + 1
     val prixUnitaire = rd.nextFloat() * 100
     val qte = rd.nextInt(9) + 1
     val idMagasin = rd.nextInt(1200) + 1
     Product(productId.toString,prixUnitaire.toString,qte.toString,idMagasin.toString)
  }
}

case class Product(idProduit:String,prixUnitaire:String,quantiteDisponible:String,idMagasin:String)

