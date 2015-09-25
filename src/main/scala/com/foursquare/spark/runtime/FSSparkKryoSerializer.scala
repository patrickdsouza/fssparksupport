// Copyright 2015 Foursquare Labs Inc. All Rights Reserved.

package com.foursquare.spark.runtime

import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.esotericsoftware.kryo.io.{Input, Output}
import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.thrift.{TBase, TDeserializer, TFieldIdEnum, TSerializer}
import org.apache.thrift.protocol.TCompactProtocol

class TBaseSerializer extends Serializer[TBase[_ <: TBase[_, _], _ <: TFieldIdEnum]] {
  val protocolFactory = new TCompactProtocol.Factory()
  val serializer = new TSerializer(protocolFactory)
  val deserializer = new TDeserializer(protocolFactory)

  override def write(kryo: Kryo, output: Output, instance: TBase[_ <: TBase[_, _], _ <: TFieldIdEnum]): Unit = {
    val bytes = serializer.serialize(instance)
    output.writeInt(bytes.length, true)
    output.writeBytes(bytes)
  }

  override def read(
    kryo: Kryo,
    input: Input,
    clazz: Class[TBase[_ <: TBase[_, _], _ <: TFieldIdEnum]]
  ): TBase[_ <: TBase[_, _], _ <: TFieldIdEnum] = {
    val length = input.readInt(true)
    val bytes = new Array[Byte](length)
    input.readBytes(bytes)

    val instance = clazz.newInstance()
    deserializer.deserialize(instance, bytes)
    instance
  }
}

class FSSparkKryoSerializer(conf: SparkConf) extends KryoSerializer(conf) {
  override def newKryo(): Kryo = {
    val kryo = super.newKryo()
    kryo.addDefaultSerializer(classOf[TBase[_, _]], new TBaseSerializer)
    kryo
  }
}
