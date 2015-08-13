/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import kafka.tools.MirrorMaker
import joptsimple.UnrecognizedOptionException
import java.io.File
import java.io.FileWriter;
import java.util.HashMap;
import org.yaml.snakeyaml.Yaml
import junit.framework.Assert._
import org.junit.Test

class MirrorMakerTest {
  
  def createyamlFile():String = {
		val f = File.createTempFile("mirrormaker", ".yaml")
    val current:Array[String] =  new Array[String](2)
		current(0) ="first_topic"
    current(1) = "second_topic"
    val data: HashMap[String,Array[String] ]  = new java.util.HashMap[String,Array[String]]()
    data.put("whitelist",current)

	  val yaml:Yaml  = new Yaml()
	  val writer: FileWriter = new FileWriter(f.getAbsolutePath())
		yaml.dump(data, writer)
		f.deleteOnExit()
    f.getAbsolutePath()
  }
   
  @Test
  def testparseYaml() {
    assertEquals(MirrorMaker.parseYaml(createyamlFile()),"first_topic|second_topic")
  }
}
