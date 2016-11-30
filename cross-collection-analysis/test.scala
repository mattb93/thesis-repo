import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.mapreduce.AvroKeyInputFormat
import org.apache.avro.mapred.AvroKey
import org.apache.hadoop.io.NullWritable
import org.apache.avro.mapred.AvroInputFormat
import org.apache.avro.mapred.AvroWrapper
import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.{AvroInputFormat, AvroWrapper}
import org.apache.hadoop.io.NullWritable

val path = "/user/mattb93/avrotest/z_157/part-m-00000.avro"
val avroRDD = sc.hadoopFile[AvroWrapper[GenericRecord], NullWritable, AvroInputFormat[GenericRecord]](path)

avroRDD.map(l => new String(l._1.datum.get("text").toString())).map(line => line.split(" "))
