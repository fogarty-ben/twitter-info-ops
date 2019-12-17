package com.benfogarty.mpcs53014;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;

import com.opencsv.bean.CsvToBeanBuilder;

public class HadoopUsersSerialization {

	public static void main(String[] args) {
		try {
			Configuration conf = new Configuration();
			//String serializations = conf.get("io.serializations").trim();
			//String delim = serializations.isEmpty() ? "" : ",";
			
			final TSerializer ser = new TSerializer(new TBinaryProtocol.Factory());
		
			//conf.set("io.serializations", serializations + delim + UserSerialization.class.getName());
			FileSystem fs = FileSystem.get(conf);
			Path seqFilePath = new Path("/benfogarty/finalProject/users/usersSerialized.out");
			SequenceFile.Writer writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(seqFilePath),
					SequenceFile.Writer.keyClass(IntWritable.class), SequenceFile.Writer.valueClass(BytesWritable.class),
					SequenceFile.Writer.compression(CompressionType.BLOCK));
			
			
			//Documentation at http://opencsv.sourceforge.net/
			List<UserBean> userBeans = new CsvToBeanBuilder<UserBean>(new FileReader(args[0]))
																     .withType(UserBean.class)
																     .build()
																     .parse(); 
			userBeans.forEach(userBean -> writeUser(ser, writer, userBean));
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void writeUser(TSerializer ser, SequenceFile.Writer writer, UserBean user) {
		try {
			IntWritable key = new IntWritable(1);
			User val = user.prepareForSerialization();
			BytesWritable valSerialized = new BytesWritable(ser.serialize(val));
			writer.append(key, valSerialized);
		} catch (IOException | TException e) {
			e.printStackTrace();
		}
	}

}
