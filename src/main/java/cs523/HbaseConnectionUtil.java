package cs523;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HbaseConnectionUtil {
	final static Logger logger = LoggerFactory.getLogger(HbaseConnectionUtil.class);

	private static final String TABLE_NAME = "ds_salaries";
	private static final String CF_DEFAULT = "row_key";
	private static final String CF_INFO = "job_info";
	private static final String CF_SALARY = "salary";
	
	private static Configuration config;

	public HbaseConnectionUtil() throws IOException{
		createConnectionAndTable();
	}
	
	private void createConnectionAndTable() throws IOException
	{
		config = HBaseConfiguration.create();

		try (Connection connection = ConnectionFactory.createConnection(config);
				Admin admin = connection.getAdmin())
		{
			HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
			table.addFamily(new HColumnDescriptor(CF_DEFAULT).setCompressionType(Algorithm.NONE));
			table.addFamily(new HColumnDescriptor(CF_INFO));
			table.addFamily(new HColumnDescriptor(CF_SALARY));

			logger.info("Creating table.... ");

			if (admin.tableExists(table.getTableName()))
			{
				admin.disableTable(table.getTableName());
				admin.deleteTable(table.getTableName());
			}
			admin.createTable(table);

			logger.info("Table created.... ");
		}
	}
	
	public void saveDataToHbase(List<DsSalariesDTO> dto) throws IOException{
		if (dto.isEmpty()) {
			logger.info("Data empty!.... ");
            return;
        }
        for (DsSalariesDTO d : dto) {
            HTable dsTable = new HTable(config, TABLE_NAME);
    		Put row = new Put(Bytes.toBytes(d.getId()));
    		
    		row.add(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes("workYear"),Bytes.toBytes(d.getWorkYear()));
    		row.add(Bytes.toBytes(CF_INFO), Bytes.toBytes("experienceLevel"),Bytes.toBytes(d.getExperienceLevel()));
    		row.add(Bytes.toBytes(CF_INFO), Bytes.toBytes("jobTitle"),Bytes.toBytes(d.getJobTitle()));
    		row.add(Bytes.toBytes(CF_INFO), Bytes.toBytes("companySize"),Bytes.toBytes(d.getCompanySize()));
    		row.add(Bytes.toBytes(CF_SALARY), Bytes.toBytes("salary"),Bytes.toBytes(d.getSalary()));
    		row.add(Bytes.toBytes(CF_SALARY), Bytes.toBytes("salaryCurrency"),Bytes.toBytes(d.getSalaryCurrency()));
    		row.add(Bytes.toBytes(CF_SALARY), Bytes.toBytes("salaryInUsd"),Bytes.toBytes(d.getSalaryInUsd()));
    		
    		dsTable.put(row);
    		logger.info("Data inserted.... ");
        }
	}
}
