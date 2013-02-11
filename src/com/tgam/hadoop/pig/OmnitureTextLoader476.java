package com.tgam.hadoop.pig;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.Expression;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.util.Utils;

import com.tgam.hadoop.mapreduce.OmnitureDataFileInputFormat476;
import com.tgam.hadoop.mapreduce.OmnitureDataFileRecordReader476;

/**
 * A Pig custom loader for reading and parsing raw Omniture daily hit data files (hit_data.tsv).
 * @author Mike Sukmanowsky (<a href="mailto:mike.sukmanowsky@gmail.com">mike.sukmanowsky@gmail.com</a>)
 */
public class OmnitureTextLoader476 extends LoadFunc implements LoadMetadata {
	// private static final Log LOG = LogFactory.getLog(OmnitureTextLoader.class);
	
	private static final String DELIMITER = "\\t";
	// Yes, I could store this schema somewhere else rather than hard code, but in the case of Omniture's
	// files they change so infrequently that it seemed to make sense to put it in schema.txt then use
	// a tiny Ruby script to generate the hard coded string
	
	private static final String STRING_SCHEMA_200 = "hit_time_gmt:long,service:chararray,accept_language:chararray,date_time:chararray,visid_high:chararray,visid_low:chararray,event_list:chararray,homepage:chararray,ip:chararray,page_event:int,page_event_var1:chararray,page_event_var2:chararray,page_type:chararray,page_url:chararray,pagename:chararray,product_list:chararray,user_server:chararray,channel:chararray,prop1:chararray,prop2:chararray,prop3:chararray,prop4:chararray,prop5:chararray,prop6:chararray,prop7:chararray,prop8:chararray,prop9:chararray,prop10:chararray,prop11:chararray,prop12:chararray,prop13:chararray,prop14:chararray,prop15:chararray,prop16:chararray,prop17:chararray,prop18:chararray,prop19:chararray,prop20:chararray,prop21:chararray,prop22:chararray,prop23:chararray,prop24:chararray,prop25:chararray,prop26:chararray,prop27:chararray,prop28:chararray,prop29:chararray,prop30:chararray,prop31:chararray,prop32:chararray,prop33:chararray,prop34:chararray,prop35:chararray,prop36:chararray,prop37:chararray,prop38:chararray,prop39:chararray,prop40:chararray,prop41:chararray,prop42:chararray,prop43:chararray,prop44:chararray,prop45:chararray,prop46:chararray,prop47:chararray,prop48:chararray,prop49:chararray,prop50:chararray,purchaseid:chararray,referrer:chararray,state:chararray,user_agent:chararray,zip:chararray,search_engine:int,exclude_hit:int,hier1:chararray,hier2:chararray,hier3:chararray,hier4:chararray,hier5:chararray,browser:int,post_browser_height:int,post_browser_width:int,post_cookies:chararray,post_java_enabled:chararray,post_persistent_cookie:chararray,color:int,connection_type:int,country:int,domain:chararray,post_t_time_info:chararray,javascript:int,language:int,os:int,plugins:chararray,resolution:int,last_hit_time_gmt:long,first_hit_time_gmt:long,visit_start_time_gmt:long,last_purchase_time_gmt:long,last_purchase_num:long,first_hit_page_url:chararray,first_hit_pagename:chararray,visit_start_page_url:chararray,visit_start_pagename:chararray,first_hit_referrer:chararray,visit_referrer:chararray,visit_search_engine:int,visit_num:long,visit_page_num:long,prev_page:long,geo_city:chararray,geo_country:chararray,geo_region:chararray,duplicate_purchase:int,new_visit:int,daily_visitor:int,hourly_visitor:int,monthly_visitor:int,yearly_visitor:int,post_campaign:chararray,evar1:chararray,evar2:chararray,evar3:chararray,evar4:chararray,evar5:chararray,evar6:chararray,evar7:chararray,evar8:chararray,evar9:chararray,evar10:chararray,evar11:chararray,evar12:chararray,evar13:chararray,evar14:chararray,evar15:chararray,evar16:chararray,evar17:chararray,evar18:chararray,evar19:chararray,evar20:chararray,evar21:chararray,evar22:chararray,evar23:chararray,evar24:chararray,evar25:chararray,evar26:chararray,evar27:chararray,evar28:chararray,evar29:chararray,evar30:chararray,evar31:chararray,evar32:chararray,evar33:chararray,evar34:chararray,evar35:chararray,evar36:chararray,evar37:chararray,evar38:chararray,evar39:chararray,evar40:chararray,evar41:chararray,evar42:chararray,evar43:chararray,evar44:chararray,evar45:chararray,evar46:chararray,evar47:chararray,evar48:chararray,evar49:chararray,evar50:chararray,post_evar1:chararray,post_evar2:chararray,post_evar3:chararray,post_evar4:chararray,post_evar5:chararray,post_evar6:chararray,post_evar7:chararray,post_evar8:chararray,post_evar9:chararray,post_evar10:chararray,post_evar11:chararray,post_evar12:chararray,post_evar13:chararray,post_evar14:chararray,post_evar15:chararray,post_evar16:chararray,post_evar17:chararray,post_evar18:chararray,post_evar19:chararray,post_evar20:chararray,post_evar21:chararray,post_evar22:chararray,post_evar23:chararray,post_evar24:chararray,post_evar25:chararray,post_evar26:chararray,post_evar27:chararray,post_evar28:chararray,post_evar29:chararray,post_evar30:chararray,post_evar31:chararray,post_evar32:chararray,post_evar33:chararray,post_evar34:chararray,post_evar35:chararray,post_evar36:chararray,post_evar37:chararray,post_evar38:chararray,post_evar39:chararray,post_evar40:chararray,post_evar41:chararray,post_evar42:chararray,post_evar43:chararray,post_evar44:chararray,post_evar45:chararray,post_evar46:chararray,post_evar47:chararray,post_evar48:chararray,post_evar49:chararray,post_evar50:chararray,click_action:chararray,click_action_type:chararray,click_context:chararray,click_context_type:chararray,click_sourceid:chararray,click_tag:chararray";

	private static final String STRING_SCHEMA_400 = "accept_language:chararray,browser:int,browser_height:chararray,browser_width:chararray,c_color:chararray,campaign:chararray,channel:chararray,click_action:chararray,click_action_type:chararray,click_context:chararray,click_context_type:chararray,click_sourceid:chararray,click_tag:chararray,code_ver:chararray,color:chararray,connection_type:int,cookies:chararray,country:int,ct_connect_type:chararray,curr_factor:chararray,curr_rate:chararray,currency:chararray,cust_hit_time_gmt:long,cust_visid:chararray,daily_visitor:int,date_time:chararray,domain:chararray,duplicate_events:chararray,duplicate_purchase:int,duplicated_from:chararray,evar1:chararray,evar10:chararray,evar11:chararray,evar12:chararray,evar13:chararray,evar14:chararray,evar15:chararray,evar16:chararray,evar17:chararray,evar18:chararray,evar19:chararray,evar2:chararray,evar20:chararray,evar21:chararray,evar22:chararray,evar23:chararray,evar24:chararray,evar25:chararray,evar26:chararray,evar27:chararray,evar28:chararray,evar29:chararray,evar3:chararray,evar30:chararray,evar31:chararray,evar32:chararray,evar33:chararray,evar34:chararray,evar35:chararray,evar36:chararray,evar37:chararray,evar38:chararray,evar39:chararray,evar4:chararray,evar40:chararray,evar41:chararray,evar42:chararray,evar43:chararray,evar44:chararray,evar45:chararray,evar46:chararray,evar47:chararray,evar48:chararray,evar49:chararray,evar5:chararray,evar50:chararray,evar51:chararray,evar52:chararray,evar53:chararray,evar54:chararray,evar55:chararray,evar56:chararray,evar57:chararray,evar58:chararray,evar59:chararray,evar6:chararray,evar60:chararray,evar61:chararray,evar62:chararray,evar63:chararray,evar64:chararray,evar65:chararray,evar66:chararray,evar67:chararray,evar68:chararray,evar69:chararray,evar7:chararray,evar70:chararray,evar71:chararray,evar72:chararray,evar73:chararray,evar74:chararray,evar75:chararray,evar8:chararray,evar9:chararray,event_list:chararray,exclude_hit:int,first_hit_page_url:chararray,first_hit_pagename:chararray,first_hit_referrer:chararray,first_hit_time_gmt:long,geo_city:chararray,geo_country:chararray,geo_dma:chararray,geo_region:chararray,geo_zip:chararray,hier1:chararray,hier2:chararray,hier3:chararray,hier4:chararray,hier5:chararray,hit_source:chararray,hit_time_gmt:long,hitid_high:chararray,hitid_low:chararray,homepage:chararray,hourly_visitor:int,ip:chararray,ip2:chararray,j_jscript:chararray,java_enabled:chararray,javascript:int,language:int,last_hit_time_gmt:long,last_purchase_num:long,last_purchase_time_gmt:long,mobile_id:chararray,monthly_visitor:chararray,mvvar1:chararray,mvvar2:chararray,mvvar3:chararray,namespace:chararray,new_visit:int,os:int,p_plugins:chararray,page_event:int,page_event_var1:chararray,page_event_var2:chararray,page_event_var3:chararray,page_type:chararray,page_url:chararray,pagename:chararray,paid_search:chararray,partner_plugins:chararray,persistent_cookie:chararray,plugins:chararray,post_browser_height:int,post_browser_width:int,post_campaign:chararray,post_channel:chararray,post_cookies:chararray,post_currency:chararray,post_cust_hit_time_gmt:long,post_cust_visid:chararray,post_evar1:chararray,post_evar10:chararray,post_evar11:chararray,post_evar12:chararray,post_evar13:chararray,post_evar14:chararray,post_evar15:chararray,post_evar16:chararray,post_evar17:chararray,post_evar18:chararray,post_evar19:chararray,post_evar2:chararray,post_evar20:chararray,post_evar21:chararray,post_evar22:chararray,post_evar23:chararray,post_evar24:chararray,post_evar25:chararray,post_evar26:chararray,post_evar27:chararray,post_evar28:chararray,post_evar29:chararray,post_evar3:chararray,post_evar30:chararray,post_evar31:chararray,post_evar32:chararray,post_evar33:chararray,post_evar34:chararray,post_evar35:chararray,post_evar36:chararray,post_evar37:chararray,post_evar38:chararray,post_evar39:chararray,post_evar4:chararray,post_evar40:chararray,post_evar41:chararray,post_evar42:chararray,post_evar43:chararray,post_evar44:chararray,post_evar45:chararray,post_evar46:chararray,post_evar47:chararray,post_evar48:chararray,post_evar49:chararray,post_evar5:chararray,post_evar50:chararray,post_evar51:chararray,post_evar52:chararray,post_evar53:chararray,post_evar54:chararray,post_evar55:chararray,post_evar56:chararray,post_evar57:chararray,post_evar58:chararray,post_evar59:chararray,post_evar6:chararray,post_evar60:chararray,post_evar61:chararray,post_evar62:chararray,post_evar63:chararray,post_evar64:chararray,post_evar65:chararray,post_evar66:chararray,post_evar67:chararray,post_evar68:chararray,post_evar69:chararray,post_evar7:chararray,post_evar70:chararray,post_evar71:chararray,post_evar72:chararray,post_evar73:chararray,post_evar74:chararray,post_evar75:chararray,post_evar8:chararray,post_evar9:chararray,post_event_list:chararray,post_hier1:chararray,post_hier2:chararray,post_hier3:chararray,post_hier4:chararray,post_hier5:chararray,post_java_enabled:chararray,post_keywords:chararray,post_mvvar1:chararray,post_mvvar2:chararray,post_mvvar3:chararray,post_page_event:chararray,post_page_event_var1:chararray,post_page_event_var2:chararray,post_page_event_var3:chararray,post_page_type:chararray,post_page_url:chararray,post_pagename:chararray,post_pagename_no_url:chararray,post_partner_plugins:chararray,post_persistent_cookie:chararray,post_product_list:chararray,post_prop1:chararray,post_prop10:chararray,post_prop11:chararray,post_prop12:chararray,post_prop13:chararray,post_prop14:chararray,post_prop15:chararray,post_prop16:chararray,post_prop17:chararray,post_prop18:chararray,post_prop19:chararray,post_prop2:chararray,post_prop20:chararray,post_prop21:chararray,post_prop22:chararray,post_prop23:chararray,post_prop24:chararray,post_prop25:chararray,post_prop26:chararray,post_prop27:chararray,post_prop28:chararray,post_prop29:chararray,post_prop3:chararray,post_prop30:chararray,post_prop31:chararray,post_prop32:chararray,post_prop33:chararray,post_prop34:chararray,post_prop35:chararray,post_prop36:chararray,post_prop37:chararray,post_prop38:chararray,post_prop39:chararray,post_prop4:chararray,post_prop40:chararray,post_prop41:chararray,post_prop42:chararray,post_prop43:chararray,post_prop44:chararray,post_prop45:chararray,post_prop46:chararray,post_prop47:chararray,post_prop48:chararray,post_prop49:chararray,post_prop5:chararray,post_prop50:chararray,post_prop51:chararray,post_prop52:chararray,post_prop53:chararray,post_prop54:chararray,post_prop55:chararray,post_prop56:chararray,post_prop57:chararray,post_prop58:chararray,post_prop59:chararray,post_prop6:chararray,post_prop60:chararray,post_prop61:chararray,post_prop62:chararray,post_prop63:chararray,post_prop64:chararray,post_prop65:chararray,post_prop66:chararray,post_prop67:chararray,post_prop68:chararray,post_prop69:chararray,post_prop7:chararray,post_prop70:chararray,post_prop71:chararray,post_prop72:chararray,post_prop73:chararray,post_prop74:chararray,post_prop75:chararray,post_prop8:chararray,post_prop9:chararray,post_purchaseid:chararray,post_referrer:chararray,post_search_engine:chararray,post_state:chararray,post_survey:chararray,post_t_time_info:chararray,post_tnt:chararray,post_transactionid:chararray,post_visid_high:chararray,post_visid_low:chararray,post_visid_type:chararray,post_zip:chararray,prev_page:long,product_list:chararray,product_merchandising:chararray,prop1:chararray,prop10:chararray,prop11:chararray,prop12:chararray,prop13:chararray,prop14:chararray,prop15:chararray,prop16:chararray,prop17:chararray,prop18:chararray,prop19:chararray,prop2:chararray,prop20:chararray,prop21:chararray,prop22:chararray,prop23:chararray,prop24:chararray,prop25:chararray,prop26:chararray,prop27:chararray,prop28:chararray,prop29:chararray,prop3:chararray,prop30:chararray,prop31:chararray,prop32:chararray,prop33:chararray,prop34:chararray,prop35:chararray,prop36:chararray,prop37:chararray,prop38:chararray,prop39:chararray,prop4:chararray,prop40:chararray,prop41:chararray,prop42:chararray,prop43:chararray,prop44:chararray,prop45:chararray,prop46:chararray,prop47:chararray,prop48:chararray,prop49:chararray,prop5:chararray,prop50:chararray,prop51:chararray,prop52:chararray,prop53:chararray,prop54:chararray,prop55:chararray,prop56:chararray,prop57:chararray,prop58:chararray,prop59:chararray,prop6:chararray,prop60:chararray,prop61:chararray,prop62:chararray,prop63:chararray,prop64:chararray,prop65:chararray,prop66:chararray,prop67:chararray,prop68:chararray,prop69:chararray,prop7:chararray,prop70:chararray,prop71:chararray,prop72:chararray,prop73:chararray,prop74:chararray,prop75:chararray,prop8:chararray,prop9:chararray,purchaseid:chararray,quarterly_visitor:chararray,ref_domain:chararray,ref_type:chararray,referrer:chararray,resolution:int,s_resolution:chararray,sampled_hit:chararray,search_engine:int,search_page_num:chararray,secondary_hit:chararray,service:chararray,sourceid:chararray,state:chararray,stats_server:chararray,t_time_info:chararray,tnt:chararray,tnt_post_vista:chararray,transactionid:chararray,truncated_hit:chararray,ua_color:chararray,ua_os:chararray,ua_pixels:chararray,user_agent:chararray,user_hash:chararray,user_server:chararray,userid:chararray,username:chararray,va_closer_detail:chararray,va_closer_id:chararray,va_finder_detail:chararray,va_finder_id:chararray,va_instance_event:chararray,va_new_engagement:chararray,visid_high:chararray,visid_low:chararray,visid_new:chararray,visid_timestamp:chararray,visid_type:chararray,visit_keywords:chararray,visit_num:long,visit_page_num:long,visit_referrer:chararray,visit_search_engine:int,visit_start_page_url:chararray,visit_start_pagename:chararray,visit_start_time_gmt:chararray,weekly_visitor:chararray,yearly_visitor:int,zip:chararray";

    private static final String STRING_SCHEMA = STRING_SCHEMA_400;

	private static final int FIELD_COUNT = STRING_SCHEMA.split(",").length;
	
	private TupleFactory tupleFactory = TupleFactory.getInstance();
	private BagFactory bagFactory = BagFactory.getInstance();
	private OmnitureDataFileRecordReader476 reader;
	private String udfcSignature = null;
	private ResourceFieldSchema[] fields;
	
	@Override
	public void setUDFContextSignature(String signature) {
		udfcSignature = signature;
	}

	@Override
	/**
	 * Provide a new OmnitureDataFileInputFormat for RecordReading.
	 * @return a new OmnitureDataFileInputFormat()
	 */
	public InputFormat<LongWritable, Text> getInputFormat() throws IOException {
		return new OmnitureDataFileInputFormat476();
	}
	
	@Override
	/**
	 * Sets the location of the data file for the call to this custom loader.  This is assumed to be an HDFS path
	 * and thus FileInputFormat is used.
	 */
	public void setLocation(String location, Job job) throws IOException {
		FileInputFormat.setInputPaths(job, location);	
	}
	
	@Override
	@SuppressWarnings("rawtypes")
	public void prepareToRead(RecordReader reader, PigSplit split)
			throws IOException {
		// LOG.info("RecordReader is of type " + reader.getClass().getName());
		this.reader = (OmnitureDataFileRecordReader476)reader;
		ResourceSchema schema = new ResourceSchema(Utils.getSchemaFromString(STRING_SCHEMA));
		fields = schema.getFields();
	}
	
	@Override
	public Tuple getNext() throws IOException {
		Tuple tuple = null;
		Text value = null;
		String []values;
		
		try {
			// Read the next key-value pair from the record reader.  If it's
			// finished, return null
			if (!reader.nextKeyValue()) return null;
			
			value = (Text)reader.getCurrentValue();
			values = value.toString().split(DELIMITER, -1);
		} catch (InterruptedException ie) {
			throw new IOException(ie);
		}
				
		// Create a new Tuple optimized for the number of fields that we know we'll need
		tuple = tupleFactory.newTuple(FIELD_COUNT);
		
		for (int i = 0; i < FIELD_COUNT; i++) {			
			// Optimization
			ResourceFieldSchema field = fields[i];
			String val = values[i];
			
			switch(field.getType()) {
			case DataType.INTEGER:
				try {
					tuple.set(i, Integer.parseInt(val));
				} catch (NumberFormatException nfe1) {
					// Throw a more descriptive message
					throw new NumberFormatException("Error while trying to parse " + val + " into an Integer for field " + field.getName() + "\n" + value.toString());
				}
				break;
			case DataType.CHARARRAY:
				tuple.set(i, val);
				break;
			case DataType.LONG:
				try {
					tuple.set(i, Long.parseLong(val));
				} catch (NumberFormatException nfe2) {
					throw new NumberFormatException("Error while trying to parse " + val + " into a Long for field " + field.getName() + "\n" + value.toString());
				}
				
				break;
			case DataType.BAG:
				if (field.getName().equals("event_list")) {
					DataBag bag = bagFactory.newDefaultBag();
					String []events = val.split(",");
					
					if (events == null) {
						tuple.set(i, null);
					} else {
						for (int j = 0; j < events.length; j++) {
							Tuple t = tupleFactory.newTuple(1);
							if (events[j] == "") {
								t.set(0, null);
							} else {
								t.set(0, events[j]);
							}
							bag.add(t);
						}
						tuple.set(i, bag);
					}					
				} else {
					throw new IOException("Can not process bags for the field " + field.getName() + ". Can only process for the event_list field.");
				}
				break;
			default:
				throw new IOException("Unexpected or unknown type in input schema (Omniture fields should be int, chararray or long): " + field.getType());
			}
		}
		
		return tuple;
	}

	@Override
	public ResourceSchema getSchema(String location, Job job) throws IOException {
		// The schema for hit_data.tsv won't change for quite sometime and when it does, this class should be updated
		
		ResourceSchema s = new ResourceSchema(Utils.getSchemaFromString(STRING_SCHEMA));
		
		// Store the schema to our UDF context on the backend (is this really necessary considering it's private static final?)
		UDFContext udfc = UDFContext.getUDFContext();
		Properties p = udfc.getUDFProperties(this.getClass(), new String[]{udfcSignature});
		p.setProperty("pig.omnituretextloader.schema", STRING_SCHEMA);
		
		return s;
	}
	
	@Override
	/** 
	 * Not currently used, but could later on be used to partition based on hit_time_gmt perhaps.
	 */
	public String[] getPartitionKeys(String location, Job job) throws IOException {
		// TODO: Build out partition keys based on hit_time_gmt 
		return null;
	}


	@Override
	/**
	 * Not used in this class.
	 * @return null
	 */
	public ResourceStatistics getStatistics(String location, Job job)
			throws IOException {
		return null;
	}

	@Override
	/** 
	 * Not currently used, but could later on be used to partition based on hit_time_gmt perhaps.
	 */
	public void setPartitionFilter(Expression arg0) throws IOException {
		// TODO: Build out partition keys based on hit_time_gmt
		
	}
}
