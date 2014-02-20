package com.tgam.hadoop.pig;

import com.tgam.hadoop.mapreduce.OmnitureDataFileInputFormat227;
import com.tgam.hadoop.mapreduce.OmnitureDataFileRecordReader227;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.LoadMetadata;
import org.apache.pig.ResourceSchema;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.impl.util.Utils;

import java.io.IOException;

/**
 * A Pig custom loader for reading and parsing raw Omniture daily hit data files (hit_data.tsv).
 * @author Mike Sukmanowsky (<a href="mailto:mike.sukmanowsky@gmail.com">mike.sukmanowsky@gmail.com</a>)
 */
public class OmnitureTextLoader227 extends OmnitureTextLoader implements LoadMetadata {

	private static final String STRING_SCHEMA = "hit_time_gmt:long,service:chararray,accept_language:chararray,date_time:chararray,visid_high:chararray,visid_low:chararray,event_list:chararray,homepage:chararray,ip:chararray,page_event:int,page_event_var1:chararray,page_event_var2:chararray,page_type:chararray,page_url:chararray,pagename:chararray,product_list:chararray,user_server:chararray,channel:chararray,prop1:chararray,prop2:chararray,prop3:chararray,prop4:chararray,prop5:chararray,prop6:chararray,prop7:chararray,prop8:chararray,prop9:chararray,prop10:chararray,prop11:chararray,prop12:chararray,prop13:chararray,prop14:chararray,prop15:chararray,prop16:chararray,prop17:chararray,prop18:chararray,prop19:chararray,prop20:chararray,prop21:chararray,prop22:chararray,prop23:chararray,prop24:chararray,prop25:chararray,prop26:chararray,prop27:chararray,prop28:chararray,prop29:chararray,prop30:chararray,prop31:chararray,prop32:chararray,prop33:chararray,prop34:chararray,prop35:chararray,prop36:chararray,prop37:chararray,prop38:chararray,prop39:chararray,prop40:chararray,prop41:chararray,prop42:chararray,prop43:chararray,prop44:chararray,prop45:chararray,prop46:chararray,prop47:chararray,prop48:chararray,prop49:chararray,prop50:chararray,purchaseid:chararray,referrer:chararray,state:chararray,user_agent:chararray,zip:chararray,search_engine:int,exclude_hit:int,hier1:chararray,hier2:chararray,hier3:chararray,hier4:chararray,hier5:chararray,browser:int,post_browser_height:int,post_browser_width:int,post_cookies:chararray,post_java_enabled:chararray,post_persistent_cookie:chararray,color:int,connection_type:int,country:int,domain:chararray,post_t_time_info:chararray,javascript:int,language:int,os:int,plugins:chararray,resolution:int,last_hit_time_gmt:long,first_hit_time_gmt:long,visit_start_time_gmt:long,last_purchase_time_gmt:long,last_purchase_num:long,first_hit_page_url:chararray,first_hit_pagename:chararray,visit_start_page_url:chararray,visit_start_pagename:chararray,first_hit_referrer:chararray,visit_referrer:chararray,visit_search_engine:int,visit_num:long,visit_page_num:long,prev_page:long,geo_city:chararray,geo_country:chararray,geo_region:chararray,duplicate_purchase:int,new_visit:int,daily_visitor:int,hourly_visitor:int,monthly_visitor:int,yearly_visitor:int,post_campaign:chararray,evar1:chararray,evar2:chararray,evar3:chararray,evar4:chararray,evar5:chararray,evar6:chararray,evar7:chararray,evar8:chararray,evar9:chararray,evar10:chararray,evar11:chararray,evar12:chararray,evar13:chararray,evar14:chararray,evar15:chararray,evar16:chararray,evar17:chararray,evar18:chararray,evar19:chararray,evar20:chararray,evar21:chararray,evar22:chararray,evar23:chararray,evar24:chararray,evar25:chararray,evar26:chararray,evar27:chararray,evar28:chararray,evar29:chararray,evar30:chararray,evar31:chararray,evar32:chararray,evar33:chararray,evar34:chararray,evar35:chararray,evar36:chararray,evar37:chararray,evar38:chararray,evar39:chararray,evar40:chararray,evar41:chararray,evar42:chararray,evar43:chararray,evar44:chararray,evar45:chararray,evar46:chararray,evar47:chararray,evar48:chararray,evar49:chararray,evar50:chararray,post_evar1:chararray,post_evar2:chararray,post_evar3:chararray,post_evar4:chararray,post_evar5:chararray,post_evar6:chararray,post_evar7:chararray,post_evar8:chararray,post_evar9:chararray,post_evar10:chararray,post_evar11:chararray,post_evar12:chararray,post_evar13:chararray,post_evar14:chararray,post_evar15:chararray,post_evar16:chararray,post_evar17:chararray,post_evar18:chararray,post_evar19:chararray,post_evar20:chararray,post_evar21:chararray,post_evar22:chararray,post_evar23:chararray,post_evar24:chararray,post_evar25:chararray,post_evar26:chararray,post_evar27:chararray,post_evar28:chararray,post_evar29:chararray,post_evar30:chararray,post_evar31:chararray,post_evar32:chararray,post_evar33:chararray,post_evar34:chararray,post_evar35:chararray,post_evar36:chararray,post_evar37:chararray,post_evar38:chararray,post_evar39:chararray,post_evar40:chararray,post_evar41:chararray,post_evar42:chararray,post_evar43:chararray,post_evar44:chararray,post_evar45:chararray,post_evar46:chararray,post_evar47:chararray,post_evar48:chararray,post_evar49:chararray,post_evar50:chararray,click_action:chararray,click_action_type:chararray,click_context:chararray,click_context_type:chararray,click_sourceid:chararray,click_tag:chararray";

    public OmnitureTextLoader227() {
        super(STRING_SCHEMA);
    }

	@Override
	/**
	 * Provide a new OmnitureDataFileInputFormat for RecordReading.
	 * @return a new OmnitureDataFileInputFormat()
	 */
	public InputFormat<LongWritable, Text> getInputFormat() throws IOException {
		return new OmnitureDataFileInputFormat227();
	}
	
	@Override
	@SuppressWarnings("rawtypes")
	public void prepareToRead(RecordReader reader, PigSplit split)
			throws IOException {
		// LOG.info("RecordReader is of type " + reader.getClass().getName());
		this.reader = (OmnitureDataFileRecordReader227)reader;
		ResourceSchema schema = new ResourceSchema(Utils.getSchemaFromString(STRING_SCHEMA));
		fields = schema.getFields();
	}

}
