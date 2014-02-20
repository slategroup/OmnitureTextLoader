package com.tgam.hadoop.pig;

import com.tgam.hadoop.mapreduce.OmnitureDataFileInputFormat353;
import com.tgam.hadoop.mapreduce.OmnitureDataFileRecordReader353;
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
public class OmnitureTextLoader353 extends OmnitureTextLoader implements LoadMetadata {

    private static final String STRING_SCHEMA = "user_hash:chararray,username:chararray,userid:chararray,service:chararray,hit_time_gmt:long,date_time:chararray,visid_high:chararray,visid_low:chararray,hitid_high:chararray,hitid_low:chararray,accept_language:chararray,browser_height:chararray,browser_width:chararray,c_color:chararray,click_action:chararray,click_action_type:chararray,click_context:chararray,click_context_type:chararray,click_sourceid:chararray,click_tag:chararray,code_ver:chararray,cookies:chararray,ct_connect_type:chararray,campaign:chararray,event_list:chararray,homepage:chararray,ip:chararray,j_jscript:chararray,java_enabled:chararray,p_plugins:chararray,page_event:int,page_event_var1:chararray,page_event_var2:chararray,page_type:chararray,page_url:chararray,pagename:chararray,persistent_cookie:chararray,product_list:chararray,channel:chararray,user_server:chararray,purchaseid:chararray,referrer:chararray,s_resolution:chararray,secondary_hit:chararray,state:chararray,stats_server:chararray,t_time_info:chararray,ua_color:chararray,ua_os:chararray,ua_pixels:chararray,user_agent:chararray,zip:chararray,search_engine:int,exclude_hit:int,hier1:chararray,hier2:chararray,hier3:chararray,hier4:chararray,hier5:chararray,currency:chararray,curr_rate:chararray,curr_factor:chararray,post_browser_height:int,post_browser_width:int,post_cookies:chararray,post_java_enabled:chararray,post_persistent_cookie:chararray,post_t_time_info:chararray,browser:int,color:int,connection_type:int,country:int,domain:chararray,javascript:int,language:int,os:int,plugins:chararray,resolution:int,last_hit_time_gmt:long,first_hit_time_gmt:long,visit_start_time_gmt:long,last_purchase_time_gmt:long,last_purchase_num:long,first_hit_page_url:chararray,first_hit_pagename:chararray,visit_start_page_url:chararray,visit_start_pagename:chararray,first_hit_referrer:chararray,visit_referrer:chararray,visit_search_engine:int,visit_num:long,visit_page_num:long,prev_page:long,geo_city:chararray,geo_country:chararray,geo_region:chararray,duplicate_purchase:int,new_visit:int,daily_visitor:int,hourly_visitor:int,monthly_visitor:int,yearly_visitor:int,geo_dma:chararray,duplicate_events:chararray,weekly_visitor:chararray,quarterly_visitor:chararray,sampled_hit:chararray,truncated_hit:chararray,post_search_engine:chararray,paid_search:chararray,search_page_num:chararray,post_keywords:chararray,product_merchandising:chararray,hit_source:chararray,cust_hit_time_gmt:chararray,post_cust_hit_time_gmt:chararray,sourceid:chararray,ip2:chararray,cust_visid:chararray,transactionid:chararray,page_event_var3:chararray,mobile_id:chararray,va_closer_detail:chararray,va_finder_detail:chararray,va_finder_id:chararray,va_closer_id:chararray,prop1:chararray,prop2:chararray,prop3:chararray,prop4:chararray,prop5:chararray,prop6:chararray,prop7:chararray,prop8:chararray,prop9:chararray,prop10:chararray,prop11:chararray,prop12:chararray,prop13:chararray,prop14:chararray,prop15:chararray,prop16:chararray,prop17:chararray,prop18:chararray,prop19:chararray,prop20:chararray,prop21:chararray,prop22:chararray,prop23:chararray,prop24:chararray,prop25:chararray,prop26:chararray,prop27:chararray,prop28:chararray,prop29:chararray,prop30:chararray,prop31:chararray,prop32:chararray,prop33:chararray,prop34:chararray,prop35:chararray,prop36:chararray,prop37:chararray,prop38:chararray,prop39:chararray,prop40:chararray,prop41:chararray,prop42:chararray,prop43:chararray,prop44:chararray,prop45:chararray,prop46:chararray,prop47:chararray,prop48:chararray,prop49:chararray,prop50:chararray,prop51:chararray,prop52:chararray,prop53:chararray,prop54:chararray,prop55:chararray,prop56:chararray,prop57:chararray,prop58:chararray,prop59:chararray,prop60:chararray,prop61:chararray,prop62:chararray,prop63:chararray,prop64:chararray,prop65:chararray,prop66:chararray,prop67:chararray,prop68:chararray,prop69:chararray,prop70:chararray,prop71:chararray,prop72:chararray,prop73:chararray,prop74:chararray,prop75:chararray,evar1:chararray,evar2:chararray,evar3:chararray,evar4:chararray,evar5:chararray,evar6:chararray,evar7:chararray,evar8:chararray,evar9:chararray,evar10:chararray,evar11:chararray,evar12:chararray,evar13:chararray,evar14:chararray,evar15:chararray,evar16:chararray,evar17:chararray,evar18:chararray,evar19:chararray,evar20:chararray,evar21:chararray,evar22:chararray,evar23:chararray,evar24:chararray,evar25:chararray,evar26:chararray,evar27:chararray,evar28:chararray,evar29:chararray,evar30:chararray,evar31:chararray,evar32:chararray,evar33:chararray,evar34:chararray,evar35:chararray,evar36:chararray,evar37:chararray,evar38:chararray,evar39:chararray,evar40:chararray,evar41:chararray,evar42:chararray,evar43:chararray,evar44:chararray,evar45:chararray,evar46:chararray,evar47:chararray,evar48:chararray,evar49:chararray,evar50:chararray,evar51:chararray,evar52:chararray,evar53:chararray,evar54:chararray,evar55:chararray,evar56:chararray,evar57:chararray,evar58:chararray,evar59:chararray,evar60:chararray,evar61:chararray,evar62:chararray,evar63:chararray,evar64:chararray,evar65:chararray,evar66:chararray,evar67:chararray,evar68:chararray,evar69:chararray,evar70:chararray,evar71:chararray,evar72:chararray,evar73:chararray,evar74:chararray,evar75:chararray,post_campaign:chararray,post_evar1:chararray,post_evar2:chararray,post_evar3:chararray,post_evar4:chararray,post_evar5:chararray,post_evar6:chararray,post_evar7:chararray,post_evar8:chararray,post_evar9:chararray,post_evar10:chararray,post_evar11:chararray,post_evar12:chararray,post_evar13:chararray,post_evar14:chararray,post_evar15:chararray,post_evar16:chararray,post_evar17:chararray,post_evar18:chararray,post_evar19:chararray,post_evar20:chararray,post_evar21:chararray,post_evar22:chararray,post_evar23:chararray,post_evar24:chararray,post_evar25:chararray,post_evar26:chararray,post_evar27:chararray,post_evar28:chararray,post_evar29:chararray,post_evar30:chararray,post_evar31:chararray,post_evar32:chararray,post_evar33:chararray,post_evar34:chararray,post_evar35:chararray,post_evar36:chararray,post_evar37:chararray,post_evar38:chararray,post_evar39:chararray,post_evar40:chararray,post_evar41:chararray,post_evar42:chararray,post_evar43:chararray,post_evar44:chararray,post_evar45:chararray,post_evar46:chararray,post_evar47:chararray,post_evar48:chararray,post_evar49:chararray,post_evar50:chararray,post_evar51:chararray,post_evar52:chararray,post_evar53:chararray,post_evar54:chararray,post_evar55:chararray,post_evar56:chararray,post_evar57:chararray,post_evar58:chararray,post_evar59:chararray,post_evar60:chararray,post_evar61:chararray,post_evar62:chararray,post_evar63:chararray,post_evar64:chararray,post_evar65:chararray,post_evar66:chararray,post_evar67:chararray,post_evar68:chararray,post_evar69:chararray,post_evar70:chararray,post_evar71:chararray,post_evar72:chararray,post_evar73:chararray,post_evar74:chararray,post_evar75:chararray,duplicated_from:chararray";

    public OmnitureTextLoader353() {
        super(STRING_SCHEMA);
    }

    @Override
    /**
     * Provide a new OmnitureDataFileInputFormat for RecordReading.
     * @return a new OmnitureDataFileInputFormat()
     */
    public InputFormat<LongWritable, Text> getInputFormat() throws IOException {
        return new OmnitureDataFileInputFormat353();
    }

    @Override
    @SuppressWarnings("rawtypes")
    public void prepareToRead(RecordReader reader, PigSplit split)
            throws IOException {
        // LOG.info("RecordReader is of type " + reader.getClass().getName());
        this.reader = (OmnitureDataFileRecordReader353)reader;
        ResourceSchema schema = new ResourceSchema(Utils.getSchemaFromString(STRING_SCHEMA));
        fields = schema.getFields();
    }

}
