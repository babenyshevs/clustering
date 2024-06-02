----------provide user inupt to Stanislav

drop view tmpa.event_user_input_sample_base_v

create view tmpa.event_user_input_sample_base_v as

 SELECT a.session_id,
    a.event_id,
    a.created_ux,
    a.created_dt,
	a.user_input,
        CASE
            WHEN a.channel_init IS NOT NULL THEN a.channel_init
            WHEN a.channel_orchestrtor IS NOT NULL THEN a.channel_orchestrtor
            WHEN a.channel_frontend IS NOT NULL THEN a.channel_frontend
            ELSE NULL::text
        END AS channel,
	a.top_intent_1,
	a.top_intent_2,
	a.top_intent_3,
	a.top_intent_4,
	a.top_intent_5,
    a.issuer,
    a.content_version,
    a.command,
    a.input_mode,
        CASE
            WHEN a.customer_segment IS NOT NULL THEN a.customer_segment
            WHEN a.customer_segment_init IS NOT NULL THEN a.customer_segment_init
            ELSE NULL::text
        END AS customer_segment,
    a.action_nlu_fallback_flg,
    a.action_core_fallback_flg,
    a.auth_level,
    a.pg_ingest_id,
    a.pg_process_id
   FROM ( SELECT s.session_id,
            s.event_id,
            s.created_ux,
            s.created_dt,
		     s.request_jsn -> 'report'   -> 'report-data'  -> 'input' ->> 'text' as user_input,
            ((s.request_jsn -> 'report'::text) -> 'report-data'::text) ->> 'channel'::text AS channel_orchestrtor,
            (((((s.request_jsn -> 'report'::text) -> 'report-data'::text) -> 'input'::text) -> 'json'::text) -> 'initialCtx'::text) ->> 'channel'::text AS channel_init,
            (((s.request_jsn -> 'report'::text) -> 'report-data'::text) -> 'frontend'::text) ->> 'channelType'::text AS channel_frontend,
            (s.request_jsn -> 'report'::text) ->> 'report-source'::text AS report_source,
            (s.request_jsn -> 'report'::text) ->> 'report-type'::text AS report_type,
            (((s.request_jsn -> 'report'::text) -> 'report-data'::text) -> 'meta'::text) ->> 'issuer'::text AS issuer,
            (((s.request_jsn -> 'report'::text) -> 'report-data'::text) -> 'meta'::text) ->> 'contentVersion'::text AS content_version,
            (((s.request_jsn -> 'report'::text) -> 'report-data'::text) -> 'input'::text) ->> 'command'::text AS command,
            (((s.request_jsn -> 'report'::text) -> 'report-data'::text) -> 'input'::text) ->> 'inputMode'::text AS input_mode,
            ((s.request_jsn -> 'report'::text) -> 'report-data'::text) ->> 'customerSegment'::text AS customer_segment,
            (((((s.request_jsn -> 'report'::text) -> 'report-data'::text) -> 'input'::text) -> 'json'::text) -> 'initialCtx'::text) ->> 'customerSegment'::text AS customer_segment_init,

                CASE
                    WHEN POSITION(('action_nlu_fallback'::text) IN ((((s.request_jsn -> 'report'::text) -> 'report-data'::text) -> 'debug'::text) ->> 'actions'::text)) > 0 THEN 1
                    ELSE 0
                END AS action_nlu_fallback_flg,
                CASE
                    WHEN POSITION(('action_core_fallback'::text) IN ((((s.request_jsn -> 'report'::text) -> 'report-data'::text) -> 'debug'::text) ->> 'actions'::text)) > 0 THEN 1
                    ELSE 0
                END AS action_core_fallback_flg,
            ((((((((s.request_jsn -> 'report'::text) -> 'report-data'::text) -> 'input'::text) -> 'json'::text) -> 'inputCtx'::text) -> 'userCtx'::text) -> 'auth'::text) -> 'authLevel'::text)::integer AS auth_level,
            s.request_jsn -> 'report'  -> 'report-data'  -> 'debug' ->> 'topIntent1' as top_intent_1,
            s.request_jsn -> 'report'  -> 'report-data'  -> 'debug' ->> 'topIntent2' as top_intent_2,
		    s.request_jsn -> 'report'  -> 'report-data'  -> 'debug' ->> 'topIntent2' as top_intent_3,
		    s.request_jsn -> 'report'  -> 'report-data'  -> 'debug' ->> 'topIntent2' as top_intent_4,
		    s.request_jsn -> 'report'  -> 'report-data'  -> 'debug' ->> 'topIntent2' as top_intent_5,
		    s.pg_ingest_id,
            s.pg_process_id
         FROM il.poc_il_event_stg s
		 where   (s.request_jsn -> 'report'::text) ->> 'report-type' = 'dialog-transaction'
		         AND  s.request_jsn -> 'report'   -> 'report-data'  -> 'input' -> 'text' is not null		
		) a  
 
	
	
--5-11.2 CW6 --75.632

select *
	from tmpa.event_user_input_sample_base_v 
	where created_dt between '2024-02-05' and '2024-02-11'
	      and channel  = 'web'
	      and issuer = 'RASA'  and  command = 'message'		
		  
		  
--8.4-14.4 CW15 --67.584

select *
	from tmpa.event_user_input_sample_base_v 
	where created_dt between '2024-04-08' and '2024-04-14'
	      and channel  = 'web'
	      and issuer = 'RASA'  and  command = 'message'	
		  
		  
--6.5-12.5 CW19 --55.934

select *
	from tmpa.event_user_input_sample_base_v 
	where created_dt between '2024-05-06' and '2024-05-12'
	      and channel  = 'web'
	      and issuer = 'RASA'  and  command = 'message'				
		
		


