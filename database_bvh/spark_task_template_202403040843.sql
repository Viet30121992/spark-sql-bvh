INSERT INTO bvh.spark_task_template (job_id,task_id,task_name,data_source_name,data_des_name,query,type_task,type_source,type_destination,task_order,list_option_source,mode_save,spark_view_type,spark_tempview_name,task_level,task_group,task_id_parent,insert_time,status,list_option_des) VALUES
	 (1,1,'t1','1',NULL,'select * from bvh.data_test','E','postgres',NULL,1,NULL,NULL,'tempview','',1,'Y',NULL,'2024-02-27 13:39:15.544587',1,NULL),
	 (1,3,'t3','1',NULL,'select * from bvh.data_test','E','postgres',NULL,1,NULL,NULL,'tempview','test3',1,'N',NULL,'2024-02-27 15:50:23.026392',1,NULL),
	 (1,2,'t2','1',NULL,'select * from bvh.data_test','E','postgres',NULL,2,NULL,NULL,'tempview','test2',1,'N',NULL,'2024-02-27 13:40:23.750662',1,NULL),
	 (1,7,'t4','1',NULL,'select * from test2
union all
select * from test3
union all
select * from test1_1
union all
select * from test1_2
union all
select * from test1_3','T','sparksql',NULL,3,NULL,NULL,'tempview','result_test1',1,'N',NULL,'2024-02-29 13:26:21.987609',1,NULL),
	 (1,4,'t1_1','1',NULL,'select * from bvh.data_test','E','postgres',NULL,1,NULL,NULL,'tempview','test1_1',2,'N',1,'2024-02-29 10:21:24.998688',1,NULL),
	 (1,5,'t1_2','1',NULL,'select * from bvh.data_test','E','postgres',NULL,1,NULL,NULL,'tempview','test1_2',2,'N',1,'2024-02-29 10:22:21.524193',1,NULL),
	 (1,6,'t1_3','1',NULL,'select * from bvh.data_test','E','postgres',NULL,2,NULL,NULL,'tempview','test1_3',2,'N',1,'2024-02-29 10:46:53.873173',1,NULL),
	 (1,8,'t5','1','1','select * from result_test1','L','sparksql','postgres',4,NULL,'append',NULL,NULL,1,'N',NULL,'2024-02-29 13:56:13.824971',1,'dbtable:bvh.data_test_2'),
	 (1,9,'t6','1','1','select * from bvh.data_test','L','postgres','postgres',4,NULL,'append',NULL,NULL,1,'N',NULL,'2024-02-29 13:58:17.469108',1,'dbtable:bvh.data_test_3');
