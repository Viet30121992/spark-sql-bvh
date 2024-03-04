CREATE SCHEMA bvh AUTHORIZATION airflow;

CREATE TABLE bvh.data_test (
	id int4 NULL,
	first_name varchar NULL,
	last_name varchar NULL,
	email varchar NULL,
	gender varchar NULL,
	job varchar NULL
);


CREATE TABLE bvh.data_test_2 (
	id int4 NULL,
	first_name varchar NULL,
	last_name varchar NULL,
	email varchar NULL,
	gender varchar NULL,
	job varchar NULL
);


CREATE TABLE bvh.data_test_3 (
	id int4 NULL,
	first_name varchar NULL,
	last_name varchar NULL,
	email varchar NULL,
	gender varchar NULL,
	job varchar NULL
);

CREATE TABLE bvh.spark_log_job (
	id varchar(200) NOT NULL, -- gia tri event id duoc truyen vao tu airflow
	taskid int8 NULL,
	taskname varchar(500) NULL,
	starttime varchar(20) NULL,
	endtime varchar(20) NULL,
	duration float8 NULL,
	loglevel varchar(20) NULL, -- ERROR, INFO, WARNING
	logmessage varchar NULL,
	statustask varchar(20) NULL, -- SUCCESS or FAILED
	resultcount int8 NULL,
	jobid int8 NULL,
	insert_time timestamp DEFAULT now() NULL
);
COMMENT ON TABLE bvh.spark_log_job IS 'thong tin chay cua cac task';

-- Column comments

COMMENT ON COLUMN bvh.spark_log_job.id IS 'gia tri event id duoc truyen vao tu airflow';
COMMENT ON COLUMN bvh.spark_log_job.loglevel IS 'ERROR, INFO, WARNING';
COMMENT ON COLUMN bvh.spark_log_job.statustask IS 'SUCCESS or FAILED';

CREATE TABLE bvh.spark_source_config (
	source_name varchar NULL,
	source_type varchar NULL,
	source_value varchar NULL,
	insert_time timestamp DEFAULT CURRENT_DATE NULL,
	update_time timestamp NULL,
	id int4 GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE) NOT NULL,
	CONSTRAINT spark_source_config_pk PRIMARY KEY (id)
);

CREATE TABLE bvh.spark_task_template (
	id int8 GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1 NO CYCLE) NOT NULL,
	job_id int8 NULL,
	task_id int8 NULL,
	task_name varchar(1000) NULL,
	data_source_name varchar(1000) NULL,
	data_des_name varchar(1000) NULL,
	query text NULL,
	type_task varchar(10) NULL,
	type_source varchar(20) NULL,
	type_destination varchar(20) NULL,
	task_order int4 NULL,
	list_option_source varchar NULL, -- thong tin cau hinh luc doc
	mode_save varchar(20) NULL,
	spark_view_type varchar(20) NULL,
	spark_tempview_name varchar(100) NULL,
	task_level int4 DEFAULT 1 NOT NULL,
	task_group varchar(2) DEFAULT 'N'::character varying NULL,
	task_id_parent int8 NULL,
	insert_time timestamp DEFAULT now() NULL,
	status int4 DEFAULT 1 NULL, -- trang thai cua task, chi thuc thi cac task trang thai 1
	list_option_des varchar NULL, -- thong tin cau hinh them luc luu
	CONSTRAINT spark_task_template_pk PRIMARY KEY (id)
);

-- Column comments

COMMENT ON COLUMN bvh.spark_task_template.list_option_source IS 'thong tin cau hinh luc doc';
COMMENT ON COLUMN bvh.spark_task_template.status IS 'trang thai cua task, chi thuc thi cac task trang thai 1';
COMMENT ON COLUMN bvh.spark_task_template.list_option_des IS 'thong tin cau hinh them luc luu';