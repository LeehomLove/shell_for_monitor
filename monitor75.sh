#set -x
#!/bin/sh -x
#дһ���ű���ÿһ������һ���ļ����ļ���ʽ˵�����ű���������
#�ļ����ƣ�����������.���ݿ����ơ�
#�ļ����ݣ�
#	1�����±�/2���ձ�/3�����³���/4�������һ��/5�����³���/6day��(DD)/
#	7������/8����/9�����±�/10��������/11����/12����/13���µ�һ��/14���µڶ���
#  �����ı����ƣ�"1�����±�/2���ձ�/3�����³���/4�������һ��/5�����³���/6day��(DD)/
#				  7������/8����/9�����±�/10��������/11����/12����/13���µ�һ��/14���µڶ��족  ��ʱ����
#�����ʽ  ������/�ձ��±�/��������/����ԭ��/��ʼʱ��/���ʱ��(û�����\t)
#db2info.proc_com
 
#����˼·��ÿ�춨ʱȥ�����ݣ�ͨ���ļ��������鿴��
#1.ͨ���ṩ���ļ�������ɨ�裬�鿴����ĳ���ִ�б�����ɹ�����ִ����һ��
#2.����Ҳ�������ȥ��־Ŀ¼�����ļ������Ƿ����ļ���������ļ�����ֻ��5�����£���ʼѭ���ж������ı��жϴ��ڣ�����Ϊ��
#	 ����ļ�Ҳ�Ҳ������϶�������û�л��ѣ���ʾ���鿴ǰ������
#3.���ϵͳ������ʾΪִ�д�����ȥ�����ļ���ץ��"warning report" �ѱ�����Ϣ���

#
#monitor.sh
. /homeex/wangyang/.profile
export PATH=/homeex/wangyang/monitor/:$PATH
#  db2 connect reset
# db2 connect to shcrm2 user db2info using infodb21
  
  
proc_path="/homeex/wangyang/monitor/src"
db_info="75"
log_path="/homeex/wangyang/monitor/log"
result_path="/homeex/wangyang/monitor/result" 

#��׼����ʱ�����
#����
v_time=`date +'%Y%m%d'`
#����
v_yes_d=`gettime ${v_time}0000 day 1|cut -c1-8`
#����
v_tom_d=`gettime ${v_time}0000 day -1|cut -c1-8`
#����
v_tom2_d=`gettime ${v_time}0000 day -2|cut -c1-8`
#���µ�һ��
v_mon_f_d="`echo ${v_time}0000|cut -c1-6`01"
#����
v_last_m=`gettime ${v_time}0000 month 1|cut -c1-6`
#�������һ��
v_last_mon_la_d=`gettime ${v_mon_f_d}0000 day -1|cut -c1-8`
#���µ�һ��
v_last_mon_f_d="${v_last_m}01"
#day��
d_time=`date +'%d'`
#����
v_month=`date +'%Y%m'`
#������
v_pre_m=`gettime ${v_time}0000 month 2|cut -c1-6`
#��������
v_pre2_m=`gettime ${v_time}0000 month 3|cut -c1-6`
#����
v_next_m=`gettime ${v_time}0000 month -1|cut -c1-6`
#���µ�һ��
v_next_f_d="`echo ${v_next_m}0000|cut -c1-6`01"
#���µڶ���
v_next_s_d="`echo ${v_next_m}0000|cut -c1-6`02"
#�������һ��
v_last_mon_la_d=`gettime ${v_next_f_d}0000 day 1|cut -c1-8`

#����ʱ��
exe_time="${v_yes_d}"

conn_db()
{
	if [ $1 -eq 77 ]
	then
	db2 connect to crmnew77 user aimdata using aimd0717
	else
	db2 connect to shcrm3 user db2info using shdb0403
	fi	
}

ftp_get()
{
ftp -n <<eof
open 10.9.144.84
user db2info yxwl0210
prompt off
cd /logfile/log/trace/odsst/${1}
lcd ${3}
get ${2}
eof
}

check_table()
{
	v_count=`db2 -x "select count(*) from syscat.tables where tabschema='${3}' and tabname=upper('${1}'||'${2}') with ur"`
	if [ ${v_count} -gt 0 ]
	then
	v_count = 0;
	v_count=`db2 -x "select count(*) from ${3}.${1}${2} with ur"`
	 if [ v_count -gt 0 ]
		then
		continue;
		else
		err_str="${proc_name}\t${4}\t��Ϊ��\t${3}.${1}${2} is empty\t\t"
	 fi
	else
		err_str="${proc_name}\t${4}\t������\t${3}.${1}${2} is not exist\t\t"
	fi
	echo ${err_str} >> ${result_path}/${exe_time}.csv
}

check()
{
#$1,proc_name
#$2 proc_file
#$3 data_time
#proc_name=`echo ${1}|awk -F'_' '{print $1}'`
#db_info=`echo ${1}|awk -F'_' '{print $2}'`
	v_cont=`db2 -x "select count(*) from db2info.proc_com where proc_name='${1}' and op_time='${3}'  and status = 3`
	v_start_time=`db2 -x "select START_TIME from db2info.proc_com where proc_name='${1}' and op_time='${3}'  and status = 3`
	v_end_time=`db2 -x "select END_TIME from db2info.proc_com where proc_name='${1}' and op_time='${3}'  and status = 3`	
	if [ ${v_cont} -eq 0 ]
	then
		#�鿴�Ƿ����ļ�����
		#rm ${log_path}/${1}.trace
		ftp_get ${exe_time} "${1}.trace" ${log_path}
		if [ -f ${log_path}/"${1}.trace"	]
		then
		if [ `grep -c "δ׼���ýӿ�" ${log_path}/${1}.trace`  -gt 0  ]
			then
			 etl_id=`grep "δ׼���ýӿ�"|awk -F':' '{print substr($3,2)}'|awk -F',' '{print $1}'`
			 etl_t=`grep "δ׼���ýӿ�"|awk -F':' '{print substr($3,2)}'|awk -F',' '{print $2}'`
			 tab_name=`db2 -x "SELECT  
			 distinct a.DEST_SCHEMA||'.'|| (case when  a.DEST_TABLE like '%$%' then substr(a.DEST_TABLE,1,locate('$',a.DEST_TABLE)-1)||'${etl_t}'
											else a.DEST_TABLE||'${etl_t}' end)
				FROM	  DB2INFO.ETL_LOAD_CFG a
				JOIN DB2INFO.ETL_INTER_CFG b
				ON a.INTER_CODE = b.INTER_CODE
				AND a.CYCLE_TYPE = b.CYCLE_TYPE
				WHERE  b.IS_VALID = 1  and a.INTER_CODE='${etl_id}'' 
				FETCH FIRST 1 ROWS ONLY WITH UR"`
				#������Ϣ
			echo "${proc_name}\t${4}\t����ӿ�\t${tab_name}δ׼���ýӿ�\t\t" >> ${result_path}/${exe_time}.csv
		elif [	`wc -l	${log_path}/"${1}.trace"|awk {'print $1'}` -le 5 ]
			then
			##���ԭ��
			for src_tab in `cat ${proc_path}/${2}`
			do
			tab=`echo ${src_tab}|awk -F ',' '{print $1}'`
			tab_schema=`echo ${src_tab}|awk -F '.' '{print $1}'`
			time_flag=`echo ${src_tab}|awk -F ',' '{print $2}'`
			#ѭ������check_table()
			#1�����±�/2���ձ�/3�����³���/4�������һ��/5�����³���/6day��(DD)/
			#7������/8����/9�����±�/10��������/11����/12����/13���µ�һ��/14���µڶ���
			case ${time_flag} in 
			1)
			#time_new ='1���±�'
			check_table ${tab}	${v_last_m}	 ${tab_schema}	${4}
			;;
			2)
			#time_new ='2���ձ�'
			check_table ${tab}	${v_time}	${tab_schema}	${4}
			;;
			3)
			#time_new ='3�����³���'
			check_table ${tab}	${v_mon_f_d}	${tab_schema}	${4}
			;;
			4)
			#time_new ='4�������һ��'
			check_table ${tab}	${v_last_mon_la_d}	${tab_schema}	${4}
			;;
			5)
			#time_new ='5�����³���'
			check_table ${tab}	${v_last_mon_f_d}	${tab_schema}	${4}
			;;
			6)
			#time_new ='6�ձ�(DD)'
			check_table ${tab}	${d_time}	${tab_schema}	${4}
			;;
			7)
			#time_new ='7������'
			check_table ${tab}	${v_pre_m}	${tab_schema}	${4}
			;;
			8)
			#time_new ='8����'
			check_table ${tab}	${v_yes_d}	${tab_schema}	${4}
			;;
			9)
			#time_new ='9�����±�'
			check_table ${tab}	${v_month}	${tab_schema}	${4}
			;;	
			10)
			#time_new ='10��������'
			check_table ${tab}	${v_pre2_m}	${tab_schema}	${4}
			;;	
			11)
			#time_new ='11����'
			check_table ${tab}	${v_tom_d}	${tab_schema}	${4}
			;;	
			12)
			#time_new ='12����'
			check_table ${tab}	${v_tom2_d}	${tab_schema}	${4}
			;;	
			13)
			#time_new ='13���µ�һ��'
			check_table ${tab}	${v_next_f_d}	${tab_schema}	${4}
			;;	
			14)
			#time_new ='14���µڶ���'
			check_table ${tab}	${v_next_s_d}	${tab_schema}	${4}
			;;	
			esac
			done
		 ##�鿴����״̬�����������У�����  
		elif [ `grep -c 'warning report' ${log_path}/"${1}.trace"` -gt 0 ]
				then
					err_str=`cat ${log_path}/"${1}.trace"|sed -n "/warning report/,/end warning report/p"|sed -n '2p'`
					echo "${proc_name}\t${4}\t���򱨴�\t${err_str}\t\t" >> ${result_path}/${exe_time}.csv
		fi		 
		else	   
			##����û�е�����������Ҫ�鿴ǰ������
			echo "${proc_name}\t${4}\t��������\t����û�е���!\t\t" >> ${result_path}/${exe_time}.csv
		fi	
	else		 
		echo "${proc_name}\t${4}\t���гɹ�\t\t${v_start_time}\t${v_end_time}" >> ${result_path}/${exe_time}.csv
	fi
   
}


#ɨ��Ŀ¼
conn_db	${db_info}
for proc_file in `ls ${proc_path}`
do
		proc_name=`echo ${proc_file}|awk -F'.' '{print $1}'`
		#db_info=`echo ${proc_file}|awk -F'.' '{print $2}'`
		date_flag=`echo ${proc_file}|awk -F'.' '{print $3}'`
		#conn_db	${db_info}
		if [ "${date_flag}" = "��" ]
			then
			check ${proc_name} ${proc_file} ${exe_time}	 ${date_flag}
		else
			check ${proc_name} ${proc_file} ${v_month}	${date_flag}
		fi	
		echo "finish"
done   

