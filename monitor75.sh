#set -x
#!/bin/sh -x
#写一个脚本，每一个程序一个文件，文件格式说明，脚本区分日月
#文件名称，“程序名称.数据库名称”
#文件内容，
#	1上月月表/2当日表/3本月月初表/4当月最后一天/5上月月初表/6day表(DD)/
#	7上上月/8昨天/9当月月表/10上上上月/11明天/12后天/13下月第一天/14下月第二天
#  依赖的表名称，"1上月月表/2当日表/3本月月初表/4当月最后一天/5上月月初表/6day表(DD)/
#				  7上上月/8昨天/9当月月表/10上上上月/11明天/12后天/13下月第一天/14下月第二天”  延时周期
#输出格式  程序名/日表月表/出错类型/出错原因/开始时间/完成时间(没有填空\t)
#db2info.proc_com
 
#建设思路，每天定时去跑数据，通过文件名称来查看，
#1.通过提供的文件，进行扫描，查看当天的程序执行表，如果成功，则不执行下一步
#2.如果找不到，则去日志目录查找文件，看是否有文件，如果有文件，且只有5行以下，则开始循环判断依赖的表，判断存在，并不为空
#	 如果文件也找不到，肯定是依赖没有唤醒，提示，查看前置依赖
#3.如果系统表中显示为执行错误，则去查找文件，抓到"warning report" 把报错信息输出

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

#先准备好时间参数
#当天
v_time=`date +'%Y%m%d'`
#昨天
v_yes_d=`gettime ${v_time}0000 day 1|cut -c1-8`
#明天
v_tom_d=`gettime ${v_time}0000 day -1|cut -c1-8`
#后天
v_tom2_d=`gettime ${v_time}0000 day -2|cut -c1-8`
#当月第一天
v_mon_f_d="`echo ${v_time}0000|cut -c1-6`01"
#上月
v_last_m=`gettime ${v_time}0000 month 1|cut -c1-6`
#当月最后一天
v_last_mon_la_d=`gettime ${v_mon_f_d}0000 day -1|cut -c1-8`
#上月第一天
v_last_mon_f_d="${v_last_m}01"
#day表
d_time=`date +'%d'`
#当月
v_month=`date +'%Y%m'`
#上上月
v_pre_m=`gettime ${v_time}0000 month 2|cut -c1-6`
#上上上月
v_pre2_m=`gettime ${v_time}0000 month 3|cut -c1-6`
#下月
v_next_m=`gettime ${v_time}0000 month -1|cut -c1-6`
#下月第一天
v_next_f_d="`echo ${v_next_m}0000|cut -c1-6`01"
#下月第二天
v_next_s_d="`echo ${v_next_m}0000|cut -c1-6`02"
#当月最后一天
v_last_mon_la_d=`gettime ${v_next_f_d}0000 day 1|cut -c1-8`

#调度时间
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
		err_str="${proc_name}\t${4}\t表为空\t${3}.${1}${2} is empty\t\t"
	 fi
	else
		err_str="${proc_name}\t${4}\t表不存在\t${3}.${1}${2} is not exist\t\t"
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
		#查看是否有文件存在
		#rm ${log_path}/${1}.trace
		ftp_get ${exe_time} "${1}.trace" ${log_path}
		if [ -f ${log_path}/"${1}.trace"	]
		then
		if [ `grep -c "未准备好接口" ${log_path}/${1}.trace`  -gt 0  ]
			then
			 etl_id=`grep "未准备好接口"|awk -F':' '{print substr($3,2)}'|awk -F',' '{print $1}'`
			 etl_t=`grep "未准备好接口"|awk -F':' '{print substr($3,2)}'|awk -F',' '{print $2}'`
			 tab_name=`db2 -x "SELECT  
			 distinct a.DEST_SCHEMA||'.'|| (case when  a.DEST_TABLE like '%$%' then substr(a.DEST_TABLE,1,locate('$',a.DEST_TABLE)-1)||'${etl_t}'
											else a.DEST_TABLE||'${etl_t}' end)
				FROM	  DB2INFO.ETL_LOAD_CFG a
				JOIN DB2INFO.ETL_INTER_CFG b
				ON a.INTER_CODE = b.INTER_CODE
				AND a.CYCLE_TYPE = b.CYCLE_TYPE
				WHERE  b.IS_VALID = 1  and a.INTER_CODE='${etl_id}'' 
				FETCH FIRST 1 ROWS ONLY WITH UR"`
				#错误信息
			echo "${proc_name}\t${4}\t程序接口\t${tab_name}未准备好接口\t\t" >> ${result_path}/${exe_time}.csv
		elif [	`wc -l	${log_path}/"${1}.trace"|awk {'print $1'}` -le 5 ]
			then
			##检查原表
			for src_tab in `cat ${proc_path}/${2}`
			do
			tab=`echo ${src_tab}|awk -F ',' '{print $1}'`
			tab_schema=`echo ${src_tab}|awk -F '.' '{print $1}'`
			time_flag=`echo ${src_tab}|awk -F ',' '{print $2}'`
			#循环调用check_table()
			#1上月月表/2当日表/3本月月初表/4当月最后一天/5上月月初表/6day表(DD)/
			#7上上月/8昨天/9当月月表/10上上上月/11明天/12后天/13下月第一天/14下月第二天
			case ${time_flag} in 
			1)
			#time_new ='1上月表'
			check_table ${tab}	${v_last_m}	 ${tab_schema}	${4}
			;;
			2)
			#time_new ='2当日表'
			check_table ${tab}	${v_time}	${tab_schema}	${4}
			;;
			3)
			#time_new ='3本月月初表'
			check_table ${tab}	${v_mon_f_d}	${tab_schema}	${4}
			;;
			4)
			#time_new ='4当月最后一天'
			check_table ${tab}	${v_last_mon_la_d}	${tab_schema}	${4}
			;;
			5)
			#time_new ='5上月月初表'
			check_table ${tab}	${v_last_mon_f_d}	${tab_schema}	${4}
			;;
			6)
			#time_new ='6日表(DD)'
			check_table ${tab}	${d_time}	${tab_schema}	${4}
			;;
			7)
			#time_new ='7上上月'
			check_table ${tab}	${v_pre_m}	${tab_schema}	${4}
			;;
			8)
			#time_new ='8昨天'
			check_table ${tab}	${v_yes_d}	${tab_schema}	${4}
			;;
			9)
			#time_new ='9当月月表'
			check_table ${tab}	${v_month}	${tab_schema}	${4}
			;;	
			10)
			#time_new ='10上上上月'
			check_table ${tab}	${v_pre2_m}	${tab_schema}	${4}
			;;	
			11)
			#time_new ='11明天'
			check_table ${tab}	${v_tom_d}	${tab_schema}	${4}
			;;	
			12)
			#time_new ='12后天'
			check_table ${tab}	${v_tom2_d}	${tab_schema}	${4}
			;;	
			13)
			#time_new ='13下月第一天'
			check_table ${tab}	${v_next_f_d}	${tab_schema}	${4}
			;;	
			14)
			#time_new ='14下月第二天'
			check_table ${tab}	${v_next_s_d}	${tab_schema}	${4}
			;;	
			esac
			done
		 ##查看程序状态，是在运行中，还是  
		elif [ `grep -c 'warning report' ${log_path}/"${1}.trace"` -gt 0 ]
				then
					err_str=`cat ${log_path}/"${1}.trace"|sed -n "/warning report/,/end warning report/p"|sed -n '2p'`
					echo "${proc_name}\t${4}\t程序报错\t${err_str}\t\t" >> ${result_path}/${exe_time}.csv
		fi		 
		else	   
			##程序没有调度起来，需要查看前置依赖
			echo "${proc_name}\t${4}\t程序依赖\t程序没有调度!\t\t" >> ${result_path}/${exe_time}.csv
		fi	
	else		 
		echo "${proc_name}\t${4}\t运行成功\t\t${v_start_time}\t${v_end_time}" >> ${result_path}/${exe_time}.csv
	fi
   
}


#扫描目录
conn_db	${db_info}
for proc_file in `ls ${proc_path}`
do
		proc_name=`echo ${proc_file}|awk -F'.' '{print $1}'`
		#db_info=`echo ${proc_file}|awk -F'.' '{print $2}'`
		date_flag=`echo ${proc_file}|awk -F'.' '{print $3}'`
		#conn_db	${db_info}
		if [ "${date_flag}" = "日" ]
			then
			check ${proc_name} ${proc_file} ${exe_time}	 ${date_flag}
		else
			check ${proc_name} ${proc_file} ${v_month}	${date_flag}
		fi	
		echo "finish"
done   

