package com.spark.domain

import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import com.spark.date.DateFactory
import com.spark.oracle.OracleFactory
import com.spark.util.Tools
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object AppMain {
//
//  val conf: Configuration = new Configuration()
//  var fs: FileSystem = null
//  var files: RemoteIterator[LocatedFileStatus] = null


  def main(args: Array[String]) {

    var time :String = ""
    var T :Double = 0.5
    var u :Double = 0.3

    if (args.length == 3){
      time = args(0)
      T = args(1).toDouble
      u = args(2).toDouble
    }else if (args.length == 1){
      time = args(0)
    }else{
      time = DateFactory.getYesterdayYYYYMMDD()
    }

    var sparkConf : SparkConf =new SparkConf().setAppName("fqd_judge_function2").setMaster("local[*]")

    var sparkSession : SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    var start_time :String = DateFactory.format.format(new Date())

    var record_no:String = "02"+DateFactory.format2.format(new Date())
    var task_no :String = Tools.getUUID()
    var run_result :String = "01"
    var mdl_id :String = "djqdmx_gggjbf"
    var data_stat_time :String = DateFactory.format.format(DateFactory.getDateFromYYYYMMDDStart(time))
    var data_end_time :String  = DateFactory.format.format(DateFactory.getDateFromYYYYMMDDEnd(time))


    sparkSession.sql("use fqd")



    // 2022/1/24

    val create_e_mp_all =
      s"""
         |insert overwrite table e_mp_cur_curve_a_all
         |select
         |emcc.*
         |from
         |tmp_c_mp_qin tcm join
         |(select * from e_mp_cur_curve where dt = '$time' ) emcc
         |on tcm.mp_id = emcc.mp_id
       """.stripMargin

    val create_e_mp_a_all =
      s"""
         |insert overwrite table e_mp_cur_curve_a_all
         |select
         |*
         |from
         |e_mp_cur_curve
         |where dt = '$time' and phase_flag='1'
       """.stripMargin

    val create_e_mp_a =
      s"""
         |insert overwrite table  e_mp_cur_curve_a
         |select
         |*
         |from
         |e_mp_cur_curve_a_all emall
         |left semi join e_mp_cur_curve_c emc on ( emall.meter_id = emc.meter_id)
       """.stripMargin

    val create_e_mp_c =
      s"""
         |insert overwrite  table e_mp_cur_curve_c
         |select
         |*
         |from
         |e_mp_cur_curve
         |where dt = '$time' and phase_flag='3'
       """.stripMargin

    val create_c_mp_init =
      s"""
         |insert overwrite table tmp_c_mp_qin
         |select
         |cm.mp_id mp_id,
         |cm.cons_id cons_id,
         |cm.meas_mode meas_mode
         |from c_mp cm
         |where cm.meas_mode = '1'
       """.stripMargin

    val create_e_mp_c_init=
      s"""
         |insert overwrite table e_mp_cur_curve_c
         |select
         |*
         |from
         |e_mp_cur_curve_c emccc
         |left semi join tmp_c_mp_qin tcmq on ( emccc.mp_id = tcmq.mp_id )
       """.stripMargin


    sparkSession.sql(create_c_mp_init)

    println("c_mp:"+sparkSession.sql("select count(*) from tmp_c_mp_qin").collect()(0)(0))

    sparkSession.sql(create_e_mp_all)

    println("e_mp_all_select:"+sparkSession.sql("select count(*) from e_mp_cur_curve_a_all").collect()(0)(0))

    sparkSession.sql(create_e_mp_a_all)

    sparkSession.sql(create_e_mp_c)

//    sparkSession.sql(create_c_mp_init)

    sparkSession.sql(create_e_mp_c_init)

    sparkSession.sql(create_e_mp_a)




    //end 2022/1/24


    //test
//
//
//    val create_e_mp_a_all =
//      s"""
//         |insert overwrite table e_mp_cur_curve_a_all
//         |select
//         |*
//         |from
//         |e_mp_cur_curve
//         |where dt = '$time' and phase_flag='1'
//       """.stripMargin
//
//    val create_e_mp_a_all_new =
//      s"""
//         |insert overwrite table e_mp_cur_curve_a_all
//         |select
//         |DT,ID,DATA_DATE,PHASE_FLAG,DATA_WHOLE_FLAG,DATA_POINT_FLAG,MP_ID,METER_ID,COLL_OBJ_ID,I1,I2,I3,I4,I5,I6,I7,I8,I9,I10,I11,I12,I13,I14,I15,I16,I17,I18,I19,I20,I21,I22,I23,I24,I25,I26,I27,I28,I29,I30,I31,I32,I33,I34,I35,I36,I37,I38,I39,I40,I41,I42,I43,I44,I45,I46,I47,I48,I49,I50,I51,I52,I53,I54,I55,I56,I57,I58,I59,I60,I61,I62,I63,I64,I65,I66,I67,I68,I69,I70,I71,I72,I73,I74,I75,I76,I77,I78,I79,I80,I81,I82,I83,I84,I85,I86,I87,I88,I89,I90,I91,I92,I93,I94,I95,I96
//         |from
//         |(select * , row_number() over ( partition by mp_id , meter_id order by id desc ) rn from e_mp_cur_curve where dt = '$time' and phase_flag = '1' ) T
//         |where rn = 1
//       """.stripMargin
//
//    val create_e_mp_a =
//      s"""
//         |insert overwrite table  e_mp_cur_curve_a
//         |select
//         |*
//         |from
//         |e_mp_cur_curve_a_all emall
//         |left semi join e_mp_cur_curve_c emc on ( emall.meter_id = emc.meter_id)
//       """.stripMargin
//
//    val create_e_mp_c =
//      s"""
//         |insert overwrite  table e_mp_cur_curve_c
//         |select
//         |*
//         |from
//         |e_mp_cur_curve
//         |where dt = '$time' and phase_flag='3'
//       """.stripMargin
//
//    val create_e_mp_c_init=
//      s"""
//         |insert overwrite table e_mp_cur_curve_c
//         |select
//         |*
//         |from
//         |e_mp_cur_curve_c emccc
//         |left semi join tmp_c_mp_qin tcmq on ( emccc.mp_id = tcmq.mp_id )
//       """.stripMargin
//
//    val create_c_mp_init =
//      s"""
//         |insert overwrite table tmp_c_mp_qin
//         |select
//         |cm.mp_id mp_id,
//         |cm.cons_id cons_id,
//         |cm.meas_mode meas_mode
//         |from c_mp cm
//         |where cm.meas_mode = '1'
//       """.stripMargin
//
//
//    sparkSession.sql(create_c_mp_init)
//
//    println("c_mp:"+sparkSession.sql("select count(*) from tmp_c_mp_qin").collect()(0)(0))
//
////    sparkSession.sql(truncate_e_mp_a_all)
//
////    println("truncate:"+sparkSession.sql("select count(*) from e_mp_cur_curve_a_all").collect()(0)(0))
//
//    Thread.sleep(5000)
//
////    sparkSession.sql(create_e_mp_a_all)
//
//    sparkSession.sql(create_e_mp_a_all_new)
//
//    sparkSession.sql(create_e_mp_c)
//
//    sparkSession.sql(create_e_mp_c_init)
//
//    sparkSession.sql(create_e_mp_a)
//
//

    //end test


//    sparkSession.sql(drop_e_mp_a_all)

    println("a:"+sparkSession.sql("select count(*) from e_mp_cur_curve_a").collect()(0)(0))
    println("c:"+sparkSession.sql("select count(*) from e_mp_cur_curve_c").collect()(0)(0))
//
//    sparkSession.sql(
//      s"""
//         |select
//         |*
//         |from
//         |e_mp_cur_curve
//         |where dt = '$time'
//       """.stripMargin).registerTempTable("e_mp_cur_curve_temp")

//    var drop_emp_judge =
//      s"""
//         drop table if exists emp_judge
//       """.stripMargin
//
//    sparkSession.sql(drop_emp_judge)

    var create_emp_judge =
      s"""
         |insert overwrite  table emp_judge
         |select a_t.meter_id meter_id , a_t.mp_id mp_id , a_t.a_list a_list , c_t.c_list c_list from
         |(select  meter_id , concat_ws(',',I1,I2,I3,I4,I5,I6,I7,I8,I9,I10,I11,I12,I13,I14,I15,I16,I17,I18,I19,I20,I21,I22,I23,I24,I25,I26,I27,I28,I29,I30,I31,I32,I33,I34,I35,I36,I37,I38,I39,I40,I41,I42,I43,I44,I45,I46,I47,I48,I49,I50,I51,I52,I53,I54,I55,I56,I57,I58,I59,I60,I61,I62,I63,I64,I65,I66,I67,I68,I69,I70,I71,I72,I73,I74,I75,I76,I77,I78,I79,I80,I81,I82,I83,I84,I85,I86,I87,I88,I89,I90,I91,I92,I93,I94,I95,I96) c_list from e_mp_cur_curve_c) c_t
         |left join
         |(select  meter_id , mp_id , concat_ws(',',I1,I2,I3,I4,I5,I6,I7,I8,I9,I10,I11,I12,I13,I14,I15,I16,I17,I18,I19,I20,I21,I22,I23,I24,I25,I26,I27,I28,I29,I30,I31,I32,I33,I34,I35,I36,I37,I38,I39,I40,I41,I42,I43,I44,I45,I46,I47,I48,I49,I50,I51,I52,I53,I54,I55,I56,I57,I58,I59,I60,I61,I62,I63,I64,I65,I66,I67,I68,I69,I70,I71,I72,I73,I74,I75,I76,I77,I78,I79,I80,I81,I82,I83,I84,I85,I86,I87,I88,I89,I90,I91,I92,I93,I94,I95,I96) a_list from e_mp_cur_curve_a) a_t
         |on a_t.meter_id = c_t.meter_id """.stripMargin

    sparkSession.sql(create_emp_judge)


//    sparkSession.sql(drop_e_mp_a)
//    sparkSession.sql(drop_e_mp_c)

    println("emp_judge:"+sparkSession.sql("select count(*) from emp_judge").collect()(0)(0))

    sparkSession.udf.register("judgeStealUDF",judgeStealUDF _)


//    var drop_judge_result =
//      s"""
//         |drop table if exists judge_result
//       """.stripMargin
//
//    sparkSession.sql(drop_judge_result)

    val create_judge_result =
      s"""
         |insert overwrite  table judge_result
         |select
         |meter_id,
         |mp_id,
         |judgeStealUDF(a_list,c_list,$T,$u) judge,
         |cast(from_unixtime(unix_timestamp()) as timestamp) analysis_time
         |from emp_judge
       """.stripMargin

    sparkSession.sql(create_judge_result)


//    sparkSession.sql(drop_emp_judge)


//    val drop_insert_find_record =
//      s"""
//         |drop table if exists insert_find_record
//       """.stripMargin
//
//    sparkSession.sql(drop_insert_find_record)

    val create_insert_find_record =
      s"""
         |insert overwrite  table insert_find_record
         |select
         | meter_id ,
         | mp_id ,
         | split(judge,'#')[1] as prob_desc ,
         | analysis_time
         | from judge_result
         | where judge like concat('1','%')
       """.stripMargin

    sparkSession.sql(create_insert_find_record)

//    sparkSession.sql(drop_judge_result)

    println("异常个数："+sparkSession.sql("select count(*) from insert_find_record").collect()(0)(0))


//    创建 d_meter 临时表
//    val drop_tmp_d_meter_qin =
//      s"""
//         |drop table if exists tmp_d_meter_qin
//       """.stripMargin
    val create_tmp_d_mter_qin =
      s"""
         |insert overwrite  table tmp_d_meter_qin
         |select
         |dm.meter_id meter_id,
         |dm.bar_code bar_code
         |from d_meter dm
         |left semi join insert_find_record ifr on (dm.meter_id = ifr.meter_id)
       """.stripMargin
//    sparkSession.sql(drop_tmp_d_meter_qin)

    sparkSession.sql(create_tmp_d_mter_qin)


//    val drop_tmp_c_mp_qin =
//      s"""
//         |drop table if exists tmp_c_mp_qin
//       """.stripMargin
    val create_tmp_c_mp_qin =
      s"""
         |insert overwrite table tmp_c_mp_qin
         |select
         |cm.mp_id mp_id,
         |cm.cons_id cons_id,
         |cm.meas_mode meas_mode
         |from c_mp cm
         |left semi join insert_find_record ifr on (cm.mp_id = ifr.mp_id)
       """.stripMargin
//    sparkSession.sql(drop_tmp_c_mp_qin)

    sparkSession.sql(create_tmp_c_mp_qin)

//    val drop_tmp_c_cons_qin =
//      s"""
//         |drop table if exists tmp_c_cons_qin
//       """.stripMargin
    val create_tmp_c_cons_qin =
      s"""
         |insert overwrite table tmp_c_cons_qin
         |select
         |cc.cons_id cons_id,
         |cc.org_no org_no,
         |cc.cons_no cons_no,
         |cc.cons_sort_code cons_sort_code
         |from c_cons cc
         |left semi join tmp_c_mp_qin tcmq on (cc.cons_id = tcmq.cons_id)
       """.stripMargin
//    sparkSession.sql(drop_tmp_c_cons_qin)

    sparkSession.sql(create_tmp_c_cons_qin)

//    val drop_find_record=
//      s"""
//         |drop table if exists insert_task_find_record
//       """.stripMargin
//
//
//    sparkSession.sql(drop_find_record)

    var insert_find_record =
      s"""
         |insert overwrite  table insert_task_find_record
         |select
         |date_format(ifr.analysis_time, '02yyyyMMddHHmmssSS') run_list_no,
         |'$record_no' record_no,
         | dm.bar_code bar_code,
         | ifr.analysis_time analysis_time,
         | ifr.prob_desc prob_desc,
         |'01' type_code,
         |'$mdl_id' mdl_id,
         |cm.cons_id cons_id
         |from insert_find_record ifr
         |left join tmp_d_meter_qin dm on ifr.meter_id = dm.meter_id
         |left join tmp_c_mp_qin cm on ifr.mp_id = cm.mp_id
         |where cm.meas_mode = '1'
       """.stripMargin

    sparkSession.sql(insert_find_record)

//    sparkSession.sql(drop_insert_find_record)

//    val drop_finally_find_record =
//      s"""
//         |drop table if exists insert_finally_find_record
//       """.stripMargin
//
//    sparkSession.sql(drop_finally_find_record)

    val create_finally_find_record =
      s"""
         |insert overwrite table insert_finally_find_record
         |select
         |ifr.run_list_no run_list_no,
         |ifr.record_no record_no,
         | cc.org_no org_no,
         |ifr.bar_code bar_code,
         | cc.cons_no cons_no,
         | ifr.analysis_time analysis_time,
         | ifr.prob_desc prob_desc,
         |ifr.type_code type_code,
         |ifr.mdl_id mdl_id,
         |'01' mdl_source
         |from insert_task_find_record ifr
         |left join tmp_c_cons_qin cc on ifr.cons_id = cc.cons_id
         |where cc.cons_sort_code = '01'
       """.stripMargin

//    create table ID_RECORD_FIND_CONS
//    (
//
//      suspicion_level NUMBER(8,2),
//      occur_time      DATE,
//      end_time        DATE,
//
//
//      mdl_source      VARCHAR2(2)
//    )


    sparkSession.sql(create_finally_find_record)
//    sparkSession.sql(drop_find_record)

    //写入 ID_RECORD_FIND_CONS 表
    val df2oracle_find_record = sparkSession.sql("select * from insert_finally_find_record")


    var state_write1:Int = OracleFactory.run(df2oracle_find_record,"ID_RECORD_FIND_CONS")

    if (state_write1 != 0){
      println("写入 ID_RECORD_FIND_CONS 表 record error ")
      run_result = "02"
    }
    //end 写入 ID_RECORD_FIND_CONS 表

//    val drop_task_run_record =
//      s"""
//         |drop table if exists insert_task_run_record
//       """.stripMargin
//
//    sparkSession.sql(drop_task_run_record)

    //写入 ID_TASK_RUN_RECORD 表
    val insert_task_run_record =
      s"""
         |insert overwrite  table insert_task_run_record
         |select
         |'$record_no' record_no,
         |'$task_no' task_no,
         |cast('$start_time' as timestamp) start_time,
         |cast(from_unixtime(unix_timestamp()) as timestamp) end_time,
         |'$run_result' run_result,
         |count(*) abnormal_cons_cnt,
         |'$mdl_id' mdl_id,
         |cast('$data_stat_time' as timestamp) data_stat_time,
         |cast('$data_end_time' as timestamp) data_end_time,
         |'' run_desc
         |from insert_finally_find_record
       """.stripMargin

//    val df2oracle = sparkSession.sql(insert_task_run_record)
    sparkSession.sql(insert_task_run_record)
    val df2oracle = sparkSession.sql("select * from insert_task_run_record")

    df2oracle.show()
//
    var state_write2 :Int =  OracleFactory.run(df2oracle,"ID_TASK_RUN_RECORD")

    if (state_write2 != 0){
      println("write record error ")
      sparkSession.close()
      return
    }

//    sparkSession.sql(drop_tmp_c_cons_qin)
//    sparkSession.sql(drop_tmp_c_mp_qin)
//    sparkSession.sql(drop_tmp_d_meter_qin)
//
//    sparkSession.sql(drop_finally_find_record)
//    sparkSession.sql(drop_task_run_record)

//    println("总电能表数："+count_meter_id)
//    println("用电异常电能表数："+abnormal_cons)


    sparkSession.close()

  }

  def judgeStealUDF(I1_96_A :String, I1_96_C :String , T :Double , u :Double): String ={
    if (I1_96_A == null || I1_96_C == null){
      return "0#"
    }
    var a_collect = I1_96_A.split(",")
    var c_collect = I1_96_C.split(",")

    if (a_collect.length != 96 || c_collect.length != 96){
      return "0#"
    }

    var a_mean : Double = 0 ;
    var c_mean :Double = 0 ;

    var ac_max :Double = Double.MinValue
    var ac_min :Double = Double.MaxValue

    var a_total:Double = 0
    var c_total:Double = 0

    //求AC最大值，以及统计总和
    var i = 0
    while ( i < 96){

      var a :Double =  a_collect.apply(i).toDouble
      var c :Double =  c_collect.apply(i).toDouble

      a_total += a
      c_total += c

      if (ac_max < a){
        ac_max = a
      }
      if (ac_max < c ){
        ac_max = c
      }

      if (ac_min > a){
        ac_min = a
      }

      if (ac_min > c){
        ac_min = c
      }

      i += 1
    }

    //求平均值
    a_mean = a_total / 96
    c_mean = c_total / 96

    var ac_mean_min : Double = a_mean
    if (ac_mean_min > c_mean)
    {
      ac_mean_min = c_mean
    }


    var count_g : Int = 0
    var g_array : Array[Double] = new Array[Double](96)

    i = 0
    while ( i < 96){
      var a :Double = a_collect.apply(i).toDouble
      var c :Double = c_collect.apply(i).toDouble

      // AC两列中，均大于ac_mean_min的行，进行计算，并统计个数
      if ( a >= ac_mean_min && c >= ac_mean_min){


        var row_abs :Double = Math.abs(a-c)

        var row_max :Double = a
        if (row_max < c){
          row_max = c
        }
        g_array(count_g) = row_abs / row_max

        count_g += 1

      }
      i += 1
    }

    //统计G数组中 >- T的个数

    var F :Int = 0
    i = 0
    while ( i < count_g) {

      if(g_array(i) >= T){
        F += 1
      }


      i += 1
    }


    var isSteal : Boolean = false

    if ( F >= count_g * u){

      isSteal = true
    }

    var prob_desc :String = "".concat(ac_max.toString).concat(",").concat(ac_min.toString).concat(",").concat(a_mean.toString).concat(",").concat(c_mean.toString)

    if (isSteal){
      "1#".concat(prob_desc)
    }else{
      "0#".concat(prob_desc)
    }
  }


  def getI1ToI96(E_MP_CUR_CURVE : DataFrame): DataFrame = {

    E_MP_CUR_CURVE.select("I1","I2","I3","I4","I5","I6","I7","I8","I9","I10","I11","I12","I13","I14","I15","I16","I17","I18","I19","I20","I21","I22","I23","I24","I25","I26","I27","I28","I29","I30","I31","I32","I33","I34","I35","I36","I37","I38","I39","I40","I41","I42","I43","I44","I45","I46","I47","I48","I49","I50","I51","I52","I53","I54","I55","I56","I57","I58","I59","I60","I61","I62","I63","I64","I65","I66","I67","I68","I69","I70","I71","I72","I73","I74","I75","I76","I77","I78","I79","I80","I81","I82","I83","I84","I85","I86","I87","I88","I89","I90","I91","I92","I93","I94","I95","I96")

  }

  def mkdir(dir : String) :Boolean= {
    var result = false


    result
  }



}
