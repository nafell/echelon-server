use chrono::{DateTime, Utc};
use influxdb::{InfluxDbWriteable};
use serde::{Deserialize, Serialize};

#[derive(InfluxDbWriteable, Deserialize, Serialize, Debug)]
pub struct WearReading {
    pub time: DateTime<Utc>,
    #[influxdb(tag)]
    pub facility_name: String,
    #[influxdb(tag)]
    pub machine_type: String,
    #[influxdb(tag)]
    pub equipment_id: String,
    #[influxdb(tag)]
    pub equipment_version: String,
    n_0um: i32,
    n_1um: i32,
    n_2um: i32,
    n_3um: i32,
    n_4um: i32,
    n_5um: i32,
    n_6um: i32,
    n_7um: i32,
    n_8um: i32,
    n_9um: i32,
    n_10um: i32,
    n_11um: i32,
    n_12um: i32,
    n_13um: i32,
    n_14um: i32,
    n_15um: i32,
    n_16um: i32,
    n_17um: i32,
    n_18um: i32,
    n_19um: i32,
    n_20um: i32,
    n_21um: i32,
    n_22um: i32,
    n_23um: i32,
    n_24um: i32,
    n_25um: i32,
    n_26um: i32,
    n_27um: i32,
    n_28um: i32,
    n_29um: i32,
    n_30um: i32,
    n_31um: i32,
    n_32um: i32,
    n_33um: i32,
    n_34um: i32,
    n_35um: i32,
    n_36um: i32,
    n_37um: i32,
    n_38um: i32,
    n_39um: i32,
    n_40um: i32,
    n_41um: i32,
    n_42um: i32,
    n_43um: i32,
    n_44um: i32,
    n_45um: i32,
    n_46um: i32,
    n_47um: i32,
    n_48um: i32,
    n_49um: i32,
    n_50um: i32,
    n_51um: i32,
    n_52um: i32,
    n_53um: i32,
    n_54um: i32,
    n_55um: i32,
    n_56um: i32,
    n_57um: i32,
    n_58um: i32,
    n_59um: i32,
    n_60um: i32,
    n_61um: i32,
    n_62um: i32,
    n_63um: i32,
    n_64um: i32,
    n_65um: i32,
    n_66um: i32,
    n_67um: i32,
    n_68um: i32,
    n_69um: i32,
    n_70um: i32,
    n_71um: i32,
    n_72um: i32,
    n_73um: i32,
    n_74um: i32,
    n_75um: i32,
    n_76um: i32,
    n_77um: i32,
    n_78um: i32,
    n_79um: i32,
    n_80um: i32,
    n_81um: i32,
    n_82um: i32,
    n_83um: i32,
    n_84um: i32,
    n_85um: i32,
    n_86um: i32,
    n_87um: i32,
    n_88um: i32,
    n_89um: i32,
    n_90um: i32,
    n_91um: i32,
    n_92um: i32,
    n_93um: i32,
    n_94um: i32,
    n_95um: i32,
    n_96um: i32,
    n_97um: i32,
    n_98um: i32,
    n_99um: i32,
    n_100um: i32,
    n_101um: i32,
}

pub fn create_wear_reading(
    time: DateTime<Utc>, 
    facility_name: String, 
    machine_type: String, 
    equipment_id: String, 
    equipment_version: String, 
    data: Vec<i32>
) -> WearReading {
    let wear_reading = WearReading {
        time: time,
        facility_name: facility_name,
        machine_type: machine_type,
        equipment_id: equipment_id,
        equipment_version: equipment_version,
        n_0um: data[0],
        n_1um: data[1],
        n_2um: data[2],
        n_3um: data[3],
        n_4um: data[4],
        n_5um: data[5],
        n_6um: data[6],
        n_7um: data[7],
        n_8um: data[8],
        n_9um: data[9],
        n_10um: data[10],
        n_11um: data[11],
        n_12um: data[12],
        n_13um: data[13],
        n_14um: data[14],
        n_15um: data[15],
        n_16um: data[16],
        n_17um: data[17],
        n_18um: data[18],
        n_19um: data[19],
        n_20um: data[20],
        n_21um: data[21],
        n_22um: data[22],
        n_23um: data[23],
        n_24um: data[24],
        n_25um: data[25],
        n_26um: data[26],
        n_27um: data[27],
        n_28um: data[28],
        n_29um: data[29],
        n_30um: data[30],
        n_31um: data[31],
        n_32um: data[32],
        n_33um: data[33],
        n_34um: data[34],
        n_35um: data[35],
        n_36um: data[36],
        n_37um: data[37],
        n_38um: data[38],
        n_39um: data[39],
        n_40um: data[40],
        n_41um: data[41],
        n_42um: data[42],
        n_43um: data[43],
        n_44um: data[44],
        n_45um: data[45],
        n_46um: data[46],
        n_47um: data[47],
        n_48um: data[48],
        n_49um: data[49],
        n_50um: data[50],
        n_51um: data[51],
        n_52um: data[52],
        n_53um: data[53],
        n_54um: data[54],
        n_55um: data[55],
        n_56um: data[56],
        n_57um: data[57],
        n_58um: data[58],
        n_59um: data[59],
        n_60um: data[60],
        n_61um: data[61],
        n_62um: data[62],
        n_63um: data[63],
        n_64um: data[64],
        n_65um: data[65],
        n_66um: data[66],
        n_67um: data[67],
        n_68um: data[68],
        n_69um: data[69],
        n_70um: data[70],
        n_71um: data[71],
        n_72um: data[72],
        n_73um: data[73],
        n_74um: data[74],
        n_75um: data[75],
        n_76um: data[76],
        n_77um: data[77],
        n_78um: data[78],
        n_79um: data[79],
        n_80um: data[80],
        n_81um: data[81],
        n_82um: data[82],
        n_83um: data[83],
        n_84um: data[84],
        n_85um: data[85],
        n_86um: data[86],
        n_87um: data[87],
        n_88um: data[88],
        n_89um: data[89],
        n_90um: data[90],
        n_91um: data[91],
        n_92um: data[92],
        n_93um: data[93],
        n_94um: data[94],
        n_95um: data[95],
        n_96um: data[96],
        n_97um: data[97],
        n_98um: data[98],
        n_99um: data[99],
        n_100um: data[100],
        n_101um: data[101],
    };
    
    return wear_reading;
}

pub fn wear_string(wear_result: WearResult) -> String {
    match wear_result {
        WearResult::Nominal => "正常".to_string(),
        WearResult::Warning => "警告".to_string(),
        WearResult::Critical => "危険".to_string(),
    }
}

pub fn calc_wear(wear_reading: &WearReading) -> WearResult {
    if wear_reading.machine_type == "ボールベアリング" {
        calc_wear_bearing(wear_reading)
    } else if wear_reading.machine_type == "ギアトレイン" {
        calc_wear_geartrain(wear_reading)
    } else if wear_reading.machine_type == "混合" {
        let result_bearing = calc_wear_bearing(&wear_reading);
        let result_geartrain = calc_wear_geartrain(&wear_reading);
        
        if result_bearing as i32 > result_geartrain as i32 {
            calc_wear_bearing(&wear_reading)
        } else {
            calc_wear_geartrain(&wear_reading)
        }
    } else {
        WearResult::Nominal
    }
}

pub fn calc_wear_bearing(wear_reading: &WearReading) -> WearResult {
    let particle_sum:i32 = wear_reading.n_0um + wear_reading.n_1um + wear_reading.n_2um;
    if particle_sum > 5000 {
        WearResult::Critical
    } else if particle_sum > 3000 {
        WearResult::Warning
    } else {
        WearResult::Nominal
    }
}
pub fn calc_wear_geartrain(wear_reading: &WearReading) -> WearResult {
    let particle_sum:i32 = wear_reading.n_80um + wear_reading.n_81um + wear_reading.n_82um + wear_reading.n_83um + wear_reading.n_84um + wear_reading.n_85um + wear_reading.n_86um + wear_reading.n_87um + wear_reading.n_88um + wear_reading.n_89um + wear_reading.n_90um + wear_reading.n_91um + wear_reading.n_92um + wear_reading.n_93um + wear_reading.n_94um + wear_reading.n_95um + wear_reading.n_96um + wear_reading.n_97um + wear_reading.n_98um + wear_reading.n_99um + wear_reading.n_100um + wear_reading.n_101um;
    if particle_sum > 0 {
        WearResult::Critical
    } else {
        WearResult::Nominal
    }
}

pub enum WearResult {
    Nominal = 1,
    Warning = 2,
    Critical = 3,
}

