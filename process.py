
from pyspark import SparkContext, SparkConf

conf = (SparkConf()
        .setMaster("local")
        .setAppName("My app")
        .set("spark.executor.memory", "5g"))
sc = SparkContext(conf=conf)

def filter_line(line):
    line = line.split('\t')
    return len(line) == 44

def line_format(line):
    line = line.split('\t')
    label = float(line[0])
    feats = [s.split(':')[1] for s in line[1:]]
    feats = [float(i) for i in feats]
    return [label] + feats

#feat_nb is the column index of the cross feature
#area=2, time_cost=3, start_dest_distance=5
def join_table(feat_rdd, ecr_rdd, feat_nb=2):
    feat_rdd = feat_rdd.map(lambda line: ((int(line[feat_nb]), int(line[1])), line))
    def add_ecr(line):
        key, feat_ecr = line[0], line[1]
        feat_ecr[0].append(feat_ecr[1])
        return (key, feat_ecr[0])
    ##pair form is (key_pair, (lis, ecr_rate))
    feat_rdd = feat_rdd.leftOuterJoin(ecr_rdd).filter(lambda line: line[1][1] is not None).map(add_ecr).map(lambda line: line[1])
    return feat_rdd

def line_format_1(line):
    line = [str(i) for i in line]
    label, feat = line[0], line[1:]
    feat = [str(ind+1)+':'+feat[ind] for ind in range(len(feat))]
    feat_str = "\t".join(feat)
    feat_str = label+"\t"+feat_str
    return feat_str

if __name__=="__main__":
    area_fee = sc.textFile('/user/bigdata_driver_ecosys_test/xiajizhong/area_fee').map(lambda line: eval(line)).map(lambda line: ((int(line[0]), int(line[1])), line[2]))
    time_fee = sc.textFile('/user/bigdata_driver_ecosys_test/xiajizhong/time_fee').map(lambda line: eval(line)).map(lambda line: ((int(line[0]), int(line[1])), line[2]))
    dis_fee = sc.textFile('/user/bigdata_driver_ecosys_test/xiajizhong/dis_fee').map(lambda line: eval(line)).map(lambda line: ((int(line[0]), int(line[1])), line[2]))
    #feat_rdd = sc.textFile('/user/bigdata_driver_ecosys_test/cgb/price_fe/xgb_v2/2017/06/11/*').filter(filter_line).map(line_format)
    feat_rdd = sc.textFile('/user/bigdata_driver_ecosys_test/cgb/price_fe/xgb_v2/2017/06/13/*').filter(filter_line).map(line_format)
    feat_rdd = join_table(feat_rdd, area_fee, 2)
    feat_rdd = join_table(feat_rdd, time_fee, 3)
    feat_rdd = join_table(feat_rdd, dis_fee, 5)
    feat_rdd = feat_rdd.map(line_format_1)
    feat_rdd.saveAsTextFile('/user/bigdata_driver_ecosys_test/xiajizhong/area_time_dis_ecr_val')
