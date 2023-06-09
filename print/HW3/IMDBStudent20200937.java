public final class IMDBStudent20200937 {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: IMDB <in-file> <out-file>");
            System.exit(1);
        }
        SparkSession spark = SparkSession
            .builder()
            .appName("IMDB")
            .getOrCreate();

        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

        FlatMapFunction<String, String> fmf = new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) {
            	ArrayList<String> result = new ArrayList<String>();
            	String[] values = s.split("::");
            	StringTokenizer itr = new StringTokenizer(values[2], "|");
            	String genre = "";
            	while (itr.hasMoreTokens()) {
            		genre = itr.nextToken();
            		result.add(genre);
            	}
                return result.iterator();
            }
        };
        JavaRDD<String> words = lines.flatMap(fmf);

        PairFunction<String, String, Integer> pf = new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2(s, 1);
            }
        };
        JavaPairRDD<String, Integer> ones = words.mapToPair(pf);

        Function2<Integer, Integer, Integer> f2 = new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer x, Integer y) {
                return x + y;
            }
        };
        JavaPairRDD<String, Integer> counts = ones.reduceByKey(f2);
        
        JavaRDD<String> result = counts.map(x -> x._1 + " " + x._2);

        result.saveAsTextFile(args[1]);
        spark.stop();
    }
}
