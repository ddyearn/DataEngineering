public class IMDBStudent20200937 {
	public static class Map extends Mapper<LongWritable, Text, Text, LongWritable> {
		private final LongWritable one = new LongWritable(1);
		private Text word = new Text();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String [] splt = value.toString().split("::");
			
			if(splt.length < 3) return;
			
			String genre = splt[splt.length - 1];
			
			StringTokenizer itr = new StringTokenizer(genre, "|");
			while(itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}
	
	public static class Reduce extends Reducer<Text, LongWritable, Text, LongWritable> {
		private LongWritable count = new LongWritable();
		
		public void reduce(Text key, Iterable<LongWritable> value, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for(LongWritable val : value) {
				sum += val.get();
			}
			
			count.set(sum);
			context.write(key, count);
		}
	}
}
