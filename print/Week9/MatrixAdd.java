public class MatrixAdd {
	public static class MatrixAddMapper extends Mapper <Object, Text, Text, IntWritable> {
		private final static IntWritable i_value = new IntWritable(1);
		private Text word = new Text();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			int row_id = Integer.parseInt( itr.nextToken().trim() );
			int col_id = Integer.parseInt( itr.nextToken().trim() );
			int m_value = Integer.parseInt( itr.nextToken().trim() );
			word.set( row_id + "," + col_id );
			i_value.set( m_value );
			context.write(word, i_value );
		}
	}
	public static class MatrixAddReducer extends Reducer <Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String [] otherArgs = new GenericOptionParser(conf, args).getRemainingArgs();
		if(otherArgs.length != 2) {
			System.exit(2);
		}
		Job job = new Job(conf, "MatrixAdd");
		job.setJarByClass(MatrixAdd.class);
		job.setMapperClass(MatrixAddMapper.class);
		job.setReducerClass(MatrixAddReducer.class);
		job.setCombinerClass(MatrixAddReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputFormat(job, new Path(otherArgs[0]));
		FileOutputFormat.addOutputFormat(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete(new Path(otherArgs[1]), true);
		job.waitForCompletion(true);
	}
}
