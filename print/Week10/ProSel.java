public class ProSel {
	public static class ProSelMapper extends Mapper<Object, Text, Text, Text>{
		private Text word = new Text();
		private Text result = new Text( new byte[0] );
		
		public void map(Object key, Text value, Context context) throws IOException,InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), "|");
			if( itr.countTokens() < 4 ) return;
			
			String emp_id = itr.nextToken().trim();
			String dept_id = itr.nextToken().trim();
			int salary = Integer.parseInt( itr.nextToken().trim() );
			String emp_info = itr.nextToken().trim();
			// selection
			if ( salary >= 3500000 ) {
				word.set( new byte[0] );
				// projection
				word.set( emp_id + "|" + salary );
				context.write(word, result);
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: ProSel <in> <out>"); System.exit(2);
		}
		
		String inputPath = otherArgs[0] + "/relation";
		String outputPath = otherArgs[1] + "/prosel";
		
		Job job = new Job(conf, "ProSel");
		job.setJarByClass(ProSel.class);
		job.setMapperClass(ProSelMapper.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		FileSystem.get(job.getConfiguration()).delete( new Path(outputPath), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
