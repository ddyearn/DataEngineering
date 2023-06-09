public class MatrixMul {
	public static class MatrixMulMapper1 extends Mapper<Object, Text, Text, IntWritable>
	{
		private IntWritable i_value = new IntWritable();
		private Text word = new Text();
		private int m_value;
		private int k_value;
		private int n_value;
		private boolean isA = false;
		private boolean isB = false;
		
		protected void setup(Context context) throws IOException, InterruptedException
		{
			Configuration conf = context.getConfiguration();
			m_value = conf.getInt("m", -1);
			k_value = conf.getInt("k", -1);
			n_value = conf.getInt("n", -1);
			String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
			if( filename.equals( "matrix_a.txt" ) ) isA = true;
			if( filename.equals( "matrix_b.txt" ) ) isB = true;
		}
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			if(itr.countTokens() < 3) return;
			// if A_matrix row_id(i) col_id(x)	// if B_matrix row_id(x) col_id(j)
			int row_id = Integer.parseInt( itr.nextToken().trim() ); 
			int col_id = Integer.parseInt( itr.nextToken().trim() ); 
			int matrix_value = Integer.parseInt( itr.nextToken().trim() );
			i_value.set( matrix_value );
			
			if ( isA ) {
				for( int i = 0 ; i < n_value ; i++ ) {		// i, j, x sequence
					word.set(new byte[0]);
					word.set(row_id + "," + i + "," + col_id);
					context.write(word, i_value);
				}
			} else if ( isB ) {
				for( int i = 0; i < m_value ; i++ ) {		// i, j, x sequence
					word.set(new byte[0]);
					word.set(i + "," + col_id + "," + row_id);
					context.write(word, i_value);
				}
			}
		}
	}
	
	public static class MatrixMulReducer1 extends Reducer<Text,IntWritable,Text,IntWritable>{
		private IntWritable result = new IntWritable();
		private Text word = new Text();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int mul = 1;
			
			for(IntWritable val : values) {
				mul *= val.get();
			}
			result.set(mul);
			
			StringTokenizer itr = new StringTokenizer(key.toString(), ",");
			int row_id = Integer.parseInt( itr.nextToken().trim() ); 
			int col_id = Integer.parseInt( itr.nextToken().trim() );
			word.set(row_id + "," + col_id);
			
			context.write(word, result);
		}
	}
	
	public static class MatrixMulMapper2 extends Mapper<Object, Text, Text, IntWritable> {
		private Text word = new Text();
		private IntWritable i_value = new IntWritable();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer( value.toString() );
			String i_key = itr.nextToken().trim();
			// 다시 파일을 읽으면, Text로 읽히기 때문에 Key와 Value를 다시 처리하는 과정
			int matrix_value = Integer.parseInt( itr.nextToken().trim() );
			i_value.set( matrix_value );
			word.set( i_key );
			context.write(word, i_value );
		}
	}
	
	public static class MatrixMulReducer2 extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int add = 0;
			for (IntWritable val : values) {
				add += val.get();
			}
			result.set(add);
			context.write( key, result);
		}
	}
	
	
	
	
	
	
	
	
	
	public static void main(String[] args) throws Exception {
		// m은 matrix_a의 행의 갯수
		// n은 matrix_b의 열의 갯수
		// k는 matrix_a의 열의 갯수, matrix_b의 행의 갯수
		int m_value = 2;
		int k_value = 2;
		int n_value = 2;
		String first_phase_result = "/first_phase_result" ;
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2)
		{
			System.err.println("Usage: MatrixMul <in> <out>");
			System.exit(2);
		}
		conf.setInt("m", m_value);
		conf.setInt("k", k_value);
		conf.setInt("n", n_value);
		
		Job job1 = new Job(conf, "matrix mult1");
		job1.setJarByClass(MatrixMul.class);
		job1.setMapperClass(MatrixMulMapper1.class);
		job1.setReducerClass(MatrixMulReducer1.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job1, new Path(otherArgs[0] + "/matrix"));
		FileOutputFormat.setOutputPath(job1, new Path(first_phase_result));
		FileSystem.get(job1.getConfiguration()).delete( new Path( first_phase_result ), true);
		job1.waitForCompletion(true);
		
		Job job2 = new Job(conf, "matrix mult2");
		job2.setJarByClass(MatrixMul.class);
		job2.setMapperClass(MatrixMulMapper2.class);
		job2.setReducerClass(MatrixMulReducer2.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job2, new Path(first_phase_result));
		FileOutputFormat.setOutputPath(job2, new Path( otherArgs[1] + "/matrix"));
		FileSystem.get(job2.getConfiguration()).delete( new Path( otherArgs[1] + "/matrix"), true);
		job2.waitForCompletion(true);
	}
}
