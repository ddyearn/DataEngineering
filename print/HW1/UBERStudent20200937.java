public class UBERStudent20200937{

        public static class UBERMapper extends Mapper<Object, Text, Text, Text>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException  {
		    String[] uber = value.toString().split(",");
		    String region = uber[0];
		    String date = uber[1];
		    String vehicles = uber[2];
		    String trips = uber[3];
		
		    String[] days = {"SUN", "MON", "TUE", "WED", "THR", "FRI", "SAT"};
           	    Date day = new Date(date);
		    String dayOfWeek= days[day.getDay()];
		    Text regionDate = new Text(region + "," + dayOfWeek);
		    Text tripVehicle = new Text(trips + "," + vehicles);
		    context.write(regionDate, tripVehicle);
		}
	}

	public static class UBERReducer extends Reducer<Text,Text,Text,Text>  {
		private Text word = new Text();

		public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException  {
		    int tripAll = 0;
		    int vehicleAll = 0;

		    for (Text val : value) {
			String line = val.toString();
			String[] tripVehicle = line.split(",");

			int trip = Integer.parseInt(tripVehicle[0]);
			int vehicle = Integer.parseInt(tripVehicle[1]);

			tripAll += trip;
			vehicleAll += vehicle;
		    }

		    word.set(Integer.toString(tripAll)+","+Integer.toString(vehicleAll));
		    context.write(key, word);
       		}
	}
}
