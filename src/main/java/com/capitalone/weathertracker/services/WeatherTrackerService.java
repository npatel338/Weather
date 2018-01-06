
@Component
public class WeatherTrackerService{
    
    public Response testingService(){
        return Response
            // .ok("Weather tracker is up and running!\n")
            .ok("I am in WeatherTrackerService! \n")
            .build();
    }
}