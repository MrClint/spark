package base;
import com.sun.net.httpserver.HttpServer;
import com.sun.jersey.api.container.httpserver.HttpServerFactory;
import java.io.IOException;

import javax.ws.rs.GET;
import javax.ws.rs.Produces;
import javax.ws.rs.Path;

/**
 * @company www.zbj.com
 * @comment 
 * @author Clint
 * @date 2016-02-04.
 */// The Java class will be hosted at the URI path "/helloworld"
@Path("/helloworld")
public class WebServiceTest {
    // The Java method will process HTTP GET requests
    @GET
    // The Java method will produce content identified by the MIME Media type "text/plain"
    @Produces("text/plain")
   // @Produces("application/json")  MediaType

    public String getClichedMessage() {
        // Return some cliched textual content
        return "Hello World";
    }

    public static void main(String[] args) throws IOException {
        HttpServer server = HttpServerFactory.create("http://localhost:9998/");
        server.start();

        System.out.println("Server running");
        System.out.println("Visit: http://localhost:9998/helloworld");
        System.out.println("Hit return to stop...");
        System.in.read();
        System.out.println("Stopping server");
        server.stop(0);
        System.out.println("Server stopped");
    }
}
