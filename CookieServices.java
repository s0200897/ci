import com.google.common.base.Strings;
import com.linecorp.armeria.common.*;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Post;
import io.netty.handler.codec.http.cookie.Cookie;
import io.netty.handler.codec.http.cookie.DefaultCookie;
import io.netty.handler.codec.http.cookie.ServerCookieDecoder;
import io.netty.util.internal.PlatformDependent;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.linecorp.armeria.common.HttpHeaderNames.SET_COOKIE;
import static io.netty.handler.codec.http.cookie.ServerCookieEncoder.LAX;

public class CookieServices {

    public static final String HTTP_SESSION_KEY = "JSESSION";
    private static final Map<String, Object> httpSessionStore = PlatformDependent.newConcurrentHashMap();

    public static void main(String[] args) {
        ServerBuilder serverBuilder = new ServerBuilder();
        serverBuilder.http(8000);

        simpleCookie(serverBuilder);

        Server server = serverBuilder.build();
        CompletableFuture<Void> future = server.start();
        future.join();
    }

    private static void simpleCookie(ServerBuilder serverBuilder) {
        serverBuilder.annotatedService(
                new Object() {
                    @Get("/")
                    public HttpResponse home(HttpRequest data) {
                        boolean authorized = authorize(data);
                        String page = authorized ? "/welcome" : "/login";
                        return HttpResponse.of(
                                HttpHeaders.of(HttpStatus.OK).contentType(MediaType.HTML_UTF_8),
                                HttpData.ofUtf8("<html><body onLoad=\"window.location.href='%s'\"></body></html>", page));
                    }

                    @Post("/login")
                    public HttpResponse login(HttpParameters parameters) {
                        String name = parameters.get("name");
                        String password = parameters.get("password");
                        System.out.printf("[INFO] user login %s with password %s.%n", name, password);
                        if ("chuck".equals(name) && "norris".equals(password)) {
                            String sessionId = randomCookieId(); // FIXME: ensure not collide with existing session id
                            httpSessionStore.put(sessionId, System.currentTimeMillis());

                            final Cookie cookie = new DefaultCookie(HTTP_SESSION_KEY, sessionId);
                            cookie.setHttpOnly(true);
                            cookie.setDomain("localhost");
                            cookie.setMaxAge(60);
                            cookie.setPath("/");

                            return HttpResponse.of(
                                    HttpHeaders.of(HttpStatus.OK).contentType(MediaType.HTML_UTF_8).add(SET_COOKIE, LAX.encode(cookie)),
                                    HttpData.ofUtf8("<html><body onLoad=\"window.location.href='/welcome'\"></body></html>"));
                        } else {
                            System.out.println("[WARN] wrong user name or password!");
                            return HttpResponse.of(
                                    HttpHeaders.of(HttpStatus.OK).contentType(MediaType.HTML_UTF_8),
                                    HttpData.ofUtf8("<html><body onLoad=\"window.location.href='/login'\"></body></html>"));
                        }
                    }

                    @Get("/login")
                    public HttpResponse loginForm() {
                        return HttpResponse.of(
                                HttpHeaders.of(HttpStatus.OK).contentType(MediaType.HTML_UTF_8),
                                HttpData.ofUtf8("<html><body><form action='/login' method='POST'><input name='name'><input name='password'><input type='submit'></form></body></html>"));
                    }

                    @Get("/welcome")
                    public HttpResponse welcome(HttpRequest data) {
                        if (!authorize(data))
                            return HttpResponse.of(HttpStatus.UNAUTHORIZED);

                        return HttpResponse.of(
                                HttpHeaders.of(HttpStatus.OK).contentType(MediaType.HTML_UTF_8),
                                HttpData.ofUtf8("Welcome, %s! -- <a href='/'>home</a>", "Cookie User"));
                    }
                }
        );

    }

    public static boolean authorize(HttpRequest data) {
        final String cookies = data.headers().get(HttpHeaderNames.COOKIE);
        if (cookies == null) {
            return false;
        }

        System.err.println("[TRACE] cookies found: " + cookies);

        Optional<Cookie> cookie = ServerCookieDecoder.LAX.decode(cookies).stream().filter(
                c -> HTTP_SESSION_KEY.equals(c.name()) && !Strings.isNullOrEmpty(c.value())).findAny();
        System.err.println("[TRACE] cookie found: " + cookie);

        AtomicBoolean authenticated = new AtomicBoolean(false);
        if (cookie.isPresent()) {
            String key = cookie.get().value();
            if (httpSessionStore.containsKey(key)) {
                Long lastAccessTime = (Long) httpSessionStore.get(key);
                if (System.currentTimeMillis() - lastAccessTime > 7_000) { // expired in 7 seconds
                    httpSessionStore.remove(key);
                } else {
                    httpSessionStore.put(key, System.currentTimeMillis());
                    authenticated.set(true);
                }
            }
        }
        System.out.println("[INFO] authenticated: " + authenticated.get());
        return authenticated.get();
    }

    private static String randomCookieId() {
        int id = (int) Math.ceil(12340000 + Math.random() * 1976_2019);
        System.out.println("new cookie id: " + id);
        return String.valueOf(id);
    }
}
